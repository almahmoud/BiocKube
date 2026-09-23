sessionFor <- function(uid = "cm-uid")
{
    list(namespace = "bioc", configMap = "biockube-ab12",
         secret = "biockube-ab12", uid = uid)
}

podListHandler <- function(...)
{
    items <- list(...)
    function(req, res) {
        ## The watch that follows the list has nothing more to report.
        if (grepl("watch=true", req$query_string, fixed = TRUE))
            return(res$set_status(200L)$send(""))
        res$send_json(list(kind = "PodList", apiVersion = "v1",
                           metadata = list(resourceVersion = "1"),
                           items = items), auto_unbox = TRUE)
    }
}

logHandler <- function(text)
{
    force(text)
    function(req, res) {
        res$set_status(200L)$set_header("Content-Type", "text/plain")
        res$send(text)
    }
}

test_that("session tokens are long and unpredictable", {
    set.seed(1L)
    token <- .newToken()
    expect_match(token, "^[0-9a-f]{64}$")
    set.seed(1L)
    expect_false(identical(token, .newToken()))
    expect_match(.newToken(8L), "^[0-9a-f]{16}$")
})

test_that("the pod scripts are read for the ConfigMap", {
    scripts <- .podScripts()
    expect_named(scripts, c("server.R", "worker.R", "runner.R"))
    expect_true(all(endsWith(unlist(scripts), "\n")))
    expect_identical(
        strsplit(scripts$worker.R, "\n", fixed = TRUE)[[1L]],
        readLines(system.file("pod", "worker.R", package = "BiocKube"))
    )
    expect_named(.podScripts(c("server.R", "runner.R")),
                 c("server.R", "runner.R"))
    expect_error(.podScripts("manager.R"), "no pod script \"manager.R\"")
})

test_that("owner references let the garbage collector clean up", {
    owner <- .ownerReference("ConfigMap", "biockube-ab12", "cm-uid")
    expect_identical(owner$apiVersion, "v1")
    expect_identical(owner$kind, "ConfigMap")
    expect_identical(owner$name, "biockube-ab12")
    expect_identical(owner$uid, "cm-uid")
    expect_false(owner$controller)
    expect_false(owner$blockOwnerDeletion)
    expect_identical(.ownerReference("Job", "j", "u", "batch/v1")$apiVersion,
                     "batch/v1")
})

test_that("a worker pod runs one script and reaches nothing else", {
    manifest <- .podManifest(
        "biockube-ab12-1", "bioc", "bioconductor/bioconductor:devel",
        sessionFor(), cpu = 2, memory = "8Gi", packages = c("DESeq2", "edgeR"),
        env = c(OMP_NUM_THREADS = "2"), idle = 60L,
        labels = c("app.kubernetes.io/managed-by" = "BiocKube")
    )
    spec <- manifest$spec
    container <- spec$containers[[1L]]

    expect_identical(manifest$metadata$name, "biockube-ab12-1")
    expect_identical(manifest$metadata$namespace, "bioc")
    expect_identical(manifest$metadata$labels,
                     list("app.kubernetes.io/managed-by" = "BiocKube"))
    expect_identical(manifest$metadata$ownerReferences,
                     list(.ownerReference("ConfigMap", "biockube-ab12",
                                          "cm-uid")))

    expect_identical(spec$restartPolicy, "Never")
    expect_false(spec$automountServiceAccountToken)
    expect_false(spec$enableServiceLinks)
    expect_identical(spec$volumes,
                     list(list(name = "scripts",
                               configMap = list(name = "biockube-ab12"))))

    expect_identical(as.character(container$command),
                     c("Rscript", "/biockube/worker.R"))
    expect_identical(container$volumeMounts[[1L]]$mountPath, "/biockube")
    expect_true(container$volumeMounts[[1L]]$readOnly)
    expect_identical(container$ports[[1L]]$containerPort, 8080L)
    expect_identical(container$readinessProbe$httpGet,
                     list(path = "/healthz", port = 8080L))
    expect_identical(container$resources,
                     list(requests = list(cpu = "2", memory = "8Gi"),
                          limits = list(cpu = "2", memory = "8Gi")))
    expect_identical(container$securityContext$allowPrivilegeEscalation,
                     FALSE)
    expect_identical(as.character(container$securityContext$capabilities$drop),
                     "ALL")
    expect_null(container$securityContext$runAsNonRoot)

    expect_identical(container$env[[1L]],
                     list(name = "BIOCKUBE_TOKEN",
                          valueFrom = list(secretKeyRef = list(
                              name = "biockube-ab12", key = "token"
                          ))))
    values <- vapply(container$env[-1L], function(x) x$value, character(1))
    names(values) <- vapply(container$env[-1L], function(x) x$name,
                            character(1))
    expect_identical(values, c(BIOCKUBE_PORT = "8080", BIOCKUBE_IDLE = "60",
                               BIOCKUBE_PACKAGES = "DESeq2,edgeR",
                               OMP_NUM_THREADS = "2"))
})

test_that("a worker pod leaves out what it was not given", {
    manifest <- .podManifest("w-1", "bioc", "image", sessionFor(),
                             port = 9000L)
    container <- manifest$spec$containers[[1L]]
    expect_null(container$resources)
    expect_null(manifest$metadata$labels)
    expect_identical(as.character(container$command),
                     c("Rscript", "/biockube/worker.R"))
    expect_identical(container$ports[[1L]]$containerPort, 9000L)
    expect_identical(container$env[[2L]]$value, "9000")
    expect_length(container$env, 3L)
})

test_that("a worker pod refuses what the protocol owns", {
    session <- sessionFor()
    expect_error(.podManifest("w-1", "bioc", "image", session,
                              env = c(BIOCKUBE_TOKEN = "x")),
                 "`env` must not set BIOCKUBE_TOKEN")
    expect_error(.podManifest("w-1", "bioc", "image", session,
                              env = c("no space" = "x")),
                 "`env` names must be")
    expect_error(.podManifest("w-1", "bioc", "image", session, env = "x"),
                 "`env` must be uniquely named strings")
    expect_error(.podManifest("w-1", "bioc", "", session),
                 "`image` must be")
    expect_error(.podManifest("Worker_1", "bioc", "image", session),
                 "`name` must be a name")
    expect_error(.podManifest("w-1", "bioc", "image", session, cpu = -1),
                 "`cpu` must be")
    expect_error(.podManifest("w-1", "bioc", "image", session, memory = TRUE),
                 "`memory` must be")
    expect_error(.podManifest("w-1", "bioc", "image", session, idle = 0),
                 "`idle` must be")
})

test_that("a session is a ConfigMap owning a Secret", {
    api <- fakeApi(list(
        "POST /api/v1/namespaces/bioc/configmaps" = jsonHandler(list(
            apiVersion = "v1", kind = "ConfigMap",
            metadata = list(name = "biockube-ab12", namespace = "bioc",
                            uid = "cm-uid")
        ), 201L),
        "POST /api/v1/namespaces/bioc/secrets" = jsonHandler(list(
            apiVersion = "v1", kind = "Secret",
            metadata = list(name = "biockube-ab12", namespace = "bioc",
                            uid = "secret-uid")
        ), 201L)
    ))
    scripts <- .podScripts(c("server.R", "worker.R"))
    session <- .createSession(
        api$client, "bioc", "biockube-ab12", scripts, "0f1e2d3c",
        labels = c("app.kubernetes.io/managed-by" = "BiocKube")
    )
    expect_identical(session, sessionFor())

    configMap <- requestBody(lastRequest(
        api, "/api/v1/namespaces/bioc/configmaps"))
    expect_identical(configMap$kind, "ConfigMap")
    expect_identical(configMap$metadata$name, "biockube-ab12")
    expect_identical(configMap$metadata$namespace, "bioc")
    expect_identical(configMap$metadata$labels,
                     list("app.kubernetes.io/managed-by" = "BiocKube"))
    expect_named(configMap$data, c("server.R", "worker.R"))
    expect_identical(configMap$data$worker.R, scripts$worker.R)

    secret <- requestBody(lastRequest(api, "/api/v1/namespaces/bioc/secrets"))
    expect_identical(secret$type, "Opaque")
    expect_identical(secret$stringData, list(token = "0f1e2d3c"))
    expect_identical(secret$metadata$ownerReferences,
                     list(.ownerReference("ConfigMap", "biockube-ab12",
                                          "cm-uid")))
})

test_that("a session whose Secret is refused leaves no ConfigMap", {
    api <- fakeApi(list(
        "POST /api/v1/namespaces/bioc/configmaps" = jsonHandler(list(
            apiVersion = "v1", kind = "ConfigMap",
            metadata = list(name = "biockube-ab12", uid = "cm-uid")
        ), 201L),
        "POST /api/v1/namespaces/bioc/secrets" = statusHandler(
            403L, "Forbidden", "secrets is forbidden: cannot create secrets"
        ),
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-ab12" =
            jsonHandler(list(kind = "Status", apiVersion = "v1",
                             status = "Success"))
    ))
    expect_error(
        .createSession(api$client, "bioc", "biockube-ab12",
                       .podScripts("server.R"), "0f1e2d3c"),
        "cannot create secrets"
    )
    deleted <- lastRequest(
        api, "/api/v1/namespaces/bioc/configmaps/biockube-ab12")
    expect_identical(deleted$method, "DELETE")
})

test_that("a session needs the uid of its ConfigMap", {
    api <- fakeApi(list(
        "POST /api/v1/namespaces/bioc/configmaps" = jsonHandler(list(
            apiVersion = "v1", kind = "ConfigMap",
            metadata = list(name = "biockube-ab12")
        ), 201L),
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-ab12" =
            jsonHandler(list(kind = "Status", apiVersion = "v1",
                             status = "Success"))
    ))
    expect_error(
        .createSession(api$client, "bioc", "biockube-ab12",
                       .podScripts("server.R"), "0f1e2d3c"),
        "no uid for ConfigMap"
    )
    expect_identical(lastRequest(
        api, "/api/v1/namespaces/bioc/configmaps/biockube-ab12")$method,
        "DELETE")
})

test_that("waiting ends when every pod is ready", {
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" =
            podListHandler(podObject("worker-1"), podObject("worker-2"),
                           podObject("other", ready = "False"))
    ))
    names <- c("worker-1", "worker-2")
    expect_identical(.waitReady(api$client, "bioc", names, timeout = 10),
                     names)
    expect_identical(.waitReady(api$client, "bioc", character()), character())
})

test_that("waiting stops as soon as a pod cannot become ready", {
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" = podListHandler(podObject(
            "worker-1", phase = "Failed", ready = "False",
            state = list(terminated = list(exitCode = 1L, reason = "Error",
                                           message = "Rscript: not found"))
        )),
        "GET /api/v1/namespaces/bioc/pods/worker-1/log" =
            logHandler("loading BiocParallel\nfailed\n")
    ))
    error <- expect_error(
        .waitReady(api$client, "bioc", "worker-1", timeout = 10),
        "Pod bioc/worker-1 is not ready: Error"
    )
    expect_match(conditionMessage(error), "Rscript: not found")
    expect_match(conditionMessage(error), "Last lines of its log:\nloading")
    expect_identical(
        lastRequest(api, "/api/v1/namespaces/bioc/pods/worker-1/log")$query,
        "container=worker&tailLines=20"
    )
})

test_that("waiting goes on while a pod is not scheduled yet", {
    unplaced <- function(name)
        podObject(name, phase = "Pending", ready = "False",
                  state = list(waiting = list(reason = "ContainerCreating")),
                  conditions = list(list(
                      type = "PodScheduled", status = "False",
                      reason = "Unschedulable",
                      message = "0/3 nodes are available: 3 Insufficient cpu."
                  )))
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" =
            podListHandler(unplaced("worker-1"), podObject("worker-2"))
    ))
    started <- Sys.time()
    error <- expect_error(
        .waitReady(api$client, "bioc", c("worker-1", "worker-2"),
                   timeout = 2),
        "Pods worker-1 were not ready within 2 seconds"
    )
    expect_gte(as.numeric(Sys.time() - started, units = "secs"), 1.5)
    expect_match(conditionMessage(error), paste(
        "Pod bioc/worker-1 is not scheduled: Unschedulable: 0/3 nodes are",
        "available: 3 Insufficient cpu."
    ), fixed = TRUE)
    expect_no_match(conditionMessage(error), "worker-2")
})

test_that("waiting gives up when a pod stays pending", {
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" = podListHandler(podObject(
            "worker-1", phase = "Pending", ready = "False",
            state = list(waiting = list(reason = "ContainerCreating"))
        ))
    ))
    expect_error(.waitReady(api$client, "bioc", c("worker-1", "worker-2"),
                            timeout = 2),
                 "Pods worker-1, worker-2 were not ready within 2 seconds")
    expect_error(.waitReady(api$client, "bioc", "worker-1", timeout = 0),
                 "`timeout` must be")
})

pullingPod <- function(name, reason, message)
{
    podObject(name, phase = "Pending", ready = "False",
              state = list(waiting = list(reason = reason,
                                          message = message)))
}

test_that("waiting goes on while a failed image pull is retried", {
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" = podListHandler(
            pullingPod("worker-1", "ErrImagePull", paste(
                "failed to pull and unpack image: dial tcp 140.82.1.1:443:",
                "i/o timeout"
            )),
            pullingPod("worker-2", "ImagePullBackOff",
                       "Back-off pulling image")
        )
    ))
    started <- Sys.time()
    error <- expect_error(
        .waitReady(api$client, "bioc", c("worker-1", "worker-2"),
                   timeout = 2),
        "Pods worker-1, worker-2 were not ready within 2 seconds"
    )
    expect_gte(as.numeric(Sys.time() - started, units = "secs"), 1.5)
    expect_match(conditionMessage(error), paste(
        "Pod bioc/worker-1 could not pull its image: ErrImagePull: failed",
        "to pull and unpack image: dial tcp 140.82.1.1:443: i/o timeout"
    ), fixed = TRUE)
    expect_match(conditionMessage(error), paste(
        "Pod bioc/worker-2 could not pull its image: ImagePullBackOff:",
        "Back-off pulling image"
    ), fixed = TRUE)
})

test_that("waiting stops when the registry refuses the image", {
    missing <- paste(
        "rpc error: code = NotFound desc = failed to pull and unpack image",
        "\"ghcr.io/bioconductor/bioconductor:RELEASE_9_9\": not found"
    )
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" = podListHandler(
            pullingPod("worker-1", "ErrImagePull", missing)
        )
    ))
    error <- expect_error(
        .waitReady(api$client, "bioc", "worker-1", timeout = 30),
        "Pod bioc/worker-1 is not ready: ErrImagePull"
    )
    expect_match(conditionMessage(error), "RELEASE_9_9\": not found")

    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods" = podListHandler(
            pullingPod("worker-1", "InvalidImageName",
                       "couldn't parse image name \"Bad:Name\"")
        )
    ))
    expect_error(.waitReady(api$client, "bioc", "worker-1", timeout = 30),
                 "Pod bioc/worker-1 is not ready: InvalidImageName")
})

test_that("the back-off after a failed pull keeps the failure's message", {
    retried <- function(reason, message)
        .podState(pullingPod("worker-1", reason, message))
    failure <- retried("ErrImagePull", "429 Too Many Requests")
    backoff <- retried("ImagePullBackOff", "Back-off pulling image")
    expect_true(.pullRetried(failure))
    expect_true(.pullRetried(backoff))
    expect_false(.pullRetried(retried("ErrImagePull", "manifest unknown")))
    expect_false(.pullRetried(retried("ErrImagePull",
                                      "pull access denied")))
    expect_false(.pullRetried(retried("CrashLoopBackOff", "back-off")))
    expect_true(.pullRetried(retried("ErrImagePull", NULL)))

    expect_identical(.pullMessage(failure, NULL),
                     "ErrImagePull: 429 Too Many Requests")
    expect_identical(.pullMessage(backoff, "ErrImagePull: 429"),
                     "ErrImagePull: 429")
    expect_identical(.pullMessage(backoff, NULL),
                     "ImagePullBackOff: Back-off pulling image")
})

test_that("deleting a session removes what it created", {
    deleted <- list(kind = "Status", apiVersion = "v1", status = "Success")
    api <- fakeApi(list(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-ab12" =
            jsonHandler(deleted),
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-ab12" =
            statusHandler(404L, "NotFound", "secrets \"x\" not found")
    ))
    expect_true(.deleteSession(api$client, "bioc", "biockube-ab12"))
    expect_identical(
        lastRequest(api,
                    "/api/v1/namespaces/bioc/configmaps/biockube-ab12")$query,
        "propagationPolicy=Background"
    )
})

test_that("deleting a session gives up on an API server that stalls", {
    stalled <- function(req, res) {
        Sys.sleep(8)
        res$send_json(list(kind = "Status", apiVersion = "v1",
                           status = "Success"), auto_unbox = TRUE)
    }
    api <- fakeApi(list(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-ab12" = stalled,
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-ab12" = stalled
    ))
    started <- Sys.time()
    expect_warning(
        expect_warning(left <- .deleteSession(api$client, "bioc",
                                              "biockube-ab12", timeout = 2),
                       "Could not delete configmaps .*timed out"),
        "Could not delete secrets"
    )
    expect_false(left)
    expect_lt(as.numeric(Sys.time() - started, units = "secs"), 6)
})

test_that("a session that cannot be deleted is reported, not thrown", {
    api <- fakeApi(list(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-ab12" =
            statusHandler(500L, "InternalError", "etcd is unavailable"),
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-ab12" =
            jsonHandler(list(kind = "Status", apiVersion = "v1",
                             status = "Success"))
    ))
    expect_warning(left <- .deleteSession(api$client, "bioc",
                                          "biockube-ab12"),
                   "Could not delete configmaps \"biockube-ab12\"")
    expect_false(left)
})

test_that("a worker pod keeps its arrays on the way to the server", {
    api <- fakeApi(list(
        "POST /api/v1/namespaces/bioc/pods" =
            jsonHandler(podObject("biockube-ab12-1"), 201L)
    ))
    manifest <- .podManifest("biockube-ab12-1", "bioc", "image",
                             sessionFor(), packages = "DESeq2")
    suppressMessages(kube::kube_create(manifest, client = api$client))

    sent <- requestBody(lastRequest(api, "/api/v1/namespaces/bioc/pods"))
    container <- sent$spec$containers[[1L]]
    expect_identical(container$command,
                     list("Rscript", "/biockube/worker.R"))
    expect_identical(container$securityContext$capabilities$drop, list("ALL"))
    expect_identical(container$readinessProbe$httpGet$port, 8080L)
    expect_false(sent$spec$automountServiceAccountToken)
    expect_identical(sent$metadata$ownerReferences[[1L]]$uid, "cm-uid")
    expect_identical(container$env[[1L]]$valueFrom$secretKeyRef$key, "token")
})
