## The fake API server records the manifests and forwards the pod proxy to
## a runner.R started by helper-pod.R, so a submission runs the real
## protocol end to end.

jobName <- "toy-normalize-a1b2c"
podName <- "toy-normalize-a1b2c-9x7kd"
jobsPath <- "/apis/batch/v1/namespaces/bioc/jobs"
jobPath <- paste0(jobsPath, "/", jobName)
secretPath <- paste0("/api/v1/namespaces/bioc/secrets/", jobName)
podPath <- paste0("/api/v1/namespaces/bioc/pods/", podName)
podLogPath <- paste0(podPath, "/log")
eventsPath <- "/api/v1/namespaces/bioc/events"
image <- "bioconductor/bioconductor:devel"

toyJob <- function()
{
    skip_if_not_installed("BiocJobs")
    path <- system.file("examples", "toy", package = "BiocJobs")
    BiocJobs::findJobs(path)[["toy-normalize"]]
}

toyMatrix <- function()
{
    file.path(system.file("examples", "toy", package = "BiocJobs"),
              "test-data", "matrix.tsv")
}

## Discovery answering for batch/v1, which fakeApi() does not serve.
batchDiscovery <- function()
{
    jsonHandler(list(
        kind = "APIGroupDiscoveryList",
        apiVersion = "apidiscovery.k8s.io/v2",
        items = list(list(metadata = list(name = "batch"), versions = list(
            list(version = "v1", freshness = "Current", resources = list(list(
                resource = "jobs",
                responseKind = list(group = "batch", version = "v1",
                                    kind = "Job"),
                scope = "Namespaced", singularResource = "job",
                verbs = as.list(c("create", "delete", "get", "list", "patch",
                                  "update", "watch")),
                shortNames = list(),
                subresources = list(list(subresource = "status"))
            )))
        )))
    ))
}

jobObject <- function(annotations = NULL, status = list())
{
    object <- list(
        apiVersion = "batch/v1", kind = "Job",
        metadata = list(name = jobName, namespace = "bioc", uid = "job-uid",
                        creationTimestamp = "2025-01-01T00:00:00Z"),
        spec = list(suspend = TRUE),
        status = structure(status, names = names(status) %||% character())
    )
    if (!is.null(annotations))
        object$metadata$annotations <- annotations
    object
}

## A pod of the Job, or with `owner = NULL` a pod carrying its label only.
jobPod <- function(name = podName, owner = "job-uid", phase = "Running",
                   ready = "True", state = list(running = list()),
                   created = "2025-01-01T00:00:10Z", conditions = list())
{
    pod <- podObject(name, phase = phase, ready = ready, state = state,
                     conditions = conditions)
    pod$metadata$labels <- list("biockube.bioconductor.org/session" = jobName)
    pod$metadata$creationTimestamp <- created
    pod$status$containerStatuses[[1L]]$name <- "job"
    if (!is.null(owner))
        pod$metadata$ownerReferences <- list(list(
            apiVersion = "batch/v1", kind = "Job", name = jobName,
            uid = owner, controller = TRUE, blockOwnerDeletion = TRUE
        ))
    pod
}

createdObject <- function(kind, uid)
{
    jsonHandler(list(apiVersion = "v1", kind = kind,
                     metadata = list(name = jobName, namespace = "bioc",
                                     uid = uid)), 201L)
}

## Lists `pods`, and answers watches with an empty stream.
podList <- function(pods)
{
    force(pods)
    function(req, res) {
        if (grepl("watch=true", req$query_string, fixed = TRUE))
            return(res$set_status(200L)$send(""))
        res$send_json(list(kind = "PodList", apiVersion = "v1",
                           metadata = list(resourceVersion = "1"),
                           items = pods), auto_unbox = TRUE)
    }
}

jobList <- function()
{
    jsonHandler(list(kind = "JobList", apiVersion = "batch/v1",
                     metadata = list(resourceVersion = "1"),
                     items = list(jobObject())))
}

textHandler <- function(text)
{
    force(text)
    function(req, res) {
        res$set_status(200L)$set_header("Content-Type", "text/plain")
        res$send(text)
    }
}

## Sends `lines` at once, then holds the stream open for `hold` seconds, as
## a followed log does while the container runs.
streamingLog <- function(lines, hold)
{
    force(lines)
    force(hold)
    function(req, res) {
        if (is.null(res$locals$sent)) {
            res$locals$sent <- TRUE
            res$set_header("Content-Type", "text/plain")
            res$send_chunk(paste0(lines, "\n", collapse = ""))
            return(res$delay(hold))
        }
        res$send_chunk("later\n")
    }
}

## Answers 503 to the first `failures` requests, then as `handler` does.
flakyHandler <- function(dir, failures, handler)
{
    force(dir)
    force(failures)
    force(handler)
    function(req, res) {
        count <- length(list.files(dir))
        file.create(file.path(dir, count + 1L))
        if (count < failures)
            return(res$set_status(503L)$send_json(list(
                kind = "Status", apiVersion = "v1", status = "Failure",
                message = "the server is unable to handle the request",
                reason = "ServiceUnavailable", code = 503L
            ), auto_unbox = TRUE))
        handler(req, res)
    }
}

## Answers the n-th request as the n-th of `handlers` does, and later ones
## as the last does.
sequenceHandler <- function(dir, handlers)
{
    force(dir)
    force(handlers)
    function(req, res) {
        count <- length(list.files(dir)) + 1L
        file.create(file.path(dir, count))
        handlers[[min(count, length(handlers))]](req, res)
    }
}

## A log as the API server sends it with timestamps.
stampedLog <- function(lines)
{
    textHandler(paste0(lines, "\n", collapse = ""))
}

## The routes of a Job whose pod the cluster refuses with `message`.
refusedRoutes <- function(message)
{
    event <- list(
        apiVersion = "v1", kind = "Event",
        metadata = list(name = "e1", namespace = "bioc",
                        creationTimestamp = "2025-01-01T00:00:01Z"),
        involvedObject = list(kind = "Job", name = jobName, uid = "job-uid"),
        reason = "FailedCreate", type = "Warning",
        message = paste("Error creating:", message)
    )
    routes <- c(list(jsonHandler(list(kind = "EventList", apiVersion = "v1",
                                      metadata = list(resourceVersion = "1"),
                                      items = list(event)))),
                jobRoutes(pods = list()))
    names(routes)[1L] <- paste("GET", eventsPath)
    routes
}

requestJson <- function(request)
{
    jsonlite::fromJSON(rawToChar(request$body), simplifyVector = FALSE)
}

## The routes a submission and the calls that follow it need.
jobRoutes <- function(annotations = NULL, status = list(),
                      pods = list(jobPod()), secretOwner = "job-uid",
                      log = textHandler("normalizing\n"))
{
    object <- jobObject(annotations, status)
    routes <- list(
        batchDiscovery(),
        jsonHandler(object, 201L),
        jsonHandler(object),
        jsonHandler(object),
        jobList(),
        createdObject("ConfigMap", "cm-uid"),
        createdObject("Secret", "secret-uid"),
        jsonHandler(list(apiVersion = "v1", kind = "Secret",
                         metadata = list(
                             name = jobName, namespace = "bioc",
                             ownerReferences = list(list(
                                 apiVersion = "batch/v1", kind = "Job",
                                 name = jobName, uid = secretOwner,
                                 controller = FALSE
                             ))
                         ),
                         type = "Opaque",
                         data = list(token = "dGVzdC10b2tlbg=="))),
        podList(pods),
        log,
        jsonHandler(list(kind = "Status", apiVersion = "v1",
                         status = "Success"))
    )
    names(routes) <- c(
        "GET /apis", paste("POST", jobsPath), paste("PATCH", jobPath),
        paste("GET", jobPath), paste("GET", jobsPath),
        "POST /api/v1/namespaces/bioc/configmaps",
        "POST /api/v1/namespaces/bioc/secrets", paste("GET", secretPath),
        "GET /api/v1/namespaces/bioc/pods", paste("GET", podLogPath),
        paste("DELETE", jobPath)
    )
    if (length(pods))
        routes[[paste("GET", podPath)]] <- jsonHandler(pods[[1L]])
    routes
}

## The requests made to a path with a given method, and the last of them.
requestsFor <- function(api, method, path)
{
    Filter(function(request)
        identical(request$path, path) &&
            identical(request$method, toupper(method)), api$requests())
}

requestFor <- function(api, method, path)
{
    requests <- requestsFor(api, method, path)
    if (!length(requests))
        stop("no ", method, " request for ", path)
    requests[[length(requests)]]
}

envValues <- function(container)
{
    values <- vapply(container$env, function(e)
        e[["value"]] %||% NA_character_, character(1L))
    names(values) <- vapply(container$env, function(e) e[["name"]],
                            character(1L))
    values
}

newJob <- function(api, outputs = character(), pod = podName)
{
    new("KubeJob", name = jobName, namespace = "bioc", uid = "job-uid",
        pod = pod, token = "test-token", package = "toy",
        job = "toy-normalize", outputs = outputs, client = api$client)
}

test_that("a job name fits a DNS label", {
    spec <- toyJob()
    expect_match(.jobName(spec, NULL), "^toy-normalize-[0-9a-f]{10}$")
    expect_false(identical(.jobName(spec, NULL), .jobName(spec, NULL)))
    expect_identical(.jobName(spec, "counts-1"), "counts-1")
    expect_error(.jobName(spec, "Counts_1"), "`name` must be a name")
    expect_identical(.dnsLabel("Toy_Normalize v2.0", 57L), "toy-normalize-v2-0")
    expect_identical(.dnsLabel("--x--", 57L), "x")
    long <- .jobName(list(name = strrep("a", 80L)), NULL)
    expect_identical(nchar(long), 63L)
    expect_match(long, "^a{52}-[0-9a-f]{10}$")
})

test_that("a generated name that is taken is drawn again", {
    spec <- toyJob()
    values <- .splitValues(spec, list(matrix = toyMatrix()))
    tried <- character()
    taken <- 2L
    local_mocked_bindings(.create = function(client, manifest) {
        tried <<- c(tried, manifest$metadata$name)
        if (length(tried) <= taken)
            stop(structure(
                class = c("kube_error_alreadyexists", "kube_error", "error",
                          "condition"),
                list(message = "already exists", call = NULL)
            ))
        list(metadata = list(name = manifest$metadata$name, uid = "u"))
    })

    created <- .createJob(NULL, spec, "bioc", NULL, values, image, 60)
    expect_length(unique(tried), 3L)
    expect_identical(created$name, tried[3L])
    expect_identical(created$manifest$metadata$name, tried[3L])

    tried <- character()
    expect_error(.createJob(NULL, spec, "bioc", jobName, values, image, 60),
                 "A Job named \"toy-normalize-a1b2c\" already exists")
    expect_identical(tried, jobName)

    tried <- character()
    taken <- 5L
    expect_error(.createJob(NULL, spec, "bioc", NULL, values, image, 60),
                 class = "kube_error_alreadyexists")
    expect_length(tried, 3L)
})

test_that("values are split into staged inputs, uploads and options", {
    spec <- toyJob()
    values <- .splitValues(spec, list(method = "zscore",
                                      matrix = toyMatrix()))
    expect_length(values$inputs, 0L)
    expect_identical(names(values$uploads), "matrix")
    expect_identical(values$uploads[["matrix"]], normalizePath(toyMatrix()))
    expect_identical(values$options, list(method = "zscore"))

    staged <- .splitValues(spec, list(matrix = "https://example.org/m.tsv"))
    expect_identical(staged$inputs, c(matrix = "https://example.org/m.tsv"))
    expect_identical(staged$uploads, character())
    expect_identical(.splitValues(spec, list(matrix = "ftp://h/m.tsv"))$inputs,
                     c(matrix = "ftp://h/m.tsv"))
    expect_length(.splitValues(spec, list())$options, 0L)
})

test_that("a submission refuses values the job does not declare", {
    spec <- toyJob()
    expect_error(.splitValues(spec, list(matrix = toyMatrix(), depth = 3)),
                 "has no value \"depth\". It takes matrix, method")
    expect_error(.splitValues(spec, list("counts.tsv")),
                 "`values` must be uniquely named")
    expect_error(.splitValues(spec, list(matrix = 1L)),
                 "Input \"matrix\" must be a file path or a URL")
    expect_error(.splitValues(spec, list(matrix = "/no/such/file.tsv")),
                 "Input file \"/no/such/file.tsv\" does not exist")
    expect_error(.splitValues(spec, list(matrix = tempdir())),
                 "does not exist")
    expect_error(.splitValues(spec, list(matrix = "s3://bucket/m.tsv")),
                 "\"matrix\" must be a local file or an http, https or ftp")
})

test_that("a job is found by package and name", {
    skip_if_not_installed("BiocJobs")
    path <- system.file("examples", "toy", package = "BiocJobs")
    expect_identical(.jobSpec(path, "toy-normalize")$name, "toy-normalize")
    expect_identical(.jobSpec(toyJob(), NULL)$name, "toy-normalize")
    expect_identical(.jobSpec(toyJob(), "toy-normalize")$name, "toy-normalize")
    expect_error(.jobSpec(toyJob(), "other"),
                 "`package` is the job \"toy-normalize\", not \"other\"")
    expect_error(.jobSpec(path, "other"),
                 "declares no job \"other\", but toy-normalize")
    expect_error(.jobSpec(tempdir(), "other"), "declares no jobs")
    expect_error(.jobSpec(path, NULL), "`job` must name one of the jobs")
    expect_error(.jobSpec(1L, "x"), "`package` must be a BiocJob")
})

test_that("the manifest runs the runner in place of the job command", {
    spec <- toyJob()
    manifest <- .jobManifest(spec, "bioc", jobName, uploads = "matrix",
                             options = list(method = "zscore"),
                             image = image, keep = 3600L)
    pod <- manifest$spec$template$spec
    container <- pod$containers[[1L]]

    expect_identical(manifest$metadata$name, jobName)
    expect_identical(manifest$metadata$namespace, "bioc")
    expect_true(manifest$spec$suspend)
    expect_identical(manifest$metadata$labels[["biocjobs.job"]],
                     "toy-normalize")
    expect_identical(
        manifest$metadata$labels[["app.kubernetes.io/managed-by"]],
        "BiocKube"
    )
    expect_identical(
        manifest$metadata$labels[["biockube.bioconductor.org/session"]],
        jobName
    )
    expect_identical(manifest$spec$template$metadata$labels,
                     manifest$metadata$labels)

    expect_null(pod$initContainers)
    expect_identical(as.character(container$command),
                     c("Rscript", "/biockube/runner.R"))
    expect_identical(container$ports[[1L]]$containerPort, 8080L)
    expect_identical(container$readinessProbe$httpGet,
                     list(path = "/healthz", port = 8080L))
    expect_identical(container$workingDir, "/biocjob")
    mounts <- vapply(container$volumeMounts, function(x) x$mountPath,
                     character(1L))
    expect_identical(mounts, c("/biocjob", "/biockube"))
    volumes <- vapply(pod$volumes, function(x) x$name, character(1L))
    expect_identical(volumes, c("work", "scripts"))
    expect_identical(pod$volumes[[2L]]$configMap, list(name = jobName))
    expect_false(pod$automountServiceAccountToken)

    values <- envValues(container)
    expect_identical(values[["BIOCKUBE_KEEP"]], "3600")
    expect_identical(values[["BIOCKUBE_PORT"]], "8080")
    expect_identical(container$env[[1L]],
                     list(name = "BIOCKUBE_TOKEN",
                          valueFrom = list(secretKeyRef = list(
                              name = jobName, key = "token"))))
})

test_that("the runner and later sessions read what the manifest carries", {
    spec <- toyJob()
    manifest <- .jobManifest(spec, "bioc", jobName, uploads = "matrix",
                             options = list(method = "zscore"),
                             image = image)
    values <- envValues(manifest$spec$template$spec$containers[[1L]])
    config <- podSource("runner.R")$.runnerConfig(values[["BIOCKUBE_RUNNER"]])

    original <- BiocJobs::kubernetesJob(spec, options = list(method = "zscore"),
                                        image = image)
    expect_identical(
        config$command,
        as.character(unlist(
            original$spec$template$spec$containers[[1L]]$command))
    )
    expect_identical(config$uploads,
                     c(matrix = "/biocjob/inputs/matrix.tsv"))
    expect_identical(config$outputs,
                     c(normalized = "/biocjob/outputs/normalized.tsv"))

    annotation <- manifest$metadata$annotations[[
        "biockube.bioconductor.org/job"]]
    expect_identical(
        jsonlite::fromJSON(annotation, simplifyVector = FALSE),
        list(package = "toy", job = "toy-normalize",
             outputs = list(normalized = "/biocjob/outputs/normalized.tsv"))
    )
    expect_identical(.jobConfig(manifest),
                     list(package = "toy", job = "toy-normalize",
                          outputs = config$outputs))
})

test_that("an input the pod downloads keeps its init container", {
    spec <- toyJob()
    manifest <- .jobManifest(spec, "bioc", jobName,
                             inputs = c(matrix = "https://example.org/m.tsv"),
                             image = image)
    pod <- manifest$spec$template$spec
    expect_length(pod$initContainers, 1L)
    expect_identical(pod$initContainers[[1L]]$name, "stage-matrix")
    expect_true("https://example.org/m.tsv" %in%
                    unlist(pod$initContainers[[1L]]$command))
    values <- envValues(pod$containers[[1L]])
    config <- podSource("runner.R")$.runnerConfig(values[["BIOCKUBE_RUNNER"]])
    expect_identical(config$uploads, character())
})

test_that("a job missing a value cannot be submitted", {
    spec <- toyJob()
    expect_error(.jobManifest(spec, "bioc", jobName, image = image),
                 "Job \"toy-normalize\" needs values for inputs.matrix")
    spec$options[[1L]]$default <- NULL
    spec$options[[1L]]$required <- TRUE
    expect_error(.jobManifest(spec, "bioc", jobName, uploads = "matrix",
                              image = image),
                 "needs values for options.method")
})

test_that("input and output paths come from the job command", {
    argv <- c("Rscript", "-e", "BiocJobs::execJob()", "--matrix", "/in.tsv",
              "--normalized", "/out.tsv")
    expect_identical(.commandPaths(argv, "matrix", "input"),
                     c(matrix = "/in.tsv"))
    expect_identical(.commandPaths(argv, character(), "input"), character())
    expect_error(.commandPaths(argv, c("matrix", "counts"), "input"),
                 "carries no path for input \"counts\"")
    expect_error(.commandPaths(c(argv, "--trailing"), "trailing", "option"),
                 "carries no path for option \"trailing\"")
})

test_that("the runner configuration travels as hexadecimal text", {
    config <- list(command = c("Rscript", "-e", "1"),
                   uploads = c(a = "/in/a"), outputs = character())
    text <- .encodeConfig(config)
    expect_match(text, "^[0-9a-f]+$")
    expect_identical(podSource("runner.R")$.runnerConfig(text), config)
})

test_that("the job annotation is read as JSON and checked", {
    read <- function(text) .decodeJobConfig(text)
    expect_identical(
        read('{"package":"toy","job":"x","outputs":{"o":"/out/o.tsv"}}'),
        list(package = "toy", job = "x", outputs = c(o = "/out/o.tsv"))
    )
    expect_identical(read('{"package":"toy","job":"x","outputs":{}}')$outputs,
                     character())
    expect_identical(read('{"package":"toy","job":"x"}')$outputs,
                     character())
    expect_identical(.decodeJobConfig(.encodeJobConfig("toy", "x",
                                                       character())),
                     list(package = "toy", job = "x", outputs = character()))

    expect_error(read("nothex"), "not a JSON object")
    expect_error(read("[1, 2]"), "not a JSON object")
    expect_error(read(.encodeConfig(list(package = "toy"))),
                 "not a JSON object")
    expect_error(read('{"package":"toy"}'), "does not name a package and")
    expect_error(read('{"package":["a","b"],"job":"x"}'),
                 "does not name a package and")
    bad <- c('{"o":1}', '{"o":["/a","/b"]}', '{"o":"out/o.tsv"}',
             '{"o":"/x/.Rprofile"}', '{"o":"/x/.."}', '{"o":"/x/"}',
             '{"../o":"/x/o"}', '{"o":"/x/o","p":"/y/o"}', '["/x/o"]')
    for (outputs in bad)
        expect_error(
            read(sprintf('{"package":"toy","job":"x","outputs":%s}', outputs)),
            "outputs are not named container paths", info = outputs
        )
})

test_that("a serialized annotation is never unserialized", {
    flag <- tempfile("payload-")
    payload <- new.env()
    makeActiveBinding("spec", function() {
        file.create(flag)
        list()
    }, payload)
    job <- jobObject(annotations = list(
        "biockube.bioconductor.org/job" = paste(serialize(payload, NULL),
                                                collapse = "")
    ))
    expect_error(.jobConfig(job),
                 "unreadable biockube.bioconductor.org/job annotation")
    expect_false(file.exists(flag))
})

test_that("secret values are decoded", {
    plain <- c("", "f", "fo", "foo", "foob", "fooba", "foobar")
    coded <- c("", "Zg==", "Zm8=", "Zm9v", "Zm9vYg==", "Zm9vYmE=",
               "Zm9vYmFy")
    decoded <- vapply(coded, function(x) rawToChar(.decodeBase64(x)),
                      character(1L), USE.NAMES = FALSE)
    expect_identical(decoded, plain)
    expect_identical(rawToChar(.decodeBase64("dGVzdC10\nb2tlbg==")),
                     "test-token")
    expect_error(.decodeBase64("not base64!"), "not base64 text")
})

test_that("a KubeJob holds what a job needs to be reached", {
    make <- function(...) {
        slots <- list(name = "j", namespace = "bioc", uid = "u", pod = "p",
                      token = "t", package = "toy", job = "x",
                      outputs = character(), client = NULL)
        args <- utils::modifyList(slots, list(...))
        do.call(new, c(list("KubeJob"), args))
    }
    expect_s4_class(make(outputs = c(o = "/out/o.tsv")), "KubeJob")
    expect_error(make(name = character()), "`name` must be a string")
    expect_error(make(uid = NA_character_), "`uid` must be a string")
    expect_error(make(package = ""), "`package` must be a string")
    expect_error(make(outputs = "/out.tsv"), "`outputs` must be named paths")
    expect_error(make(outputs = c(o = "/x/..")),
                 "`outputs` must be named paths")
    expect_error(make(client = "https://example.org"),
                 "`client` must be a kube_client")
    expect_error(jobStatus("toy-normalize"), "`x` must be a KubeJob")
})

test_that("a job is created, fed its inputs and started", {
    local_mocked_bindings(.newToken = function(bytes = 32L) "test-token")
    work <- tempfile("submit-")
    dir.create(work)
    on.exit(unlink(work, recursive = TRUE), add = TRUE)
    input <- file.path(work, "matrix.tsv")
    output <- file.path(work, "normalized.tsv")
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = runnerConfig(
        c(rscript(), "-e",
          sprintf("Sys.sleep(3); file.copy(\"%s\", \"%s\")", input, output)),
        uploads = c(matrix = input), outputs = c(normalized = output)
    )))
    on.exit(stopPod(pod), add = TRUE)
    api <- fakeApi(jobRoutes(), pod = pod)

    job <- submitJob(toyJob(), values = list(matrix = toyMatrix(),
                                             method = "zscore"),
                     image = image, namespace = "bioc", name = jobName,
                     client = api$client)
    expect_s4_class(job, "KubeJob")
    expect_identical(job@name, jobName)
    expect_identical(job@namespace, "bioc")
    expect_identical(job@uid, "job-uid")
    expect_identical(job@pod, podName)
    expect_identical(job@package, "toy")
    expect_identical(job@job, "toy-normalize")
    expect_identical(job@outputs,
                     c(normalized = "/biocjob/outputs/normalized.tsv"))
    expect_identical(jobStatus(job), "running")

    created <- requestBody(requestFor(api, "POST", jobsPath))
    expect_identical(created$metadata$name, jobName)
    expect_true(created$spec$suspend)
    expect_null(created$spec$template$spec$initContainers)
    expect_identical(created$spec$template$spec$containers[[1L]]$command,
                     list("Rscript", "/biockube/runner.R"))

    configMap <- requestBody(requestFor(
        api, "POST", "/api/v1/namespaces/bioc/configmaps"))
    expect_named(configMap$data, c("server.R", "runner.R"))
    expect_identical(configMap$metadata$ownerReferences,
                     list(.ownerReference("Job", jobName, "job-uid",
                                          "batch/v1")))
    secret <- requestBody(requestFor(
        api, "POST", "/api/v1/namespaces/bioc/secrets"))
    expect_identical(secret$stringData, list(token = "test-token"))
    expect_identical(secret$metadata$ownerReferences[[1L]]$kind, "Job")

    resumed <- requestFor(api, "PATCH", jobPath)
    expect_identical(rawToChar(resumed$body), "{\"spec\":{\"suspend\":false}}")
    expect_identical(requestHeader(resumed, "Content-Type"),
                     "application/merge-patch+json")

    upload <- requestFor(api, "PUT", paste0(
        "/api/v1/namespaces/bioc/pods/", podName, ":8080/proxy/inputs/matrix"))
    expect_identical(requestHeader(upload, "X-BiocKube-Token"), "test-token")
    expect_identical(upload$body,
                     readBin(toyMatrix(), "raw", file.size(toyMatrix())))

    expect_identical(waitJob(job, timeout = 60), job)
    expect_identical(jobStatus(job), "succeeded")
    expect_identical(as.character(jobLogs(job)), "normalizing")
    expect_match(requestFor(api, "GET", podLogPath)$query, "container=job")
    expect_true(any(grepl(.exitLine, readLines(pod$log))))

    printed <- capture.output(show(job))
    expect_false(any(grepl("test-token", printed, fixed = TRUE)))
    expect_match(printed[1L], jobName)
    expect_match(printed[2L], "toy/toy-normalize", fixed = TRUE)

    results <- file.path(work, "results")
    paths <- jobOutputs(job, dir = results)
    expect_identical(paths, c(normalized = file.path(results,
                                                     "normalized.tsv")))
    expect_identical(readBin(paths[["normalized"]], "raw", 1e6L),
                     readBin(toyMatrix(), "raw", 1e6L))
    expect_identical(list.files(results, all.files = TRUE, no.. = TRUE),
                     "normalized.tsv")
    expect_identical(waitExit(pod), 0L)
    expect_length(requestsFor(api, "DELETE", jobPath), 0L)
})

test_that("only a pod the Job controls receives the inputs", {
    local_mocked_bindings(.newToken = function(bytes = 32L) "test-token")
    dir <- tempfile("owned-")
    on.exit(unlink(dir, recursive = TRUE), add = TRUE)
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = runnerConfig(
        c(rscript(), "-e", "0"),
        uploads = c(matrix = file.path(dir, "matrix.tsv"))
    )))
    on.exit(stopPod(pod), add = TRUE)
    intruder <- "toy-normalize-a1b2c-intruder"
    api <- fakeApi(jobRoutes(pods = list(
        jobPod(intruder, owner = NULL),
        jobPod("toy-normalize-a1b2c-stale", owner = "old-uid"),
        jobPod()
    )), pod = pod)

    job <- submitJob(toyJob(), values = list(matrix = toyMatrix()),
                     image = image, namespace = "bioc", name = jobName,
                     client = api$client)
    expect_identical(job@pod, podName)
    proxied <- vapply(api$requests(), function(r) r$path, character(1L))
    expect_false(any(grepl(intruder, proxied, fixed = TRUE)))
    expect_false(any(grepl("stale", proxied, fixed = TRUE)))
})

test_that("a submission that fails takes its Job off the cluster", {
    api <- fakeApi(jobRoutes(pods = list(jobPod(
        phase = "Pending", ready = "False",
        state = list(waiting = list(reason = "ImagePullBackOff",
                                    message = "no such image"))
    ))))
    expect_error(
        submitJob(toyJob(), values = list(matrix = toyMatrix()),
                  image = image, namespace = "bioc", name = jobName,
                  client = api$client),
        "is not ready: ImagePullBackOff"
    )
    deleted <- requestFor(api, "DELETE", jobPath)
    expect_identical(requestJson(deleted),
                     list(propagationPolicy = "Background",
                          preconditions = list(uid = "job-uid")))
})

test_that("an interrupted submission takes its Job off the cluster", {
    api <- fakeApi(jobRoutes())
    local_mocked_bindings(.waitJobPod = function(...) {
        signalCondition(structure(class = c("interrupt", "condition"),
                                  list(message = "", call = NULL)))
    })
    result <- tryCatch(
        submitJob(toyJob(), values = list(matrix = toyMatrix()),
                  image = image, namespace = "bioc", name = jobName,
                  client = api$client),
        interrupt = function(e) "interrupted"
    )
    expect_identical(result, "interrupted")
    expect_length(requestsFor(api, "DELETE", jobPath), 1L)
})

test_that("a pod the cluster refuses to create is reported at once", {
    api <- fakeApi(refusedRoutes(paste(
        "pods \"x\" is forbidden: violates PodSecurity \"restricted:latest\":",
        "runAsNonRoot != true"
    )))

    elapsed <- system.time(expect_error(
        submitJob(toyJob(), values = list(matrix = toyMatrix()),
                  image = image, namespace = "bioc", name = jobName,
                  client = api$client),
        "cannot run its pod: Error creating: .* violates PodSecurity"
    ))[["elapsed"]]
    expect_lt(elapsed, 30)
    query <- utils::URLdecode(requestFor(api, "GET", eventsPath)$query)
    expect_match(query, "involvedObject.uid=job-uid,reason=FailedCreate",
                 fixed = TRUE)
    expect_length(requestsFor(api, "DELETE", jobPath), 1L)
})

test_that("a pod a quota in use keeps out is waited for", {
    api <- fakeApi(refusedRoutes(paste(
        "pods \"x\" is forbidden: exceeded quota: compute, requested:",
        "pods=1, used: pods=10, limited: pods=10"
    )))
    elapsed <- system.time(expect_error(
        .waitJobPod(api$client, "bioc", jobName, "job-uid", 2),
        paste0("not ready within 2 seconds.\nThe cluster refused to create ",
               "it: Error creating: .* exceeded quota: compute")
    ))[["elapsed"]]
    expect_gte(elapsed, 2)
})

test_that("only a refusal the Job controller cannot overcome is final", {
    final <- c(
        "pods \"x\" is forbidden: violates PodSecurity \"baseline:latest\"",
        "pods \"x\" is forbidden: failed quota: q: must specify limits.cpu",
        "pods \"x\" is forbidden: maximum cpu usage per Container is 2",
        "admission webhook \"policy.example.org\" denied the request: no"
    )
    passing <- c(
        "pods \"x\" is forbidden: exceeded quota: q, requested: pods=1",
        paste("pods \"x\" is forbidden: error looking up service account",
              "bioc/default: serviceaccount \"default\" not found"),
        paste("Internal error occurred: failed calling webhook",
              "\"policy.example.org\": context deadline exceeded")
    )
    for (message in final)
        expect_true(.finalRefusal(paste("Error creating:", message)),
                    info = message)
    for (message in passing)
        expect_false(.finalRefusal(paste("Error creating:", message)),
                     info = message)
})

test_that("a ConfigMap or Secret a deleted Job left is removed first", {
    counts <- tempfile("counts-")
    dir.create(counts)
    on.exit(unlink(counts, recursive = TRUE), add = TRUE)
    configMaps <- "/api/v1/namespaces/bioc/configmaps"
    configMapPath <- paste0(configMaps, "/", jobName)
    left <- function(owners) jsonHandler(list(
        apiVersion = "v1", kind = "ConfigMap",
        metadata = list(name = jobName, namespace = "bioc",
                        uid = "left-uid", ownerReferences = owners)
    ))
    routes <- jobRoutes()
    routes[[paste("POST", configMaps)]] <- sequenceHandler(counts, list(
        statusHandler(409L, "AlreadyExists",
                      sprintf("configmaps \"%s\" already exists", jobName)),
        createdObject("ConfigMap", "cm-uid")
    ))
    routes[[paste("GET", configMapPath)]] <- left(list(
        .ownerReference("Job", jobName, "old-uid", "batch/v1")))
    routes[[paste("DELETE", configMapPath)]] <- jsonHandler(list(
        kind = "Status", apiVersion = "v1", status = "Success"))
    api <- fakeApi(routes)
    owner <- .ownerReference("Job", jobName, "job-uid", "batch/v1")

    .createJobResources(api$client, "bioc", jobName, "token", owner)
    expect_length(requestsFor(api, "POST", configMaps), 2L)
    expect_identical(requestJson(requestFor(api, "DELETE", configMapPath)),
                     list(preconditions = list(uid = "left-uid")))
    expect_length(requestsFor(api, "POST", "/api/v1/namespaces/bioc/secrets"),
                  1L)

    unlink(list.files(counts, full.names = TRUE))
    routes[[paste("GET", configMapPath)]] <- left(list())
    api <- fakeApi(routes)
    expect_error(
        .createJobResources(api$client, "bioc", jobName, "token", owner),
        sprintf("ConfigMap \"%s\" already exists in namespace \"bioc\"; %s",
                jobName, "remove it or give another `name`."),
        fixed = TRUE
    )
    expect_length(requestsFor(api, "DELETE", configMapPath), 0L)
})

test_that("a pod that stays pending times out with what it waits for", {
    api <- fakeApi(jobRoutes(pods = list(jobPod(
        phase = "Pending", ready = "False",
        state = list(waiting = list(reason = "ContainerCreating"))
    ))))
    expect_error(.waitJobPod(api$client, "bioc", jobName, "job-uid", 2),
                 paste0("not ready within 2 seconds.\nPod bioc/", podName,
                        " is waiting: ContainerCreating$"))

    api <- fakeApi(jobRoutes(pods = list(jobPod(
        phase = "Pending", ready = "False", state = list(),
        conditions = list(list(
            type = "PodScheduled", status = "False", reason = "Unschedulable",
            message = "0/3 nodes are available: 3 Insufficient cpu."
        ))
    ))))
    expect_error(.waitJobPod(api$client, "bioc", jobName, "job-uid", 2),
                 "is not scheduled: Unschedulable: 0/3 nodes are available")
})

test_that("a job that fails reports the exit status of its command", {
    local_mocked_bindings(.newToken = function(bytes = 32L) "test-token")
    never <- file.path(tempfile("failed-"), "normalized.tsv")
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = runnerConfig(
        c(rscript(), "-e", "quit(status = 3L)"),
        outputs = c(normalized = never))))
    on.exit(stopPod(pod), add = TRUE)
    api <- fakeApi(jobRoutes(), pod = pod)

    job <- submitJob(toyJob(), image = image, namespace = "bioc",
                     name = jobName, client = api$client,
                     values = list(matrix = "https://example.org/m.tsv"))
    created <- requestBody(requestFor(api, "POST", jobsPath))
    expect_length(created$spec$template$spec$initContainers, 1L)

    failure <- expect_error(waitJob(job, timeout = 60),
                            class = "biockube_job_failed")
    expect_identical(failure$status, 3L)
    expect_match(conditionMessage(failure), "failed with exit status 3")
    expect_identical(jobStatus(job), "failed")
    dir <- tempfile("results-")
    on.exit(unlink(dir, recursive = TRUE), add = TRUE)
    expect_warning(paths <- jobOutputs(job, dir = dir),
                   "did not write \"normalized\"")
    expect_identical(paths, c(normalized = NA_character_))
    expect_identical(waitExit(pod), 3L)
})

test_that("the outputs a failed job wrote are collected", {
    work <- tempfile("partial-")
    dir.create(work)
    on.exit(unlink(work, recursive = TRUE), add = TRUE)
    written <- file.path(work, "b.tsv")
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = runnerConfig(
        c(rscript(), "-e",
          sprintf("writeLines('b', '%s'); quit(status = 2L)", written)),
        outputs = c(a = file.path(work, "a.tsv"), b = written)
    )))
    on.exit(stopPod(pod), add = TRUE)
    expect_identical(podRequest(pod, "POST", "/start")$status, 202L)
    api <- fakeApi(jobRoutes(), pod = pod)
    job <- newJob(api, outputs = c(a = "/biocjob/outputs/a.tsv",
                                   b = "/biocjob/outputs/b.tsv"))
    expect_error(waitJob(job, timeout = 60, interval = 0.2),
                 class = "biockube_job_failed")

    results <- file.path(work, "results")
    expect_warning(paths <- jobOutputs(job, dir = results),
                   "did not write \"a\"")
    expect_identical(paths, c(a = NA_character_,
                              b = file.path(results, "b.tsv")))
    expect_identical(readLines(paths[["b"]]), "b")
    expect_identical(waitExit(pod), 2L)
})

test_that("inputs and outputs are streamed rather than held in memory", {
    skip_if_not(capabilities("profmem"))
    work <- tempfile("stream-")
    dir.create(work)
    on.exit(unlink(work, recursive = TRUE), add = TRUE)
    size <- 8e6
    input <- file.path(work, "input.bin")
    writeBin(as.raw(sample(0:255, size, replace = TRUE)), input)
    received <- file.path(work, "received.bin")
    output <- file.path(work, "output.bin")
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = runnerConfig(
        c(rscript(), "-e", sprintf("file.copy('%s', '%s')", received, output)),
        uploads = c(data = received), outputs = c(result = output)
    )))
    on.exit(stopPod(pod), add = TRUE)
    api <- fakeApi(jobRoutes(), pod = pod)
    job <- newJob(api, outputs = c(result = "/biocjob/outputs/result.bin"))
    results <- file.path(work, "results")

    profile <- tempfile(fileext = ".txt")
    on.exit(unlink(profile), add = TRUE)
    Rprofmem(profile, threshold = size / 4)
    .uploadInputs(job, c(data = input))
    expect_identical(podRequest(pod, "POST", "/start")$status, 202L)
    waitJob(job, timeout = 60, interval = 0.2)
    paths <- jobOutputs(job, dir = results)
    Rprofmem(NULL)

    expect_identical(unname(tools::md5sum(paths[["result"]])),
                     unname(tools::md5sum(input)))
    lines <- if (file.exists(profile)) readLines(profile) else character()
    sizes <- as.numeric(sub(" *:.*", "", grep("^[0-9]+ *:", lines,
                                              value = TRUE)))
    expect_false(any(sizes >= size / 4))
})

test_that("following a job's log ends when its command has exited", {
    lines <- c("normalizing", "",
               "2025-01-01 00:00:05 command exited with status 0")
    stamps <- c("2025-01-01T00:00:01.5Z", "2025-01-01T00:00:05Z",
                "2025-01-01T00:00:05.000000001Z")
    api <- fakeApi(jobRoutes(log = streamingLog(paste(stamps, lines), 20)))
    job <- newJob(api)
    elapsed <- system.time(
        printed <- capture.output(result <- jobLogs(job, follow = TRUE))
    )[["elapsed"]]
    expect_lt(elapsed, 10)
    expect_identical(printed, lines)
    expect_null(result)
    query <- requestFor(api, "GET", podLogPath)$query
    expect_match(query, "container=job", fixed = TRUE)
    expect_match(query, "follow=true", fixed = TRUE)
    expect_match(query, "timestamps=true", fixed = TRUE)
    expect_error(jobLogs(job, follow = NA), "`follow` must be TRUE or FALSE")
})

test_that("a followed log that ends early is read again after its last line", {
    counts <- tempfile("counts-")
    dir.create(counts)
    on.exit(unlink(counts, recursive = TRUE), add = TRUE)
    exit <- "2025-01-01 00:00:03 command exited with status 0"
    api <- fakeApi(jobRoutes(log = sequenceHandler(counts, list(
        stampedLog(c("2025-01-01T00:00:01.5Z one",
                     "2025-01-01T00:00:02.25Z two",
                     "2025-01-01T00:00:02.5Z three")),
        stampedLog(c("2025-01-01T00:00:02.25Z two",
                     "2025-01-01T00:00:02.5Z three",
                     "2025-01-01T00:00:02.5Z three again",
                     "2025-01-01T00:00:03Z ",
                     paste("2025-01-01T00:00:03.1Z", exit)))
    ))))

    printed <- capture.output(jobLogs(newJob(api), follow = TRUE))
    expect_identical(printed, c("one", "two", "three", "three again", "",
                                exit))
    queries <- vapply(requestsFor(api, "GET", podLogPath), function(r)
        utils::URLdecode(r$query), character(1L))
    expect_length(queries, 2L)
    expect_false(grepl("sinceTime", queries[1L], fixed = TRUE))
    expect_match(queries[2L], "sinceTime=2025-01-01T00:00:02Z", fixed = TRUE)
})

test_that("a followed log whose stream ends with the job is not read again", {
    api <- fakeApi(jobRoutes(status = list(succeeded = 1L), pods = list(),
                             log = stampedLog("2025-01-01T00:00:01Z done")))
    printed <- capture.output(result <- jobLogs(newJob(api), follow = TRUE))
    expect_identical(printed, "done")
    expect_null(result)
    expect_length(requestsFor(api, "GET", podLogPath), 1L)
})

test_that("log timestamps sort as the time does", {
    entry <- .logEntry("2025-01-01T00:00:02.25+02:00 two words")
    expect_identical(entry, list(second = "2025-01-01T00:00:02+02:00",
                                 key = "20250101000002250000000",
                                 text = "two words"))
    expect_identical(.logEntry("2025-01-01T00:00:02Z")$text, "")
    keys <- vapply(c("2025-01-01T00:00:02Z", "2025-01-01T00:00:02.05Z",
                     "2025-01-01T00:00:02.123456789Z",
                     "2025-01-01T00:00:02.5Z", "2025-01-01T00:00:10Z"),
                   function(x) .logEntry(x)$key, character(1L))
    expect_identical(unname(sort(keys)), unname(keys))
    expect_identical(.logEntry("no stamp here"),
                     list(second = NULL, key = NA_character_,
                          text = "no stamp here"))
})

test_that("waiting gives up on a job that cannot be reached", {
    api <- fakeApi(jobRoutes(pods = list()))
    job <- newJob(api)
    expect_identical(jobStatus(job), "unknown")
    expect_error(waitJob(job), "cannot be reached")
    expect_error(waitJob(job, timeout = 0), "`timeout` must be")
    expect_error(waitJob(job, interval = Inf), "`interval` must be")

    api <- fakeApi(list())
    expect_identical(jobStatus(newJob(api)), "unknown")
})

test_that("a job whose pod does not answer runs while the Job is active", {
    api <- fakeApi(jobRoutes(status = list(active = 1L)))
    job <- newJob(api)
    expect_identical(jobStatus(job), "running")
    expect_error(waitJob(job, timeout = 1, interval = 0.2),
                 class = "biockube_job_timeout")
    api <- fakeApi(jobRoutes(pods = list(jobPod(phase = "Pending",
                                                ready = "False"))))
    expect_identical(jobStatus(newJob(api)), "waiting")
})

test_that("a released job reports how its pod ended", {
    manifest <- .jobManifest(toyJob(), "bioc", jobName, uploads = "matrix",
                             image = image)
    ended <- function(code, phase) jobPod(
        phase = phase, ready = "False",
        state = list(terminated = list(exitCode = code, reason = "Done"))
    )
    api <- fakeApi(jobRoutes(annotations = manifest$metadata$annotations,
                             status = list(active = 1L),
                             pods = list(ended(0L, "Succeeded"))))
    job <- KubeJob(jobName, namespace = "bioc", client = api$client)
    expect_identical(jobStatus(job), "succeeded")
    expect_identical(waitJob(job), job)
    expect_match(capture.output(show(job))[5L], "succeeded")
    expect_error(jobOutputs(job, dir = tempfile()),
                 "its pod no longer serves the outputs")

    api <- fakeApi(jobRoutes(status = list(active = 1L),
                             pods = list(ended(3L, "Failed"))))
    job <- newJob(api)
    expect_identical(jobStatus(job), "failed")
    failure <- expect_error(waitJob(job), class = "biockube_job_failed")
    expect_identical(failure$status, 3L)
})

test_that("a finished job is read from the Job when its pod has gone", {
    api <- fakeApi(jobRoutes(status = list(succeeded = 1L), pods = list()))
    job <- newJob(api)
    expect_identical(jobStatus(job), "succeeded")
    expect_identical(waitJob(job), job)

    api <- fakeApi(jobRoutes(pods = list(), status = list(conditions = list(
        list(type = "Failed", status = "True", reason = "DeadlineExceeded")
    ))))
    expect_identical(jobStatus(newJob(api)), "failed")
})

test_that("waiting rides out a cluster that does not answer for a while", {
    counts <- tempfile("counts-")
    dir.create(counts)
    on.exit(unlink(counts, recursive = TRUE), add = TRUE)
    routes <- jobRoutes(status = list(succeeded = 1L), pods = list())
    routes[[paste("GET", jobPath)]] <- flakyHandler(
        counts, 2L, routes[[paste("GET", jobPath)]])
    api <- fakeApi(routes)
    job <- newJob(api)
    expect_identical(waitJob(job, timeout = 30, interval = 0.1), job)
    expect_length(list.files(counts), 3L)

    routes[[paste("GET", jobPath)]] <- flakyHandler(counts, Inf, NULL)
    api <- fakeApi(routes)
    expect_error(waitJob(newJob(api), timeout = 1, interval = 0.2),
                 "did not finish within 1 seconds. The cluster did not answer",
                 class = "biockube_job_timeout")

    routes[[paste("GET", jobPath)]] <- statusHandler(
        403L, "Forbidden", "jobs.batch is forbidden")
    api <- fakeApi(routes)
    elapsed <- system.time(expect_error(
        waitJob(newJob(api), timeout = 30), class = "kube_error_forbidden"
    ))[["elapsed"]]
    expect_lt(elapsed, 10)
})

test_that("a running job is reattached by name", {
    work <- tempfile("attach-")
    dir.create(work)
    on.exit(unlink(work, recursive = TRUE), add = TRUE)
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = runnerConfig(
        c(rscript(), "-e", "invisible(NULL)"),
        uploads = c(matrix = file.path(work, "matrix.tsv")),
        outputs = c(normalized = file.path(work, "normalized.tsv"))
    )))
    on.exit(stopPod(pod), add = TRUE)
    manifest <- .jobManifest(toyJob(), "bioc", jobName, uploads = "matrix",
                             image = image)
    pods <- list(jobPod("toy-normalize-a1b2c-newer", owner = NULL,
                        created = "2025-01-02T00:00:00Z"),
                 jobPod())
    api <- fakeApi(jobRoutes(annotations = manifest$metadata$annotations,
                             pods = pods), pod = pod)

    job <- KubeJob(jobName, namespace = "bioc", client = api$client)
    expect_identical(job@pod, podName)
    expect_identical(job@uid, "job-uid")
    expect_identical(job@token, "test-token")
    expect_identical(job@outputs,
                     c(normalized = "/biocjob/outputs/normalized.tsv"))
    expect_identical(job@package, "toy")
    expect_identical(job@job, "toy-normalize")
    expect_identical(jobStatus(job), "waiting")
    expect_error(jobOutputs(job), "is waiting; wait for it to finish")

    expect_error(KubeJob("Toy_1", client = api$client), "`name` must be")
})

test_that("a job the session did not submit is not reattached", {
    api <- fakeApi(jobRoutes())
    expect_error(KubeJob(jobName, namespace = "bioc", client = api$client),
                 "has no biockube.bioconductor.org/job annotation")
    api <- fakeApi(jobRoutes(annotations = list(
        "biockube.bioconductor.org/job" = "nothex")))
    expect_error(KubeJob(jobName, namespace = "bioc", client = api$client),
                 "unreadable biockube.bioconductor.org/job annotation")

    annotations <- list("biockube.bioconductor.org/job" =
                            .encodeJobConfig("toy", "toy-normalize",
                                             character()))
    api <- fakeApi(jobRoutes(annotations = annotations,
                             secretOwner = "other-uid"))
    expect_error(KubeJob(jobName, namespace = "bioc", client = api$client),
                 "does not belong to Job")
    api <- fakeApi(jobRoutes(annotations = annotations,
                             pods = list(jobPod(owner = NULL))))
    expect_error(KubeJob(jobName, namespace = "bioc", client = api$client),
                 "has no pod")
})

test_that("deleting a job takes its pod and secret with it", {
    api <- fakeApi(jobRoutes())
    job <- newJob(api)
    expect_identical(deleteJob(job), job)
    expect_identical(requestJson(requestFor(api, "DELETE", jobPath)),
                     list(propagationPolicy = "Background",
                          preconditions = list(uid = "job-uid")))
})

test_that("deleting a job spares a later Job of the same name", {
    ## The Job of that name now has the uid "new-uid".
    conflict <- statusHandler(
        409L, "Conflict",
        paste0("Operation cannot be fulfilled on jobs.batch \"", jobName,
               "\": Precondition failed: UID in precondition: job-uid, ",
               "UID in object meta: new-uid")
    )
    deleted <- jsonHandler(list(kind = "Status", apiVersion = "v1",
                                status = "Success"))
    routes <- jobRoutes()
    routes[[paste("DELETE", jobPath)]] <- function(req, res) {
        body <- if (length(req$.body))
            jsonlite::fromJSON(rawToChar(req$.body))
        uid <- body$preconditions$uid
        if (!is.null(uid) && !identical(uid, "new-uid"))
            return(conflict(req, res))
        deleted(req, res)
    }
    api <- fakeApi(routes)
    expect_error(deleteJob(newJob(api)),
                 "is a later Job that took the name of this one")
})

test_that("the jobs BiocKube submitted are listed", {
    api <- fakeApi(jobRoutes())
    jobs <- listJobs("bioc", api$client)
    expect_s3_class(jobs, "kube_table")
    expect_true(jobName %in% jobs$NAME)
    expect_match(requestFor(api, "GET", jobsPath)$query,
                 "labelSelector=app.kubernetes.io%2Fmanaged-by%3DBiocKube",
                 fixed = TRUE)
})
