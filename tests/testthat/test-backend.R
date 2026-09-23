## A fake API server in front of worker.R processes started on this
## machine. Pods are named "<session>-<k>", so the number a pod name ends
## with picks the process a proxy request is forwarded to.

## Until the time a file "<what>-<method>-<endpoint>" in `state` holds, a
## proxy request of that method to that endpoint is answered with 503
## ("refuse"), or forwarded and then answered with 502 ("drop").
clusterProxy <- function(ports, state)
{
    force(ports)
    force(state)
    pattern <- "^/api/v1/namespaces/[^/]+/pods/([^/:]+):([0-9]+)/proxy"
    dropped <- c("authorization", "host", "content-length", "connection",
                 "accept-encoding", "expect", "user-agent")
    troubled <- function(what, req) {
        file <- file.path(state, sprintf("%s-%s-%s", what, toupper(req$method),
                                         basename(req$path)))
        file.exists(file) &&
            as.numeric(Sys.time()) < as.numeric(readLines(file, 1L))
    }
    function(req, res) {
        if (!grepl(pattern, req$path))
            return("next")
        pod <- sub(paste0(pattern, ".*$"), "\\1", req$path)
        node <- suppressWarnings(as.integer(sub(".*-([0-9]+)$", "\\1", pod)))
        if (is.na(node) || node > length(ports))
            return(res$set_status(404L)$send("no such pod"))
        if (troubled("refuse", req))
            return(res$set_status(503L)$send("service unavailable"))
        url <- paste0("http://127.0.0.1:", ports[[node]],
                      sub(pattern, "", req$path))
        if (nzchar(req$query_string))
            url <- paste0(url, "?", req$query_string)
        handle <- curl::new_handle(customrequest = toupper(req$method),
                                   timeout = 120L)
        if (length(req$.body))
            curl::handle_setopt(handle, post = TRUE,
                                postfieldsize = length(req$.body),
                                postfields = req$.body)
        headers <- req$headers[!tolower(names(req$headers)) %in% dropped]
        if (length(headers))
            curl::handle_setheaders(handle, .list = as.list(headers))
        answer <- tryCatch(curl::curl_fetch_memory(url, handle),
                           error = function(e) NULL)
        if (is.null(answer)) {
            res$set_status(500L)
            return(res$send("error dialing backend: connection refused"))
        }
        if (troubled("drop", req))
            return(res$set_status(502L)$send("bad gateway"))
        type <- curl::parse_headers_list(answer$headers)[["content-type"]]
        res$set_status(answer$status_code)
        if (!is.null(type))
            res$set_header("Content-Type", type)
        res$send(answer$content)
    }
}

## Handlers run in the app's own process and reach only what their closure
## carries, so everything they need is defined here. A pod is reported as
## OOMKilled or as exited when the test says so, and as failed when its
## process no longer answers.
sessionHandlers <- function(state, ports)
{
    force(state)
    force(ports)
    created <- function()
        sort(sub("^pod-", "", list.files(state, pattern = "^pod-")))
    sent <- function(req) {
        file <- tempfile(fileext = ".json")
        on.exit(unlink(file))
        writeBin(req$.body, file)
        kube::kube_read(file)[[1L]]$metadata$name
    }
    answers <- function(name) {
        node <- suppressWarnings(as.integer(sub(".*-([0-9]+)$", "\\1",
                                                name)))
        if (is.na(node) || node > length(ports))
            return(TRUE)
        url <- sprintf("http://127.0.0.1:%d/healthz", ports[[node]])
        !is.null(tryCatch(
            curl::curl_fetch_memory(url, curl::new_handle(timeout = 2L)),
            error = function(e) NULL
        ))
    }
    object <- function(name) {
        marked <- function(what)
            file.exists(file.path(state, paste0(what, "-", name)))
        terminated <- if (marked("failed"))
            list(exitCode = 137L, reason = "OOMKilled")
        else if (marked("exited"))
            list(exitCode = 0L, reason = "Completed")
        else if (!answers(name))
            list(exitCode = 1L, reason = "Error")
        list(
            apiVersion = "v1", kind = "Pod",
            metadata = list(name = name, namespace = "bioc"),
            status = list(
                phase = if (is.null(terminated)) "Running"
                    else if (terminated$exitCode == 0L) "Succeeded"
                    else "Failed",
                conditions = list(list(
                    type = "Ready",
                    status = if (is.null(terminated)) "True" else "False"
                )),
                containerStatuses = list(list(
                    name = "worker", restartCount = 0L,
                    state = if (is.null(terminated)) list(running = list())
                        else list(terminated = terminated)
                ))
            )
        )
    }
    list(
        pods = created,
        createdObject = function(kind, uid) {
            force(kind)
            force(uid)
            function(req, res)
                res$set_status(201L)$send_json(
                    list(apiVersion = "v1", kind = kind,
                         metadata = list(name = sent(req),
                                         namespace = "bioc", uid = uid)),
                    auto_unbox = TRUE
                )
        },
        createPod = function(req, res) {
            if (file.exists(file.path(state, "refuse")))
                return(res$set_status(500L)$send_json(
                    list(kind = "Status", apiVersion = "v1",
                         status = "Failure", reason = "InternalError",
                         code = 500L,
                         message = "the cluster is out of room"),
                    auto_unbox = TRUE
                ))
            name <- sent(req)
            file.create(file.path(state, paste0("pod-", name)))
            res$set_status(201L)$send_json(object(name), auto_unbox = TRUE)
        },
        ## The list already reports every pod as ready, so the watch that
        ## follows it has nothing to report.
        listPods = function(req, res) {
            if (grepl("watch=true", req$query_string, fixed = TRUE))
                return(res$set_status(200L)$send(""))
            res$send_json(list(kind = "PodList", apiVersion = "v1",
                               metadata = list(resourceVersion = "1"),
                               items = lapply(created(), object)),
                          auto_unbox = TRUE)
        },
        getPod = function(req, res) {
            name <- req$params$name
            if (!name %in% created())
                return(res$set_status(404L)$send_json(
                    list(kind = "Status", apiVersion = "v1",
                         status = "Failure", reason = "NotFound",
                         code = 404L,
                         message = paste0("pods \"", name, "\" not found")),
                    auto_unbox = TRUE
                ))
            res$send_json(object(name), auto_unbox = TRUE)
        },
        deleted = function(req, res)
            res$send_json(list(kind = "Status", apiVersion = "v1",
                               status = "Success"), auto_unbox = TRUE)
    )
}

clusterApi <- function(pods, namespace = "bioc",
                       .local_envir = parent.frame())
{
    skip_if_not_installed("webfakes")
    requests <- tempfile("api-")
    state <- tempfile("pods-")
    dir.create(requests)
    dir.create(state)
    ports <- vapply(pods, function(pod) pod$port, integer(1L))
    handlers <- sessionHandlers(state, ports)
    app <- webfakes::new_app()
    app$locals$requests <- requests
    app$use(recordApiRequest)
    app$use(clusterProxy(ports, state))
    prefix <- paste0("/api/v1/namespaces/", namespace)
    app$post(paste0(prefix, "/configmaps"),
             handlers$createdObject("ConfigMap", "cm-uid"))
    app$post(paste0(prefix, "/secrets"),
             handlers$createdObject("Secret", "secret-uid"))
    app$post(paste0(prefix, "/pods"), handlers$createPod)
    app$get(paste0(prefix, "/pods"), handlers$listPods)
    app$get(paste0(prefix, "/pods/:name"), handlers$getPod)
    app$delete(paste0(prefix, "/configmaps/:name"), handlers$deleted)
    app$delete(paste0(prefix, "/secrets/:name"), handlers$deleted)
    routes <- discoveryRoutes()
    for (route in names(routes)) {
        parts <- strsplit(route, " ", fixed = TRUE)[[1L]]
        app[[tolower(parts[[1L]])]](parts[[2L]], routes[[route]])
    }
    process <- webfakes::local_app_process(app, .local_envir = .local_envir)
    url <- sub("/$", "", process$url())
    list(
        url = url, client = kubeClient(url, namespace),
        requests = function() apiRequests(requests),
        pods = handlers$pods,
        fail = function(name)
            file.create(file.path(state, paste0("failed-", name))),
        exit = function(name)
            file.create(file.path(state, paste0("exited-", name))),
        refuse = function() file.create(file.path(state, "refuse")),
        ## Proxy requests `method endpoint` fail for `seconds` from now.
        trouble = function(what, method, endpoint, seconds)
            writeLines(format(as.numeric(Sys.time()) + seconds, nsmall = 3L),
                       file.path(state, sprintf("%s-%s-%s", what, method,
                                                endpoint)))
    )
}

## The token a session draws must be the one the pods were started with,
## and its ids are easier to assert on when they count up.
fixedTokens <- function(token)
{
    force(token)
    drawn <- 0L
    function(bytes = 32L) {
        if (bytes == 32L)
            return(token)
        drawn <<- drawn + 1L
        sprintf("%08x", drawn)
    }
}

## `idle` gives each pod process its idle timeout; the param's own is not
## passed to the processes.
localSession <- function(workers = 2L, ..., token = "session-token",
                         idle = rep(3600, workers),
                         .local_envir = parent.frame())
{
    pods <- lapply(seq_len(workers), function(i)
        startPod("worker.R", token = token,
                 env = c(BIOCKUBE_IDLE = format(idle[[i]]))))
    local_mocked_bindings(.newToken = fixedTokens(token),
                          .package = "BiocKube", .env = .local_envir)
    api <- clusterApi(pods, .local_envir = .local_envir)
    list(
        pods = pods, api = api,
        names = sprintf("biockube-00000001-%d", seq_len(workers)),
        ## testthat keeps the test environment in an option of its own,
        ## and a task carrying the session's options would drag the
        ## package the tests run in along with it.
        param = KubeParam(workers = workers, namespace = "bioc",
                          client = api$client, exportglobals = FALSE, ...)
    )
}

stopSession <- function(session)
{
    if (bpisup(session$param))
        suppressWarnings(try(bpstop(session$param), silent = TRUE))
    for (pod in session$pods)
        stopPod(pod)
    invisible(NULL)
}

## An EXEC message as bploop builds it, for the tests that drive the
## backend without going through bplapply().
execMessage <- function(X, FUN, tag = 1L)
{
    environment(FUN) <- globalenv()
    BiocParallel:::.EXEC(
        tag, BiocParallel:::.workerLapply,
        args = list(
            X = X, FUN = FUN, ARGS = list(),
            OPTIONS = BiocParallel:::.workerOptions(exportglobals = FALSE),
            BPRNGSEED = BiocParallel:::.rng_init_stream(1L),
            GLOBALS = list(), PACKAGES = character()
        ),
        static.fun = TRUE, static.args = c("FUN", "ARGS", "OPTIONS", "GLOBALS")
    )
}

## A function that travels to a pod: with the test environment as its
## closure, serializing it warns about the package the tests run in.
global <- function(FUN)
{
    environment(FUN) <- globalenv()
    FUN
}

requestLines <- function(api)
{
    vapply(api$requests(), function(request)
        paste(request$method, request$path), character(1L))
}

lastCreated <- function(api, path)
{
    created <- Filter(function(request)
        identical(request$method, "POST") && identical(request$path, path),
        api$requests())
    requestBody(created[[length(created)]])
}

sentTasks <- function(api)
{
    tasks <- Filter(function(request) endsWith(request$path, "/proxy/task"),
                    api$requests())
    lapply(tasks, function(request) unserialize(request$body))
}

test_that("a session creates its pods and removes them again", {
    session <- localSession(2L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param

    expect_identical(bpstart(param), param)
    expect_true(bpisup(param))
    expect_error(bpstart(param), "already running")
    expect_identical(session$api$pods(), session$names)

    lines <- requestLines(session$api)
    expect_true("POST /api/v1/namespaces/bioc/configmaps" %in% lines)
    expect_true("POST /api/v1/namespaces/bioc/secrets" %in% lines)
    expect_identical(sum(lines == "POST /api/v1/namespaces/bioc/pods"), 2L)

    pod <- lastCreated(session$api, "/api/v1/namespaces/bioc/pods")
    expect_identical(pod$metadata$name, session$names[[2L]])
    expect_identical(pod$metadata$labels[["biockube.bioconductor.org/session"]],
                     "00000001")
    expect_identical(pod$metadata$ownerReferences[[1L]]$name,
                     "biockube-00000001")
    expect_identical(pod$spec$containers[[1L]]$image,
                     .defaultImage(NULL))

    expect_identical(bpstop(param), param)
    expect_false(bpisup(param))
    expect_identical(waitExit(session$pods[[1L]]), 0L)
    expect_identical(waitExit(session$pods[[2L]]), 0L)

    lines <- requestLines(session$api)
    expect_true(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-00000001" %in%
            lines
    )
    expect_true(
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-00000001" %in% lines
    )

    expect_identical(bpstop(param), param)
    expect_false(bpisup(param))
})

test_that("bplapply runs on the pods and keeps its results in order", {
    session <- localSession(3L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    expect_identical(bplapply(1:7, global(function(i) i * 2L),
                              BPPARAM = param),
                     as.list(seq(2L, 14L, by = 2L)))
    expect_length(sentTasks(session$api), 3L)

    endpoints <- vapply(session$api$requests(), function(request)
        paste(request$method, basename(request$path)), character(1L))
    expect_identical(sum(endpoints == "POST task"), 3L)
    expect_identical(sum(endpoints == "DELETE result"), 3L)

    bpstop(param)
})

test_that("the static part of a task is sent once per pod and run", {
    session <- localSession(1L, tasks = 4L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    double <- global(function(i) i + 1L)
    expect_identical(bplapply(1:4, double, BPPARAM = param), as.list(2:5))
    tasks <- sentTasks(session$api)
    expect_length(tasks, 4L)
    expect_null(tasks[[1L]]$msg$dynamic.only)
    expect_true(all(vapply(tasks[-1L], function(task)
        isTRUE(task$msg$dynamic.only), logical(1L))))
    expect_true(all(vapply(tasks, function(task)
        identical(task$run, tasks[[1L]]$run), logical(1L))))

    ## Another call is another run, which the pod has nothing cached for.
    bplapply(1:4, double, BPPARAM = param)
    again <- sentTasks(session$api)[-seq_len(4L)]
    expect_null(again[[1L]]$msg$dynamic.only)
    expect_false(identical(again[[1L]]$run, tasks[[1L]]$run))

    bpstop(param)
})

test_that("a pod that kept no static part is sent the whole task again", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    task <- execMessage(list(1, 2), function(i) i * 3)
    backend$static[[1L]] <- .task_const(task)
    expect_true(.send_to(backend, 1L, task))
    expect_identical(.recv_any(backend)$value$value, list(3, 6))

    sent <- sentTasks(session$api)
    expect_length(sent, 2L)
    expect_true(isTRUE(sent[[1L]]$msg$dynamic.only))
    expect_null(sent[[2L]]$msg$dynamic.only)

    bpstop(param)
})

test_that("an error in the function comes back for the element that failed", {
    session <- localSession(2L, tasks = 4L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    failing <- global(function(i) if (i == 2L) stop("no good") else i)
    result <- bptry(bplapply(1:4, failing, BPPARAM = param))
    expect_false(bpok(result)[[2L]])
    expect_s3_class(result[[2L]], "remote_error")
    expect_match(conditionMessage(result[[2L]]), "no good")
    expect_identical(result[[1L]], 1L)

    bpstopOnError(param) <- FALSE
    result <- bptry(bplapply(1:4, failing, BPPARAM = param))
    expect_identical(bpok(result), c(TRUE, FALSE, TRUE, TRUE))
    expect_identical(result[c(1L, 3L, 4L)], list(1L, 3L, 4L))

    bpstop(param)
})

test_that("BPREDO runs the elements that failed once more", {
    session <- localSession(2L, tasks = 4L, stop.on.error = FALSE)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    marker <- tempfile()
    once <- global(function(i, marker) {
        if (i == 3L && !file.exists(marker)) {
            file.create(marker)
            stop("not yet")
        }
        i * 10L
    })
    first <- bptry(bplapply(1:4, once, marker = marker, BPPARAM = param))
    expect_identical(bpok(first), c(TRUE, TRUE, FALSE, TRUE))
    expect_identical(bplapply(1:4, once, marker = marker, BPPARAM = param,
                              BPREDO = first),
                     list(10L, 20L, 30L, 40L))

    bpstop(param)
})

test_that("bpiterate sends one task per value", {
    session <- localSession(2L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    values <- 1:5
    seen <- 0L
    iter <- function() {
        seen <<- seen + 1L
        if (seen > length(values)) NULL else values[[seen]]
    }
    expect_identical(bpiterate(iter, global(function(x) x * 3L),
                               BPPARAM = param),
                     as.list(seq(3L, 15L, by = 3L)))
    expect_length(sentTasks(session$api), 5L)

    bpstop(param)
})

test_that("a backend collects every task still in flight", {
    session <- localSession(2L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    .send_all(backend, execMessage(list(1, 2), function(i) i + 1))
    results <- .recv_all(backend)
    expect_length(results, 2L)
    nodes <- vapply(results, function(result) result$node, integer(1L))
    expect_setequal(nodes, c(1L, 2L))
    expect_identical(lapply(results, function(result) result$value$value),
                     list(list(2, 3), list(2, 3)))
    expect_false(any(.inflightNodes(backend)))
    expect_identical(.recv_all(backend), list())

    bpstop(param)
})

test_that("a new run cancels what the pods were still doing", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)
    first <- backend$run

    expect_true(.send_to(backend, 1L, execMessage(list(1), function(i) {
        Sys.sleep(30)
        i
    })))
    expect_match(waitStatus(session$pods[[1L]], "busy"), "busy")

    .startRun(backend)
    expect_false(identical(backend$run, first))
    expect_false(any(.inflightNodes(backend)))
    expect_null(backend$static[[1L]])
    expect_match(waitStatus(session$pods[[1L]], "idle"), "idle")

    bpstop(param)
})

test_that("a session that cannot be started leaves nothing behind", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    session$api$refuse()

    ## A param without a namespace uses the client's.
    param <- KubeParam(workers = 1L, client = session$api$client)
    expect_error(bpstart(param), "out of room")
    expect_false(bpisup(param))

    lines <- requestLines(session$api)
    expect_true(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-00000001" %in%
            lines
    )
    expect_true(
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-00000001" %in% lines
    )
})

test_that("workers running other versions than the session are reported", {
    backend <- .kubeBackend(NULL, "bioc", list(), c("w-1", "w-2"), 8080L,
                            "token")
    reported <- list(
        r = as.character(getRversion()),
        biocparallel = as.character(packageVersion("BiocParallel"))
    )
    local_mocked_bindings(
        .podRequest = function(backend, node, path, ...)
            c(list(state = "idle"), reported),
        .package = "BiocKube"
    )
    expect_silent(.warnVersions(backend))

    reported$biocparallel <- "1.20.0"
    expect_warning(.warnVersions(backend),
                   "BiocParallel 1.20.0; this session")
    reported$r <- NULL
    expect_warning(.warnVersions(backend), "R an unknown version")
})

test_that("a pod that dies while holding a task answers for every element", {
    session <- localSession(2L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    slow <- execMessage(list(1, 2, 3), function(i) {
        Sys.sleep(30)
        i
    })
    expect_true(.send_to(backend, 1L, slow))
    expect_match(waitStatus(session$pods[[1L]], "busy"), "busy")
    stopPod(session$pods[[1L]])
    session$api$fail(session$names[[1L]])

    result <- .recv_any(backend)
    expect_identical(result$node, 1L)
    expect_false(result$value$success)
    expect_identical(result$value$tag, 1L)
    expect_length(result$value$value, 3L)
    expect_s3_class(result$value$value[[1L]], "worker_comm_error")
    expect_match(conditionMessage(result$value$value[[1L]]), "OOMKilled")
    expect_true(backend$lost[[1L]])

    bpstop(param)
})

test_that("a run starts without a pod that is gone, and says so", {
    session <- localSession(2L, tasks = 4L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    stopPod(session$pods[[2L]])
    session$api$fail(session$names[[2L]])

    expect_warning(
        result <- bplapply(1:4, global(function(i) i), BPPARAM = param),
        paste("Worker pod bioc/biockube-00000001-2 is lost: the pod failed",
              "with OOMKilled. This run uses the other 1")
    )
    expect_identical(result, as.list(1:4))
    expect_identical(bpbackend(param)$lost, c(FALSE, TRUE))
    expect_silent(result <- bplapply(1:4, global(function(i) i + 1L),
                                     BPPARAM = param))
    expect_identical(result, as.list(2:5))
    tasks <- sentTasks(session$api)
    expect_length(tasks, 8L)

    expect_identical(bpstop(param), param)
    expect_false(bpisup(param))
    expect_true(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-00000001" %in%
            requestLines(session$api)
    )
})

test_that("a pod lost during a run gets no further task", {
    session <- localSession(2L, tasks = 20L, stop.on.error = FALSE)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    ## Element 2 ends the worker process it runs under, and the task itself.
    fatal <- global(function(i) {
        if (i == 2L) {
            stat <- strsplit(readLines("/proc/self/stat"), " ")[[1L]]
            tools::pskill(as.integer(stat[[4L]]), tools::SIGKILL)
            tools::pskill(Sys.getpid(), tools::SIGKILL)
        }
        Sys.sleep(0.05)
        i
    })
    result <- bptry(bplapply(1:20, fatal, BPPARAM = param))
    expect_identical(which(!bpok(result)), 2L)
    expect_s3_class(result[[2L]], "worker_comm_error")
    expect_match(conditionMessage(result[[2L]]),
                 "worker pod bioc/biockube-00000001-[12]: the pod failed")
    expect_identical(result[-2L], as.list((1:20)[-2L]))
    expect_identical(sum(bpbackend(param)$lost), 1L)

    ## The next run, and BPREDO, go to the pod that is left.
    bpstopOnError(param) <- TRUE
    expect_identical(bplapply(1:20, global(function(i) i), BPPARAM = param,
                              BPREDO = result),
                     as.list(1:20))
    expect_identical(bplapply(1:3, global(function(i) -i), BPPARAM = param),
                     list(-1L, -2L, -3L))

    bpstop(param)
})

test_that("a run with no pod left asks for a restart", {
    session <- localSession(2L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    for (node in 1:2) {
        stopPod(session$pods[[node]])
        session$api$fail(session$names[[node]])
    }

    error <- expect_error(
        bplapply(1:2, global(function(i) i), BPPARAM = param),
        "No worker pod of this KubeParam is left; restart them with bpstop"
    )
    expect_match(conditionMessage(error),
                 "bioc/biockube-00000001-2: the pod failed with OOMKilled")
    expect_true(bpisup(param))
    expect_identical(bpstop(param), param)
    expect_false(bpisup(param))
})

test_that("pods that exited on their idle timeout are reported", {
    session <- localSession(2L, idle = c(3600, 8))
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    expect_identical(waitExit(session$pods[[2L]], timeout = 30), 0L)
    session$api$exit(session$names[[2L]])

    expect_warning(
        result <- bplapply(1:4, global(function(i) i), BPPARAM = param),
        paste("biockube-00000001-2 is lost: the pod has exited, as a worker",
              "does after 3600 seconds without a request \\(idleTimeout\\)")
    )
    expect_identical(result, as.list(1:4))

    stopPod(session$pods[[1L]])
    session$api$exit(session$names[[1L]])
    expect_error(bplapply(1:4, global(function(i) i), BPPARAM = param),
                 "No worker pod of this KubeParam is left")
    bpstop(param)
})

test_that("a run asks every pod for its state first", {
    session <- localSession(2L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)

    bplapply(1:2, global(function(i) i), BPPARAM = param)
    probes <- Filter(function(line) endsWith(line, "/proxy/status"),
                     requestLines(session$api))
    ## Two from bpstart(), which compares versions, and two from the run.
    expect_length(probes, 4L)

    bpstop(param)
})

test_that("a run keeps the pods it gives no task from idling out", {
    session <- localSession(2L, idle = c(3600, 6))
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    ## The fake API server answers one request at a time, so polls are kept
    ## short for the other requests to get through.
    local_mocked_bindings(.pollWait = 1, .package = "BiocKube")
    backend <- bpbackend(param)
    .startRun(backend)
    backend$idle <- 2
    backend$aliveAt <- .now()

    .send_to(backend, 1L, execMessage(list(1), function(i) {
        Sys.sleep(10)
        i
    }))
    expect_identical(.recv_any(backend)$value$value, list(1))
    expect_true(session$pods[[2L]]$proc$is_alive())

    bpstop(param)
})

test_that("pods are kept up while the tasks of a run are sent", {
    session <- localSession(3L, idle = c(3600, 3600, 6))
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    backend$idle <- 2
    ## Tasks that take four seconds each to upload to the first two pods.
    post <- .tryPost
    local_mocked_bindings(
        .tryPost = function(backend, node, value) {
            if (node < 3L)
                Sys.sleep(4)
            post(backend, node, value)
        },
        .package = "BiocKube"
    )

    result <- bplapply(1:3, global(function(i) i), BPPARAM = param)
    expect_identical(result, as.list(1:3))
    expect_false(any(bpbackend(param)$lost))
    expect_true(session$pods[[3L]]$proc$is_alive())

    bpstop(param)
})

test_that("an interrupted bpstop leaves a param that runs as usual", {
    param <- KubeParam(workers = 2L, namespace = "bioc", image = "me/image:1",
                       client = kubeClient("http://127.0.0.1:1"))
    param$backend <- .kubeBackend(NULL, "bioc", list(configMap = "biockube-9"),
                                  c("biockube-9-1", "biockube-9-2"), 8080L,
                                  "token")
    ## As a user's Ctrl-C would, while the pods are told to exit.
    local_mocked_bindings(.tolerate = function(expr, action) {
        tools::pskill(Sys.getpid(), tools::SIGINT)
        Sys.sleep(5)
    }, .package = "BiocKube")
    outcome <- tryCatch(bpstop(param), interrupt = function(e) "interrupted")
    expect_identical(outcome, "interrupted")
    expect_true(bpisup(param))
    expect_null(bpbackend(param)$deadline)

    started <- FALSE
    local_mocked_bindings(.startRun = function(backend) started <<- TRUE,
                          .reportLost = function(...) NULL,
                          .package = "BiocKube")
    .manager(param)
    expect_true(started)
})

test_that("a result of another task sends the task again", {
    backend <- .kubeBackend(NULL, "bioc", list(configMap = "biockube-1"),
                            "biockube-1-1", 8080L, "token")
    backend$run <- "r1"
    backend$inflight[[1L]] <- list(tag = 2L, size = 3L, run = "r1",
                                   sent = TRUE)

    ## The result of the task before, whose acknowledgement was lost.
    .received(backend, 1L, serialize(list(run = "r1", tag = 1L,
                                          value = "ignored"), NULL))
    expect_false(backend$lost[[1L]])
    expect_true(backend$acks[[1L]])
    expect_false(backend$inflight[[1L]]$sent)
    expect_identical(backend$retryAt[[1L]], 0)
    expect_length(backend$inbox, 0L)

    ## A pod that goes on answering with other results is given up.
    backend$troubleSince[[1L]] <- .now() - .troubleWindow - 1
    backend$inflight[[1L]]$sent <- TRUE
    .received(backend, 1L, serialize(list(run = "r0", tag = 2L,
                                          value = "ignored"), NULL))
    expect_true(backend$lost[[1L]])
    result <- .takeResult(backend)
    expect_identical(result$node, 1L)
    expect_identical(result$value$tag, 2L)
    expect_length(result$value$value, 3L)
    expect_match(conditionMessage(result$value$value[[1L]]),
                 "lost track of the run")
})

test_that("a task nobody will answer fails every one of its elements", {
    backend <- .kubeBackend(NULL, "bioc", list(configMap = "biockube-1"),
                            c("biockube-1-1", "biockube-1-2"), 8080L, "token")
    backend$inflight[[2L]] <- list(tag = 5L, size = 4L, run = "r1")

    .failTask(backend, 2L, simpleError("connection refused"),
              "the pod is gone")
    expect_null(backend$inflight[[2L]])
    result <- .takeResult(backend)
    expect_identical(result$node, 2L)
    expect_identical(result$value$tag, 5L)
    expect_length(result$value$value, 4L)
    expect_false(any(bpok(result$value$value)))
    expect_match(conditionMessage(result$value$value[[4L]]),
                 "worker pod bioc/biockube-1-2: the pod is gone")
    expect_match(conditionMessage(result$value$value[[4L]]),
                 "connection refused")

    expect_error(.recv_any(backend), "No task is running")
})

test_that("a pod the API server has lost is named in the error", {
    expect_identical(.podTrouble(list(exists = FALSE)), "the pod is gone")
    expect_identical(.podTrouble(list(exists = TRUE, reason = "OOMKilled")),
                     "the pod failed with OOMKilled")
    expect_identical(.podTrouble(list(exists = TRUE, reason = NA_character_)),
                     "the pod failed")
    exited <- list(exists = TRUE, phase = "Succeeded", failed = FALSE,
                   reason = NA_character_)
    expect_identical(.podTrouble(exited), "the pod has exited")
    expect_identical(.podTrouble(exited, idle = 60), paste(
        "the pod has exited, as a worker does after 60 seconds without a",
        "request (idleTimeout)"
    ))
})

## A backend whose pod requests and pod status are given by the test.
troubledBackend <- function(sent = TRUE)
{
    backend <- .kubeBackend(NULL, "bioc", list(configMap = "biockube-1"),
                            "biockube-1-1", 8080L, "token")
    backend$run <- "r1"
    backend$inflight[[1L]] <- list(
        tag = 1L, size = 2L, run = "r1", sent = sent,
        value = execMessage(list(1, 2), function(i) i),
        error = if (!sent) simpleError("connection reset")
    )
    backend
}

response <- function(status, text = "")
{
    list(status_code = status, content = charToRaw(text))
}

test_that("a pod that answers again is not counted against", {
    backend <- troubledBackend()
    local_mocked_bindings(
        .podRequest = function(...) stop(.podError(503L, "unavailable",
                                                   "bioc", "biockube-1-1")),
        .podStatus = function(...) list(exists = TRUE, phase = "Running",
                                        failed = FALSE, reason = NA),
        .package = "BiocKube"
    )
    for (blip in 1:5) {
        .polled(backend, 1L, response(503L, "service unavailable"))
        .resolveTrouble(backend)
        expect_false(backend$lost[[1L]])
        expect_identical(backend$failures[[1L]], 1L)
        expect_gt(backend$retryAt[[1L]], .now())
        for (poll in 1:3)
            .polled(backend, 1L, response(204L))
        expect_identical(backend$failures[[1L]], 0L)
        expect_true(is.na(backend$troubleSince[[1L]]))
    }
    expect_length(backend$inbox, 0L)
})

test_that("a failing pod is retried later and later, then given up", {
    backend <- troubledBackend()
    reachable <- TRUE
    local_mocked_bindings(
        .podRequest = function(...) stop(.podError(503L, "unavailable",
                                                   "bioc", "biockube-1-1")),
        .podStatus = function(...) {
            if (!reachable)
                stop("the API server is down")
            list(exists = TRUE, phase = "Running", failed = FALSE,
                 reason = NA)
        },
        .addRequest = function(...) stop("no poll is sent while waiting"),
        .package = "BiocKube"
    )
    delays <- numeric()
    for (i in 1:6) {
        backend$trouble[[1L]] <- simpleError("connection reset")
        .resolveTrouble(backend)
        delays <- c(delays, backend$retryAt[[1L]] - .now())
        .armPolls(backend)
    }
    expect_identical(round(delays), c(1, 2, 4, 8, 10, 10))
    expect_false(backend$lost[[1L]])

    ## Without the API server nothing is known about the pod, so it is not
    ## given up, however long that lasts.
    reachable <- FALSE
    backend$troubleSince[[1L]] <- .now() - .troubleWindow - 1
    backend$trouble[[1L]] <- simpleError("connection reset")
    .resolveTrouble(backend)
    expect_false(backend$lost[[1L]])

    reachable <- TRUE
    backend$trouble[[1L]] <- simpleError("connection reset")
    .resolveTrouble(backend)
    expect_true(backend$lost[[1L]])
    expect_false(backend$ended[[1L]])
    result <- .takeResult(backend)
    expect_match(conditionMessage(result$value$value[[1L]]),
                 "the pod stopped answering")
})

test_that("a refused poll is checked with a request that can refresh", {
    backend <- troubledBackend()
    local_mocked_bindings(
        .podRequest = function(backend, node, path, ...)
            list(state = "busy", run = "r1", tag = 1L),
        .package = "BiocKube"
    )
    .polled(backend, 1L, response(401L, '{"kind":"Status","code":401}'))
    .resolveTrouble(backend)
    expect_false(backend$lost[[1L]])
    expect_false(is.null(backend$inflight[[1L]]))
    expect_true(is.na(backend$troubleSince[[1L]]))

    local_mocked_bindings(
        .podRequest = function(...) stop(.podError(401L, "Unauthorized",
                                                   "bioc", "biockube-1-1")),
        .package = "BiocKube"
    )
    .polled(backend, 1L, response(401L, '{"kind":"Status","code":401}'))
    .resolveTrouble(backend)
    expect_true(backend$lost[[1L]])
    result <- .takeResult(backend)
    expect_match(conditionMessage(result$value$value[[1L]]),
                 "the request was refused")
})

test_that("a pod that dropped its task fails the task, not the pod", {
    backend <- troubledBackend()
    local_mocked_bindings(
        .podRequest = function(backend, node, path, ...)
            list(state = "idle"),
        .package = "BiocKube"
    )
    .polled(backend, 1L, response(404L, "no task"))
    .resolveTrouble(backend)
    expect_false(backend$lost[[1L]])
    result <- .takeResult(backend)
    expect_match(conditionMessage(result$value$value[[1L]]),
                 "the pod does not hold the task")
})

test_that("a finished result that cannot be fetched fails after a while", {
    backend <- troubledBackend()
    local_mocked_bindings(
        .podRequest = function(backend, node, path, ...)
            list(state = "done", run = "r1", tag = 1L),
        .package = "BiocKube"
    )
    backend$trouble[[1L]] <- simpleError("the transfer stalled")
    .resolveTrouble(backend)
    expect_false(is.na(backend$troubleSince[[1L]]))
    expect_gt(backend$retryAt[[1L]], .now())
    expect_length(backend$inbox, 0L)

    backend$troubleSince[[1L]] <- .now() - .troubleWindow - 1
    backend$trouble[[1L]] <- simpleError("the transfer stalled")
    .resolveTrouble(backend)
    expect_false(backend$lost[[1L]])
    expect_true(backend$acks[[1L]])
    result <- .takeResult(backend)
    expect_match(conditionMessage(result$value$value[[1L]]),
                 "its result could not be fetched")
})

test_that("a result slower to download than a poll's wait comes through", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    ## About five seconds for the result, against a wait of one second and
    ## requests of one second. The result must come with the first poll.
    handle <- .proxyHandle
    local_mocked_bindings(
        .pollWait = 1, .requestTimeout = 1,
        .proxyHandle = function(...) {
            request <- handle(...)
            curl::handle_setopt(request$handle,
                                max_recv_speed_large = 200000)
            request
        },
        .backOff = function(...) stop("a poll failed"),
        .package = "BiocKube"
    )
    .send_to(backend, 1L, execMessage(list(1), function(i) raw(1e6)))
    result <- .recv_any(backend)
    expect_true(result$value$success)
    expect_length(result$value$value[[1L]], 1e6)
    expect_false(backend$lost[[1L]])

    bpstop(param)
})

test_that("a task the pod does not take is tried again, unless refused", {
    backend <- troubledBackend(sent = FALSE)
    answer <- NULL
    local_mocked_bindings(
        .podRequest = function(backend, node, path, ...) {
            if (path == "status")
                return(list(state = "idle"))
            stop(answer)
        },
        .package = "BiocKube"
    )
    answer <- .podError(503L, "unavailable", "bioc", "biockube-1-1")
    .deliver(backend, 1L)
    expect_false(backend$inflight[[1L]]$sent)
    expect_gt(backend$retryAt[[1L]], .now())
    expect_length(backend$inbox, 0L)

    answer <- .podError(400L, "cannot read task", "bioc", "biockube-1-1")
    .deliver(backend, 1L)
    expect_false(backend$lost[[1L]])
    result <- .takeResult(backend)
    expect_match(conditionMessage(result$value$value[[1L]]),
                 "the task could not be sent")
})

test_that("a task posted while the API server is down is sent again", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    session$api$trouble("refuse", "POST", "task", 3)
    session$api$trouble("refuse", "GET", "status", 3)
    .send_to(backend, 1L, execMessage(list(3), function(i) i + 1))
    expect_false(backend$inflight[[1L]]$sent)
    result <- .recv_any(backend)
    expect_identical(result$value$value, list(4))
    expect_false(backend$lost[[1L]])
    expect_length(sentTasks(session$api), 2L)

    bpstop(param)
})

test_that("a lost acknowledgement does not lose the pod", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    session$api$trouble("refuse", "DELETE", "result", 30)
    .send_to(backend, 1L, execMessage(list(1), function(i) i))
    expect_identical(.recv_any(backend)$value$value, list(1))

    session$api$trouble("refuse", "POST", "task", 3)
    session$api$trouble("refuse", "GET", "status", 3)
    .send_to(backend, 1L, execMessage(list(2), function(i) i * 10,
                                      tag = 2L))
    result <- .recv_any(backend)
    expect_identical(result$value$tag, 2L)
    expect_identical(result$value$value, list(20))
    expect_false(backend$lost[[1L]])

    bpstop(param)
})

test_that("results come through an API server that fails for a while", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    .send_to(backend, 1L, execMessage(list(1, 2), function(i) {
        Sys.sleep(4)
        i * 2
    }))
    session$api$trouble("refuse", "GET", "result", 3)
    result <- .recv_any(backend)
    expect_identical(result$value$value, list(2, 4))
    expect_false(backend$lost[[1L]])
    polls <- Filter(function(line) endsWith(line, "/proxy/result"),
                    requestLines(session$api))
    expect_gte(length(polls), 2L)

    bpstop(param)
})

test_that("a task whose acceptance was lost on the way back runs once", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    session$api$trouble("drop", "POST", "task", 30)
    .send_to(backend, 1L, execMessage(list(3), function(i) i + 1))
    result <- .recv_any(backend)
    expect_identical(result$value$value, list(4))
    expect_false(backend$lost[[1L]])
    expect_length(sentTasks(session$api), 1L)

    bpstop(param)
})

test_that("a pod still busy with an interrupted run is cancelled first", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    bpstart(param)
    backend <- bpbackend(param)
    .startRun(backend)

    pod <- session$pods[[1L]]
    stale <- podRequest(pod, "POST", "/task",
                        workerTask(list(1), function(i) Sys.sleep(60),
                                   run = "old"))
    expect_identical(stale$status, 202L)

    .send_to(backend, 1L, execMessage(list(5), function(i) i * i))
    result <- .recv_any(backend)
    expect_identical(result$value$value, list(25))
    expect_false(backend$lost[[1L]])
    endpoints <- vapply(session$api$requests(), function(request)
        paste(request$method, basename(request$path)), character(1L))
    expect_identical(sum(endpoints == "POST task"), 2L)
    expect_identical(sum(endpoints == "POST cancel"), 1L)

    bpstop(param)
})

test_that("an interrupted bpstart removes what it created", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- session$param
    ## As a user's Ctrl-C would, while the pods are being waited for.
    local_mocked_bindings(.waitReady = function(...) {
        tools::pskill(Sys.getpid(), tools::SIGINT)
        Sys.sleep(5)
    }, .package = "BiocKube")

    outcome <- tryCatch(bpstart(param), interrupt = function(e) "interrupted")
    expect_identical(outcome, "interrupted")
    expect_false(bpisup(param))
    lines <- requestLines(session$api)
    expect_true("POST /api/v1/namespaces/bioc/pods" %in% lines)
    expect_true(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-00000001" %in%
            lines
    )
    expect_true(
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-00000001" %in% lines
    )
})

test_that("a started param that is dropped is stopped by its finalizer", {
    session <- localSession(1L)
    on.exit(stopSession(session), add = TRUE)
    param <- KubeParam(workers = 1L, namespace = "bioc",
                       client = session$api$client, exportglobals = FALSE)
    bpstart(param)
    expect_identical(param$.finalizer_env$self, param)

    rm(param)
    gc()
    expect_identical(waitExit(session$pods[[1L]]), 0L)
    expect_true(
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-00000001" %in%
            requestLines(session$api)
    )
})

test_that("stopping gives up on an API server that stalls", {
    stalled <- function(req, res) {
        Sys.sleep(8)
        res$set_status(204L)$send("")
    }
    api <- fakeApi(list(
        "POST /api/v1/namespaces/bioc/pods/:pod/proxy/done" = stalled,
        "DELETE /api/v1/namespaces/bioc/configmaps/biockube-9" = stalled,
        "DELETE /api/v1/namespaces/bioc/secrets/biockube-9" = stalled
    ))
    param <- KubeParam(workers = 2L, namespace = "bioc", client = api$client)
    param$backend <- .kubeBackend(api$client, "bioc",
                                  list(configMap = "biockube-9"),
                                  c("biockube-9-1", "biockube-9-2"), 8080L,
                                  "token")
    local_mocked_bindings(.stopTimeout = 3, .package = "BiocKube")

    started <- Sys.time()
    expect_warning(
        expect_warning(bpstop(param), "Could not delete configmaps"),
        "Could not delete secrets"
    )
    expect_lt(as.numeric(Sys.time() - started, units = "secs"), 7)
    expect_false(bpisup(param))
    expect_true(
        "POST /api/v1/namespaces/bioc/pods/biockube-9-1:8080/proxy/done" %in%
            requestLines(api)
    )
})
