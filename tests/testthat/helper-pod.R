## Pod scripts run as local Rscript processes on a free port.

podScript <- function(name)
{
    system.file("pod", name, package = "BiocKube", mustWork = TRUE)
}

## Sources pod scripts into a new environment without starting a server.
podSource <- function(...)
{
    env <- new.env()
    for (name in c(...))
        sys.source(podScript(name), envir = env)
    env
}

freePort <- function()
{
    for (i in seq_len(20L)) {
        port <- sample(20000:40000, 1L)
        server <- tryCatch(serverSocket(port), error = function(e) NULL)
        if (!is.null(server)) {
            close(server)
            return(port)
        }
    }
    stop("no free port found")
}

startPod <- function(script, env = character(), token = "test-token",
                     ready = TRUE)
{
    skip_on_os("windows")
    skip_if_not_installed("processx")
    port <- freePort()
    log <- tempfile(fileext = ".log")
    proc <- processx::process$new(
        file.path(R.home("bin"), "Rscript"), podScript(script),
        env = c("current", BIOCKUBE_PORT = port, BIOCKUBE_TOKEN = token, env),
        stdout = log, stderr = "2>&1", cleanup_tree = TRUE
    )
    pod <- list(proc = proc, port = port, token = token, log = log)
    if (ready)
        waitReady(pod)
    pod
}

stopPod <- function(pod)
{
    if (pod$proc$is_alive())
        pod$proc$kill_tree()
    unlink(pod$log)
}

podLog <- function(pod)
{
    paste(readLines(pod$log, warn = FALSE), collapse = "\n")
}

podRequest <- function(pod, method, path, body = NULL, token = pod$token,
                       timeout = 30)
{
    handle <- curl::new_handle(customrequest = method, timeout = timeout)
    if (!is.null(body))
        curl::handle_setopt(handle, post = TRUE,
                            postfieldsize = length(body), postfields = body)
    if (!is.null(token))
        curl::handle_setheaders(handle, "X-BiocKube-Token" = token)
    url <- sprintf("http://127.0.0.1:%d%s", pod$port, path)
    res <- curl::curl_fetch_memory(url, handle = handle)
    list(status = res$status_code, body = res$content,
         headers = curl::parse_headers_list(res$headers))
}

podText <- function(res) trimws(rawToChar(res$body))

waitReady <- function(pod, timeout = 60)
{
    deadline <- Sys.time() + timeout
    repeat {
        res <- tryCatch(
            podRequest(pod, "GET", "/healthz", token = NULL, timeout = 2),
            error = function(e) NULL
        )
        if (!is.null(res) && res$status == 200L)
            return(invisible(pod))
        if (!pod$proc$is_alive() || Sys.time() > deadline)
            stop("pod script did not start:\n", podLog(pod))
        Sys.sleep(0.1)
    }
}

waitExit <- function(pod, timeout = 20)
{
    pod$proc$wait(timeout * 1000)
    if (pod$proc$is_alive())
        stop("pod script is still running:\n", podLog(pod))
    pod$proc$get_exit_status()
}

waitStatus <- function(pod, pattern, timeout = 30)
{
    deadline <- Sys.time() + timeout
    repeat {
        status <- podText(podRequest(pod, "GET", "/status"))
        if (grepl(pattern, status, fixed = TRUE) || Sys.time() > deadline)
            return(status)
        Sys.sleep(0.1)
    }
}

## Sends a request as is and returns the response text read until the server
## closes the connection.
rawExchange <- function(pod, request, timeout = 20)
{
    if (is.character(request))
        request <- charToRaw(request)
    con <- socketConnection("127.0.0.1", pod$port, open = "r+b",
                            blocking = TRUE, timeout = timeout)
    on.exit(close(con))
    writeBin(request, con)
    out <- raw()
    while (length(chunk <- readBin(con, "raw", 65536L)))
        out <- c(out, chunk)
    rawToChar(out)
}

## A serialized task built the way BiocParallel's bploop builds EXEC messages.
workerTask <- function(X, FUN, run = "run1", tag = 1L, dynamic = FALSE)
{
    environment(FUN) <- globalenv()
    msg <- BiocParallel:::.EXEC(
        tag, BiocParallel:::.workerLapply,
        args = list(
            X = X, FUN = FUN, ARGS = list(),
            OPTIONS = BiocParallel:::.workerOptions(exportglobals = FALSE),
            BPRNGSEED = BiocParallel:::.rng_init_stream(1L),
            GLOBALS = list(), PACKAGES = character()
        ),
        static.fun = TRUE, static.args = c("FUN", "ARGS", "OPTIONS", "GLOBALS")
    )
    if (dynamic)
        msg <- BiocParallel::.task_dynamic(msg)
    serialize(list(run = run, tag = tag, msg = msg), NULL)
}

taskResult <- function(pod, wait = 30)
{
    res <- podRequest(pod, "GET", paste0("/result?wait=", wait),
                      timeout = wait + 10)
    if (res$status != 200L)
        stop("no result: ", res$status, " ", podText(res))
    unserialize(res$body)
}

runnerConfig <- function(command, uploads = character(),
                         outputs = character())
{
    config <- list(command = command, uploads = uploads, outputs = outputs)
    paste(serialize(config, NULL), collapse = "")
}

rscript <- function() file.path(R.home("bin"), "Rscript")

## A fake API server, optionally proxying to a pod started by startPod().

## Handler replying with `x` as JSON.
jsonHandler <- function(x, status = 200L)
{
    force(x)
    force(status)
    function(req, res) res$set_status(status)$send_json(x, auto_unbox = TRUE)
}

coreResource <- function(name, kind, singular, subresources = character())
{
    list(
        resource = name,
        responseKind = list(group = "", version = "", kind = kind),
        scope = "Namespaced", singularResource = singular,
        verbs = as.list(c("create", "delete", "get", "list", "patch",
                          "update", "watch")),
        shortNames = list(),
        subresources = lapply(subresources, function(x)
            list(subresource = x))
    )
}

## Aggregated discovery with the core resources a session uses.
discoveryRoutes <- function()
{
    core <- list(
        kind = "APIGroupDiscoveryList",
        apiVersion = "apidiscovery.k8s.io/v2",
        items = list(list(metadata = list(), versions = list(list(
            version = "v1", freshness = "Current",
            resources = list(
                coreResource("configmaps", "ConfigMap", "configmap"),
                coreResource("pods", "Pod", "pod",
                             c("log", "proxy", "status")),
                coreResource("secrets", "Secret", "secret")
            )
        ))))
    )
    list(
        "GET /api" = jsonHandler(core),
        "GET /apis" = jsonHandler(list(
            kind = "APIGroupDiscoveryList",
            apiVersion = "apidiscovery.k8s.io/v2", items = list()
        )),
        "GET /openapi/v2" = jsonHandler(list(
            swagger = "2.0", info = list(version = "v1.34.0"),
            paths = structure(list(), names = character()),
            definitions = structure(list(), names = character())
        ))
    )
}

recordApiRequest <- function(req, res)
{
    dir <- req$app$locals$requests
    entry <- list(method = toupper(req$method), path = req$path,
                  query = req$query_string, headers = req$headers,
                  body = req$.body)
    saveRDS(entry, file.path(dir, sprintf("%06d.rds",
                                          length(dir(dir)) + 1L)))
    "next"
}

apiRequests <- function(dir)
{
    lapply(sort(list.files(dir, full.names = TRUE)), function(file) {
        entry <- readRDS(file)
        names(entry$headers) <- tolower(names(entry$headers))
        entry$body <- if (is.null(entry$body)) raw() else entry$body
        entry
    })
}

lastRequest <- function(api, path = NULL)
{
    requests <- api$requests()
    if (!is.null(path))
        requests <- Filter(function(r) identical(r$path, path), requests)
    if (!length(requests))
        stop("no request for ", path)
    requests[[length(requests)]]
}

## The manifest a recorded request carried.
requestBody <- function(request)
{
    file <- tempfile(fileext = ".json")
    writeBin(request$body, file)
    kube::kube_read(file)[[1L]]
}

## Forwards pod proxy requests to `podUrl`, as the API server does: the
## query string and the headers travel on, except Authorization.
podProxy <- function(podUrl)
{
    force(podUrl)
    pattern <- "^/api/v1/namespaces/[^/]+/pods/[^/]+/proxy"
    dropped <- c("authorization", "host", "content-length", "connection",
                 "accept-encoding", "expect", "user-agent")
    function(req, res) {
        if (!grepl(pattern, req$path))
            return("next")
        url <- paste0(podUrl, sub(pattern, "", req$path))
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
        answer <- curl::curl_fetch_memory(url, handle)
        type <- curl::parse_headers_list(answer$headers)[["content-type"]]
        res$set_status(answer$status_code)
        if (!is.null(type))
            res$set_header("Content-Type", type)
        res$send(answer$content)
    }
}

## A client for `url`, built from a kubeconfig with no credentials.
kubeClient <- function(url, namespace = "bioc")
{
    config <- tempfile(fileext = ".yaml")
    writeLines(c(
        sprintf("clusters: [{name: fake, cluster: {server: '%s'}}]", url),
        sprintf("contexts: [{name: fake, context: {cluster: fake, %s}}]",
                paste0("namespace: ", namespace)),
        "current-context: fake"
    ), config)
    kube::kube_client(kubeconfig = config)
}

## `routes` maps "METHOD /path" (webfakes syntax) to handlers, which run in
## another process and may use values they capture but not change the
## test's. Requests to a pod proxy path go to `pod`, if one is given.
fakeApi <- function(routes = list(), pod = NULL, namespace = "bioc",
                    .local_envir = parent.frame())
{
    skip_if_not_installed("webfakes")
    dir <- tempfile("api-")
    dir.create(dir)
    app <- webfakes::new_app()
    app$locals$requests <- dir
    app$use(recordApiRequest)
    if (!is.null(pod))
        app$use(podProxy(sprintf("http://127.0.0.1:%d", pod$port)))
    routes <- c(routes, discoveryRoutes())
    for (route in names(routes)) {
        parts <- strsplit(route, " ", fixed = TRUE)[[1L]]
        app[[tolower(parts[[1L]])]](parts[[2L]], routes[[route]])
    }
    process <- webfakes::local_app_process(app, .local_envir = .local_envir)
    url <- sub("/$", "", process$url())
    list(url = url, client = kubeClient(url, namespace),
         requests = function() apiRequests(dir))
}

## Handler replying with a Kubernetes Status object.
statusHandler <- function(code, reason, message)
{
    status <- list(kind = "Status", apiVersion = "v1", status = "Failure",
                   message = message, reason = reason, code = code)
    jsonHandler(status, code)
}

## A header of a recorded request, or NULL when it was not sent.
requestHeader <- function(request, name)
{
    request$headers[tolower(name)][[1L]]
}

## A pod object as the API server returns it.
podObject <- function(name = "worker-1", phase = "Running", ready = "True",
                      restarts = 0L, state = list(running = list()),
                      conditions = list())
{
    list(
        apiVersion = "v1", kind = "Pod",
        metadata = list(name = name, namespace = "bioc"),
        spec = list(containers = list(list(name = "worker"))),
        status = list(
            phase = phase,
            conditions = c(list(list(type = "Ready", status = ready)),
                           conditions),
            containerStatuses = list(list(name = "worker",
                                          restartCount = restarts,
                                          state = state))
        )
    )
}

## Whether process `pid` exists and is not a zombie (Linux).
processAlive <- function(pid)
{
    stat <- tryCatch(
        suppressWarnings(readLines(sprintf("/proc/%d/stat", pid), 1L)),
        error = function(e) character()
    )
    length(stat) > 0L && !grepl("^[0-9]+ \\(.*\\) Z ", stat)
}

## The process id a task wrote to `file`.
waitPid <- function(file, timeout = 30)
{
    deadline <- Sys.time() + timeout
    repeat {
        pid <- if (file.exists(file)) readLines(file, warn = FALSE)
        if (length(pid) && nzchar(pid[1L]))
            return(as.integer(pid[1L]))
        if (Sys.time() > deadline)
            stop("no process id in ", file)
        Sys.sleep(0.1)
    }
}

## A socket connected to a pod, with `text` sent when given.
podSocket <- function(pod, text = NULL, timeout = 20)
{
    con <- socketConnection("127.0.0.1", pod$port, open = "r+b",
                            blocking = TRUE, timeout = timeout)
    if (!is.null(text))
        writeBin(charToRaw(text), con)
    con
}

## BiocManager's version map is read offline, so that the default image of
## a KubeParam does not depend on the network.
options(BIOCONDUCTOR_ONLINE_VERSION_DIAGNOSIS = FALSE)
