#' Control a KubeParam
#'
#' BiocParallel methods for [KubeParam]. A param that is not started is
#' started and stopped by each call of
#' [`bplapply()`][BiocParallel::bplapply] and the other BiocParallel
#' functions.
#'
#' `bpstart()` creates a ConfigMap with the pod scripts, a Secret with the
#' session token and one pod per worker, and waits up to `startTimeout`
#' seconds until every pod is ready. It fails as soon as a pod cannot
#' become ready, with the reason and the last lines of the pod's log, and
#' then removes what it created. It warns when the first two components
#' of the pods' R or BiocParallel versions differ from the session's.
#'
#' `bpstop()` asks the pods to exit and deletes the ConfigMap, which removes
#' the Secret and the pods. It does nothing on a param that is not running,
#' reports what it could not remove as a warning, spends at most about 20
#' seconds, and also runs when a started param is garbage collected or R
#' exits.
#'
#' A pod that fails, is deleted or exits while the param is started is
#' lost, and the elements of the task it held fail. With
#' `stop.on.error = FALSE` the other pods take the remaining tasks; with
#' the default `TRUE` the run stops. Passing the result of
#' [`bptry()`][BiocParallel::bptry] as `BPREDO` evaluates the
#' failed and unevaluated elements on the pods that are left. A run warns
#' about pods that have ended since the last one, for example after
#' `idleTimeout` seconds without a request, and fails when none is left;
#' `bpstop()` and `bpstart()` then start new pods.
#'
#' Inside a worker pod a `KubeParam` is not started and falls back to
#' serial evaluation, so a task's own BiocParallel calls run in its pod.
#'
#' @param x A [KubeParam].
#' @param value The number of workers, whether to log, or the logging
#'   threshold.
#' @param ... Unused.
#'
#' @return `bpstart()` and `bpstop()` return `x` invisibly, `bpisup()`
#'   whether the pods are running, `bpbackend()` the object that talks to
#'   them, `bpschedule()` whether tasks may be sent from here,
#'   `bpfallback()` whether BiocParallel may use a `SerialParam` instead of
#'   `x`, and the replacement methods return `x`.
#'
#' @seealso [KubeParam()].
#'
#' @examples
#' param <- KubeParam(workers = 2,
#'                    image = "ghcr.io/bioconductor/bioconductor:RELEASE_3_23")
#' bpisup(param)
#' bpworkers(param) <- 4
#' bpnworkers(param)
#' bpstop(param)
#'
#' @examplesIf interactive()
#' ## Needs access to a cluster.
#' param <- KubeParam(workers = 4)
#' bpstart(param)
#' bpisup(param)
#' bplapply(1:8, function(i) Sys.getpid(), BPPARAM = param)
#' bpstop(param)
#' @name KubeParam-methods
NULL

#' @rdname KubeParam-methods
#' @export
setMethod("bpstart", "KubeParam", function(x, ...) {
    if (bpisup(x))
        stop("The worker pods are already running.", call. = FALSE)
    client <- x$client
    namespace <- x$namespace
    if (is.na(namespace))
        namespace <- kube::kube_namespace(client)
    id <- .newToken(4L)
    name <- paste0("biockube-", id)
    labels <- c(.managedBy, structure(id, names = .sessionLabel))
    token <- .newToken()
    started <- FALSE
    on.exit(if (!started) .deleteSession(client, namespace, name))
    session <- .createSession(client, namespace, name,
                              .podScripts(c("server.R", "worker.R")), token,
                              labels)
    pods <- .createPods(x, client, namespace, name, session, labels)
    .waitReady(client, namespace, pods, timeout = x$startTimeout,
               selector = labels[.sessionLabel])
    backend <- .kubeBackend(client, namespace, session, pods, .workerPort,
                            token, idle = x$idleTimeout)
    .warnVersions(backend)
    bpbackend(x) <- backend
    started <- TRUE
    .bpstart_impl(x)
})

#' @rdname KubeParam-methods
#' @export
setMethod("bpstop", "KubeParam", function(x) {
    if (!bpisup(x))
        return(invisible(x))
    backend <- bpbackend(x)
    backend$deadline <- .now() + .stopTimeout
    ## Cleared even when bpstop() is interrupted, so that later runs start
    ## as usual.
    on.exit(backend$deadline <- NULL)
    .tolerate(.bpstop_impl(x), "stop the worker pods")
    .tolerate(.deleteSession(backend$client, backend$namespace,
                             backend$session$configMap,
                             timeout = backend$deadline - .now()),
              "remove the session")
    bpbackend(x) <- .kubeBackendNULL()
    invisible(x)
})

## A param that reaches a pod inside a task carries the backend of the
## session it came from, which only that session drives.
#' @rdname KubeParam-methods
#' @export
setMethod("bpisup", "KubeParam", function(x) {
    bpschedule(x) && length(bpbackend(x)) > 0L
})

#' @rdname KubeParam-methods
#' @export
setMethod("bpbackend", "KubeParam", function(x) {
    x$backend
})

#' @rdname KubeParam-methods
#' @export
setReplaceMethod("bpbackend", c("KubeParam", "KubeBackend"),
    function(x, value) {
        x$backend <- value
        x
    })

#' @rdname KubeParam-methods
#' @export
setReplaceMethod("bpworkers", c("KubeParam", "numeric"),
    function(x, value) {
        if (bpisup(x))
            stop("Stop the worker pods before changing their number.",
                 call. = FALSE)
        ## The param is a reference object: undo an invalid change.
        old <- x$workers
        x$workers <- as.integer(value)
        tryCatch(validObject(x), error = function(e) {
            x$workers <- old
            stop(e)
        })
        x
    })

#' @rdname KubeParam-methods
#' @export
setReplaceMethod("bplog", c("KubeParam", "logical"), function(x, value) {
    x$log <- value
    x
})

#' @rdname KubeParam-methods
#' @export
setReplaceMethod("bpthreshold", c("KubeParam", "character"),
    function(x, value) {
        x$threshold <- value
        x
    })

## The worker script sets BIOCKUBE_WORKER in the pod.
#' @rdname KubeParam-methods
#' @export
setMethod("bpschedule", "KubeParam", function(x) {
    !nzchar(Sys.getenv("BIOCKUBE_WORKER"))
})

## BiocParallel replaces a param by a SerialParam only when it is not
## running and may fall back.
#' @rdname KubeParam-methods
#' @export
setMethod("bpfallback", "KubeParam", function(x) {
    isTRUE(x$fallback) || !bpschedule(x)
})

## Every bplapply() or bpiterate() call is a run of its own; bpstop() only
## tells the pods to exit.
setMethod(".manager", "KubeParam", function(BPPARAM) {
    backend <- bpbackend(BPPARAM)
    if (is.null(backend$deadline)) {
        lost <- backend$lost
        .startRun(backend)
        .reportLost(backend, lost)
    }
    .kubeManager(BPPARAM, backend)
})

#' Create the worker pods of a session
#'
#' @param x A `KubeParam`.
#' @param client A `kube_client`, or `NULL` for the default client.
#' @param namespace,name Namespace and session name.
#' @param session The session from `.createSession()`.
#' @param labels Labels every pod carries.
#' @return The names of the pods.
#' @noRd
.createPods <- function(x, client, namespace, name, session, labels) {
    pods <- sprintf("%s-%d", name, seq_len(bpnworkers(x)))
    for (pod in pods)
        .create(client, .podManifest(
            pod, namespace, x$image, session,
            port = .workerPort, cpu = .orNull(x$cpu),
            memory = .orNull(x$memory), packages = x$packages, env = x$env,
            idle = x$idleTimeout, labels = labels
        ))
    pods
}

#' Compare the versions the workers report with this session's
#'
#' @param backend A `KubeBackend`.
#' @return `NULL`, invisibly, with a warning when a pod runs another R or
#'   BiocParallel series than the session. Tasks and results are
#'   BiocParallel's own messages, whose shape is internal to it.
#' @noRd
.warnVersions <- function(backend) {
    statuses <- lapply(seq_along(backend$pods), function(node)
        .podRequest(backend, node, "status", as = "parsed",
                    timeout = .requestTimeout))
    r <- as.character(getRversion())
    biocparallel <- as.character(packageVersion("BiocParallel"))
    skewed <- !vapply(statuses, function(status)
        .sameSeries(status$r, r) &&
            .sameSeries(status$biocparallel, biocparallel),
        logical(1L))
    if (!any(skewed))
        return(invisible(NULL))
    status <- statuses[[which(skewed)[1L]]]
    warning(sprintf("The worker pods report R %s and BiocParallel %s; ",
                    .orUnknown(status$r), .orUnknown(status$biocparallel)),
            sprintf("this session runs R %s and BiocParallel %s. ", r,
                    biocparallel),
            "Tasks and results use BiocParallel's internal format, which ",
            "differs between versions.", call. = FALSE)
    invisible(NULL)
}

## Patch releases keep the message format, so versions are compared up to
## their second part.
.sameSeries <- function(x, y) {
    series <- .series(x)
    !is.na(series) && identical(series, .series(y))
}

.series <- function(x) {
    if (!is.character(x) || length(x) != 1L || is.na(x))
        return(NA_character_)
    parts <- strsplit(x, "[.-]")[[1L]]
    paste(parts[seq_len(min(2L, length(parts)))], collapse = ".")
}

.orUnknown <- function(x) {
    if (is.character(x) && length(x) == 1L && !is.na(x)) x
    else "an unknown version"
}

## bpstop() runs from a finalizer as well, where an error would be raised
## during garbage collection or while R is shutting down.
.tolerate <- function(expr, action) {
    tryCatch(expr, error = function(e)
        warning(sprintf("Could not %s: %s", action, conditionMessage(e)),
                call. = FALSE))
    invisible(NULL)
}
