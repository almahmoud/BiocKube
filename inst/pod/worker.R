## BiocParallel worker pod for KubeParam. Each task runs in a forked child so
## that the server stays responsive to status, long polls and cancellation.
##
## Environment: BIOCKUBE_TOKEN (required), BIOCKUBE_PORT (8080),
## BIOCKUBE_IDLE (3600 seconds), BIOCKUBE_PACKAGES (comma separated).

.podFile <- function(name)
{
    script <- sub("^--file=", "", grep("^--file=", commandArgs(), value = TRUE))
    file.path(dirname(script[1L]), name)
}

.installed <- function(pkgs)
{
    vapply(pkgs, function(p) nzchar(system.file(package = p)), logical(1))
}

.installLibrary <- function()
{
    writable <- .libPaths()[file.access(.libPaths(), 2L) == 0L]
    if (length(writable))
        return(writable[1L])
    lib <- Sys.getenv("R_LIBS_USER")
    dir.create(lib, recursive = TRUE, showWarnings = FALSE)
    .libPaths(c(lib, .libPaths()))
    lib
}

## Installs the packages in `pkgs` that are missing, with BiocManager.
.ensurePackages <- function(pkgs)
{
    missing <- pkgs[!.installed(pkgs)]
    if (!length(missing))
        return(invisible(character()))
    lib <- .installLibrary()
    if (!.installed("BiocManager")) {
        repos <- getOption("repos")
        if (is.null(repos) || any(repos == "@CRAN@"))
            repos <- c(CRAN = "https://cloud.r-project.org")
        .log("installing BiocManager")
        utils::install.packages("BiocManager", lib = lib, repos = repos)
    }
    .log("installing ", paste(missing, collapse = ", "))
    BiocManager::install(missing, lib = lib, update = FALSE, ask = FALSE)
    failed <- missing[!.installed(missing)]
    if (length(failed))
        stop("could not install ", paste(failed, collapse = ", "),
             call. = FALSE)
    .log("installed ", paste(missing, collapse = ", "))
    invisible(missing)
}

.packagesFromEnv <- function()
{
    pkgs <- trimws(strsplit(Sys.getenv("BIOCKUBE_PACKAGES"), ",")[[1L]])
    pkgs[nzchar(pkgs)]
}

## One-shot BiocParallel worker: .recv() yields the task, then DONE.
.defineTaskMethods <- function(where = globalenv())
{
    taskClass <- "biockubeTask"
    recv <- function(worker) {
        msg <- worker$msg
        worker$msg <- list(type = "DONE")
        msg
    }
    send <- function(worker, value) {
        worker$value <- value
        invisible(TRUE)
    }
    finish <- function(worker) invisible(NULL)
    methods::setOldClass(taskClass, where = where)
    methods::setMethod(BiocParallel::.recv, taskClass, recv, where = where)
    methods::setMethod(BiocParallel::.send, taskClass, send, where = where)
    methods::setMethod(BiocParallel::.close, taskClass, finish, where = where)
}

.runTask <- function(msg)
{
    worker <- structure(new.env(parent = emptyenv()), class = "biockubeTask")
    worker$msg <- msg
    BiocParallel::.bpworker_impl(worker)
    worker$value
}

## The task's elements all fail with `reason`, as BiocParallel expects one
## value per element.
.failedValue <- function(w, reason)
{
    error <- BiocParallel::.error_worker_comm(simpleError(reason),
                                              "worker pod task failed")
    list(type = "VALUE", tag = w$tag, value = rep(list(error), w$size),
         success = FALSE, time = proc.time() - w$started, log = NULL,
         sout = NULL)
}

.newWorker <- function(idleTimeout)
{
    w <- new.env(parent = emptyenv())
    w$state <- "idle"
    w$idleTimeout <- idleTimeout
    w$versions <- list(
        r = as.character(getRversion()),
        biocparallel = as.character(utils::packageVersion("BiocParallel"))
    )
    w
}

## Restores a dynamic-only task from the static part cached for its run, or
## caches the static part of a full task.
.withStatic <- function(w, run, msg)
{
    if (isTRUE(msg$dynamic.only)) {
        if (is.null(w$static) || !identical(w$static$run, run))
            stop(.httpError(422L, "no static part cached for this run"))
        return(BiocParallel::.task_remake(msg, w$static$data))
    }
    static <- BiocParallel::.task_const(msg)
    if (!is.null(static))
        w$static <- list(run = run, data = static)
    msg
}

.readTask <- function(req)
{
    task <- tryCatch(
        unserialize(.readBody(req)),
        error = function(e) stop(.httpError(
            400L, paste("cannot read task:", conditionMessage(e))
        ))
    )
    if (!is.list(task) || !all(c("run", "tag", "msg") %in% names(task)) ||
        !identical(task$msg$type, "EXEC"))
        stop(.httpError(400L, "task must be list(run, tag, msg = <EXEC>)"))
    task
}

.postTask <- function(w, req)
{
    if (w$state == "busy")
        return(.response(409L, "busy"))
    task <- .readTask(req)
    msg <- .withStatic(w, task$run, task$msg)
    w$run <- task$run
    w$tag <- task$tag
    w$size <- length(msg$data$args$X)
    w$result <- NULL
    w$started <- proc.time()
    w$job <- .fork(.runTask(msg))
    w$state <- "busy"
    .response(202L, "accepted")
}

.collectTask <- function(w)
{
    out <- suppressWarnings(parallel::mccollect(w$job, wait = FALSE))
    if (is.null(out))
        return(invisible())
    value <- out[[1L]]
    if (inherits(value, "try-error"))
        value <- .failedValue(w, conditionMessage(attr(value, "condition")))
    else if (!identical(value$type, "VALUE"))
        value <- .failedValue(w, "task process ended without a result")
    w$result <- serialize(list(run = w$run, tag = w$tag, value = value),
                          NULL, xdr = FALSE)
    w$job <- NULL
    w$state <- "done"
    w$finishedAt <- .now()
}

## Ends a running task, with any processes it started, and drops any result.
.clearTask <- function(w)
{
    if (!is.null(w$job)) {
        .stopJob(w$job)
        .log("cancelled task ", w$tag)
    }
    w$job <- NULL
    w$result <- NULL
    w$state <- "idle"
}

.getResult <- function(w, req)
{
    wait <- req$query["wait"][[1L]]
    wait <- if (is.na(wait)) 0 else suppressWarnings(as.numeric(wait))
    if (is.na(wait) || wait < 0)
        return(.response(400L, "wait must be a non-negative number"))
    ready <- function() {
        switch(w$state,
            done = .response(200L, w$result),
            idle = .response(404L, "no task"),
            NULL
        )
    }
    .defer(ready, wait, .response(204L))
}

.status <- function(w)
{
    .json(c(list(state = w$state, run = w$run, tag = w$tag), w$versions))
}

.workerRoutes <- function(w)
{
    list(
        .route("GET", "^/status$", function(req) .status(w)),
        .route("POST", "^/task$", function(req) .postTask(w, req)),
        .route("GET", "^/result$", function(req) .getResult(w, req)),
        .route("DELETE", "^/result$", function(req) {
            if (w$state == "done")
                .clearTask(w)
            .response(204L)
        }),
        .route("POST", "^/cancel$", function(req) {
            .clearTask(w)
            .response(204L)
        }),
        .route("POST", "^/done$", function(req) {
            .clearTask(w)
            .response(204L, then = function() quit(save = "no", status = 0L))
        })
    )
}

## A running task is never idle, and its result is kept for the idle timeout
## after it ends.
.pollWorker <- function(w, idle)
{
    if (w$state == "busy")
        .collectTask(w)
    if (w$state == "busy")
        return(invisible())
    if (w$state == "done")
        idle <- min(idle, .now() - w$finishedAt)
    if (idle > w$idleTimeout) {
        .log("no requests for ", w$idleTimeout, " seconds, exiting")
        .clearTask(w)
        quit(save = "no", status = 0L)
    }
}

.main <- function()
{
    source(.podFile("server.R"))
    token <- .takeToken()
    port <- .envNumber("BIOCKUBE_PORT", 8080L)
    idleTimeout <- .envNumber("BIOCKUBE_IDLE", 3600)
    .ensurePackages(unique(c("BiocParallel", .packagesFromEnv())))
    .defineTaskMethods()
    Sys.setenv(BIOCKUBE_WORKER = "1")
    w <- .newWorker(idleTimeout)
    .log("R ", w$versions$r, ", BiocParallel ", w$versions$biocparallel)
    .serve(.workerRoutes(w), token, port,
           poll = function(idle) .pollWorker(w, idle))
}

if (sys.nframe() == 0L)
    .main()
