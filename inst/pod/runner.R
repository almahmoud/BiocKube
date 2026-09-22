## BiocJobs runner for submitJob(): receives uploads, runs the job command
## and serves its outputs until released.
##
## Environment: BIOCKUBE_TOKEN (required), BIOCKUBE_RUNNER (required),
## BIOCKUBE_PORT (8080), BIOCKUBE_IDLE (3600 seconds after startup or the
## last completed upload, while waiting to start), BIOCKUBE_KEEP (86400
## seconds, once finished).
##
## BIOCKUBE_RUNNER is the hex text of serialize(config, NULL), for example
## paste(serialize(config, NULL), collapse = ""), where config is
## list(command = <argv>, uploads = c(<name> = <path>),
##      outputs = c(<name> = <path>)).

.podFile <- function(name)
{
    script <- sub("^--file=", "", grep("^--file=", commandArgs(), value = TRUE))
    file.path(dirname(script[1L]), name)
}

.unhex <- function(x)
{
    n <- nchar(x)
    if (n %% 2L || grepl("[^0-9a-fA-F]", x))
        stop("not hexadecimal text")
    as.raw(strtoi(substring(x, seq(1L, n, 2L), seq(2L, n, 2L)), 16L))
}

.namedPaths <- function(x, what)
{
    x <- unlist(x)
    if (!length(x))
        return(character())
    if (!is.character(x) || anyNA(x) || !all(nzchar(x)) ||
        is.null(names(x)) || !all(nzchar(names(x))) || anyDuplicated(names(x)))
        stop(what, " must be uniquely named file paths", call. = FALSE)
    x
}

## Decodes and checks the runner configuration.
.runnerConfig <- function(text = Sys.getenv("BIOCKUBE_RUNNER"))
{
    if (!nzchar(text))
        stop("BIOCKUBE_RUNNER is not set", call. = FALSE)
    config <- tryCatch(
        unserialize(.unhex(text)),
        error = function(e) stop("cannot decode BIOCKUBE_RUNNER: ",
                                 conditionMessage(e), call. = FALSE)
    )
    command <- config$command
    if (!is.character(command) || !length(command) || anyNA(command) ||
        !nzchar(command[1L]))
        stop("BIOCKUBE_RUNNER command must be a character vector",
             call. = FALSE)
    list(
        command = unname(command),
        uploads = .namedPaths(config$uploads, "BIOCKUBE_RUNNER uploads"),
        outputs = .namedPaths(config$outputs, "BIOCKUBE_RUNNER outputs")
    )
}

.newRunner <- function(config, idleTimeout, keepTimeout)
{
    r <- new.env(parent = emptyenv())
    r$config <- config
    r$idleTimeout <- idleTimeout
    r$keepTimeout <- keepTimeout
    r$state <- "waiting"
    r$received <- character()
    r$uploadedAt <- .now()
    r
}

.finished <- function(r) r$state %in% c("succeeded", "failed")

.missingUploads <- function(r)
{
    setdiff(names(r$config$uploads), r$received)
}

.launch <- function(r)
{
    command <- r$config$command
    .log("running ", paste(shQuote(command), collapse = " "))
    ## system2() quotes the command itself but not its arguments.
    r$job <- .fork(system2(command[1L], shQuote(command[-1L])))
    r$state <- "running"
}

.collectJob <- function(r)
{
    out <- suppressWarnings(parallel::mccollect(r$job, wait = FALSE))
    if (is.null(out))
        return(invisible())
    code <- out[[1L]]
    if (!is.numeric(code) || length(code) != 1L || is.na(code))
        code <- 1L
    r$exitCode <- as.integer(code)
    r$state <- if (r$exitCode == 0L) "succeeded" else "failed"
    r$finishedAt <- .now()
    r$job <- NULL
    ## The command may have left its last line unfinished on the same stream.
    cat("\n", file = stderr())
    .log("command exited with status ", r$exitCode)
}

.putInput <- function(r, req, name)
{
    path <- r$config$uploads[name]
    if (is.na(path))
        return(.response(404L, paste0("unknown input '", name, "'")))
    if (r$state != "waiting")
        return(.response(409L, "the job has already started"))
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    partial <- tempfile(".upload-", tmpdir = dirname(path))
    on.exit(unlink(partial))
    .readBody(req, partial)
    if (!file.rename(partial, path))
        stop("cannot write ", path)
    r$received <- union(r$received, name)
    r$uploadedAt <- .now()
    .log("received input '", name, "'")
    .response(201L, "created")
}

.start <- function(r)
{
    if (r$state != "waiting")
        return(.response(409L, "the job has already started"))
    missing <- .missingUploads(r)
    if (length(missing))
        return(.response(409L, paste("missing inputs:",
                                     paste(missing, collapse = ", "))))
    .launch(r)
    .response(202L, "started")
}

.getOutput <- function(r, name)
{
    path <- r$config$outputs[name]
    if (is.na(path))
        return(.response(404L, paste0("unknown output '", name, "'")))
    if (!.finished(r))
        return(.response(404L, "the job has not finished"))
    if (!file.exists(path) || dir.exists(path))
        return(.response(404L, paste0("output '", name, "' was not written")))
    .response(200L, file = path)
}

.exit <- function(r)
{
    quit(save = "no", status = min(r$exitCode, 255L))
}

.runnerRoutes <- function(r)
{
    list(
        .route("GET", "^/status$", function(req) {
            .json(list(state = r$state, exit_code = r$exitCode))
        }),
        .route("PUT", "^/inputs/([^/]+)$", function(req, name) {
            .putInput(r, req, name)
        }),
        .route("POST", "^/start$", function(req) .start(r)),
        .route("GET", "^/outputs/([^/]+)$", function(req, name) {
            .getOutput(r, name)
        }),
        .route("POST", "^/release$", function(req) {
            if (!.finished(r))
                return(.response(409L, "the job has not finished"))
            .response(204L, then = function() {
                .log("released, exiting")
                .exit(r)
            })
        })
    )
}

.pollRunner <- function(r)
{
    if (r$state == "running")
        .collectJob(r)
    if (r$state == "waiting" && .now() - r$uploadedAt > r$idleTimeout) {
        .log("not started within ", r$idleTimeout, " seconds, exiting")
        quit(save = "no", status = 1L)
    }
    if (.finished(r) && .now() - r$finishedAt > r$keepTimeout) {
        .log("outputs kept for ", r$keepTimeout, " seconds, exiting")
        .exit(r)
    }
}

.main <- function()
{
    source(.podFile("server.R"))
    token <- .takeToken()
    config <- .runnerConfig()
    port <- .envNumber("BIOCKUBE_PORT", 8080L)
    r <- .newRunner(config,
                    idleTimeout = .envNumber("BIOCKUBE_IDLE", 3600),
                    keepTimeout = .envNumber("BIOCKUBE_KEEP", 86400))
    ## The pod log is the job's log: only state changes are written to it.
    .serve(.runnerRoutes(r), token, port,
           poll = function(idle) .pollRunner(r), logRequests = FALSE)
}

if (sys.nframe() == 0L)
    .main()
