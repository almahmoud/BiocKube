test_that("runner configuration is decoded and checked", {
    pod <- podSource("server.R", "runner.R")
    text <- runnerConfig(c("Rscript", "job.R"), uploads = list(a = "/in/a"),
                         outputs = c(b = "/out/b"))

    expect_identical(
        pod$.runnerConfig(text),
        list(command = c("Rscript", "job.R"), uploads = c(a = "/in/a"),
             outputs = c(b = "/out/b"))
    )
    expect_identical(pod$.runnerConfig(runnerConfig("true"))$uploads,
                     character())
    expect_error(pod$.runnerConfig(""), "BIOCKUBE_RUNNER is not set")
    expect_error(pod$.runnerConfig("zz"), "cannot decode BIOCKUBE_RUNNER")
    expect_error(pod$.runnerConfig(runnerConfig(character())),
                 "command must be a character vector")
    expect_error(pod$.runnerConfig(runnerConfig("true", uploads = "/in/a")),
                 "uploads must be uniquely named file paths")
    expect_error(
        pod$.runnerConfig(runnerConfig("true", outputs = c(b = "x", b = "y"))),
        "outputs must be uniquely named file paths"
    )
})

test_that("runner stores uploads, runs the command and serves outputs", {
    dir <- tempfile()
    on.exit(unlink(dir, recursive = TRUE))
    dir.create(dir)
    script <- file.path(dir, "job.R")
    writeLines(c(
        "args <- commandArgs(TRUE)",
        "x <- readBin(args[1L], 'raw', file.size(args[1L]))",
        "dir.create(dirname(args[2L]), showWarnings = FALSE)",
        "writeBin(rev(x), args[2L])",
        "writeLines(c(args[3L], Sys.getenv('BIOCKUBE_TOKEN')), args[4L])"
    ), script)
    paths <- file.path(dir, c("in/data.bin", "out/rev.bin", "out/note.txt"))
    arg <- "it's a \"quoted\" $HOME"
    config <- runnerConfig(
        c(rscript(), script, paths[1:2], arg, paths[3L]),
        uploads = c(data = paths[1L]),
        outputs = c(rev = paths[2L], note = paths[3L], none = "/nonexistent")
    )
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = config))
    on.exit(stopPod(pod), add = TRUE)
    data <- as.raw(sample(0:255, 2e6, replace = TRUE))

    res <- podRequest(pod, "POST", "/start")
    expect_identical(res$status, 409L)
    expect_identical(podText(res), "missing inputs: data")
    expect_identical(podRequest(pod, "PUT", "/inputs/other", data)$status,
                     404L)
    expect_identical(podRequest(pod, "GET", "/outputs/rev")$status, 404L)
    expect_identical(podRequest(pod, "POST", "/release")$status, 409L)

    expect_identical(podRequest(pod, "PUT", "/inputs/data", data)$status,
                     201L)
    expect_identical(readBin(paths[1L], "raw", 3e6), data)
    expect_identical(podText(podRequest(pod, "GET", "/status")),
                     '{"state":"waiting","exit_code":null}')

    expect_identical(podRequest(pod, "POST", "/start")$status, 202L)
    expect_identical(podRequest(pod, "POST", "/start")$status, 409L)
    expect_identical(podRequest(pod, "PUT", "/inputs/data", data)$status,
                     409L)
    expect_identical(waitStatus(pod, "succeeded"),
                     '{"state":"succeeded","exit_code":0}')

    expect_identical(podRequest(pod, "GET", "/outputs/rev")$body, rev(data))
    expect_identical(rawToChar(podRequest(pod, "GET", "/outputs/note")$body),
                     paste0(arg, "\n\n"))
    res <- podRequest(pod, "GET", "/outputs/none")
    expect_identical(res$status, 404L)
    expect_identical(podText(res), "output 'none' was not written")

    expect_identical(podRequest(pod, "POST", "/release")$status, 204L)
    expect_identical(waitExit(pod), 0L)
    log <- podLog(pod)
    expect_match(log, "received input 'data'", fixed = TRUE)
    expect_match(log, "command exited with status 0", fixed = TRUE)
    expect_match(log, "released, exiting", fixed = TRUE)
})

test_that("runner exits with the exit code of a failed command", {
    config <- runnerConfig(c(rscript(), "-e", "quit(status = 3L)"))
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = config))
    on.exit(stopPod(pod))
    expect_identical(podRequest(pod, "POST", "/start")$status, 202L)

    expect_identical(waitStatus(pod, "failed"),
                     '{"state":"failed","exit_code":3}')
    expect_identical(podRequest(pod, "POST", "/release")$status, 204L)
    expect_identical(waitExit(pod), 3L)
    expect_match(podLog(pod), "command exited with status 3")
})

test_that("runner logs the exit on a line of its own", {
    config <- runnerConfig(
        c(rscript(), "-e",
          "cat('downloading 50%', file = stderr()); quit(status = 3L)")
    )
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = config))
    on.exit(stopPod(pod))
    expect_identical(podRequest(pod, "POST", "/start")$status, 202L)

    expect_identical(waitStatus(pod, "failed"),
                     '{"state":"failed","exit_code":3}')
    lines <- readLines(pod$log, warn = FALSE)
    expect_true("downloading 50%" %in% lines)
    expect_length(grep("command exited with status 3$", lines), 1L)
})

test_that("runner connections close while the command runs", {
    config <- runnerConfig(c(rscript(), "-e", "Sys.sleep(10)"))
    pod <- startPod("runner.R", env = c(BIOCKUBE_RUNNER = config))
    on.exit(stopPod(pod))
    request <- sprintf("POST /start HTTP/1.1\r\nX-BiocKube-Token: %s\r\n\r\n",
                       pod$token)

    elapsed <- system.time(res <- rawExchange(pod, request))[["elapsed"]]
    expect_match(res, "^HTTP/1.1 202 ")
    expect_lt(elapsed, 5)
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"running"', fixed = TRUE)
})

test_that("runner exits once outputs have been kept long enough", {
    config <- runnerConfig(c(rscript(), "-e", "0"))
    pod <- startPod("runner.R",
                    env = c(BIOCKUBE_RUNNER = config, BIOCKUBE_KEEP = "1"))
    on.exit(stopPod(pod))
    expect_identical(podRequest(pod, "POST", "/start")$status, 202L)

    expect_identical(waitExit(pod), 0L)
    expect_match(podLog(pod), "outputs kept for 1 seconds, exiting")
})

test_that("runner fails when it is not started in time", {
    config <- runnerConfig("true", uploads = c(data = tempfile()))
    pod <- startPod("runner.R", ready = FALSE,
                    env = c(BIOCKUBE_RUNNER = config, BIOCKUBE_IDLE = "1"))
    on.exit(stopPod(pod))

    expect_identical(waitExit(pod), 1L)
    expect_match(podLog(pod), "not started within 1 seconds, exiting")
})

test_that("runner counts the start timeout from the last completed upload", {
    dir <- tempfile()
    on.exit(unlink(dir, recursive = TRUE))
    config <- runnerConfig(c(rscript(), "-e", "0"),
                           uploads = c(data = file.path(dir, "data")))
    pod <- startPod("runner.R",
                    env = c(BIOCKUBE_RUNNER = config, BIOCKUBE_IDLE = "3"))
    on.exit(stopPod(pod), add = TRUE)
    con <- podSocket(pod, paste0(
        "PUT /inputs/data HTTP/1.1\r\nX-BiocKube-Token: ", pod$token,
        "\r\nContent-Length: 10\r\n\r\n"
    ))
    on.exit(close(con), add = TRUE)
    for (i in seq_len(10L)) {
        Sys.sleep(0.5)
        writeBin(as.raw(i), con)
    }

    expect_match(rawToChar(readBin(con, "raw", 1024L)), "^HTTP/1.1 201 ")
    Sys.sleep(1)
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"waiting"', fixed = TRUE)
    expect_identical(waitExit(pod), 1L)
    expect_match(podLog(pod), "not started within 3 seconds, exiting")
})

test_that("runner does not start with an invalid configuration", {
    pod <- startPod("runner.R", ready = FALSE,
                    env = c(BIOCKUBE_RUNNER = "not hex"))
    on.exit(stopPod(pod))

    expect_identical(waitExit(pod), 1L)
    expect_match(podLog(pod), "cannot decode BIOCKUBE_RUNNER")
})
