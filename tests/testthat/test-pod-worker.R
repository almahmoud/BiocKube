test_that("missing packages are installed with BiocManager", {
    skip_if_not_installed("BiocManager")
    pod <- podSource("server.R", "worker.R")
    present <- "BiocParallel"
    pod$.installed <- function(pkgs) pkgs %in% present
    pod$.installLibrary <- function() tempdir()
    requested <- list()
    install <- function(pkgs, ...) {
        requested[[length(requested) + 1L]] <<- pkgs
        present <<- c(present, setdiff(pkgs, "unavailable"))
    }
    local_mocked_bindings(install.packages = install, .package = "utils")
    local_mocked_bindings(install = install, .package = "BiocManager")

    expect_identical(pod$.ensurePackages("BiocParallel"), character())
    expect_length(requested, 0L)

    log <- capture.output(
        installed <- pod$.ensurePackages(c("BiocParallel", "a", "b")),
        type = "message"
    )
    expect_identical(installed, c("a", "b"))
    expect_identical(requested, list("BiocManager", c("a", "b")))
    expect_match(log, "installing a, b$", all = FALSE)
    expect_match(log, "installed a, b$", all = FALSE)

    capture.output(type = "message", expect_error(
        pod$.ensurePackages(c("c", "unavailable")),
        "could not install unavailable"
    ))
})

test_that("worker packages are read from the environment", {
    pod <- podSource("server.R", "worker.R")
    on.exit(Sys.unsetenv("BIOCKUBE_PACKAGES"))

    Sys.setenv(BIOCKUBE_PACKAGES = " a, b ,,c ")
    expect_identical(pod$.packagesFromEnv(), c("a", "b", "c"))
    Sys.setenv(BIOCKUBE_PACKAGES = "")
    expect_identical(pod$.packagesFromEnv(), character())
})

test_that("worker runs a task and keeps its result until acknowledged", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    status <- podText(podRequest(pod, "GET", "/status"))
    expect_match(status, '"state":"idle","run":null,"tag":null', fixed = TRUE)
    expect_match(status, paste0('"biocparallel":"',
                                packageVersion("BiocParallel"), '"'))

    task <- workerTask(list(1, 2), function(x) x * 2, run = "a1", tag = 7L)
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    result <- taskResult(pod)

    expect_identical(result$run, "a1")
    expect_identical(result$tag, 7L)
    expect_identical(result$value$type, "VALUE")
    expect_identical(result$value$tag, 7L)
    expect_true(result$value$success)
    expect_identical(result$value$value, list(2, 4))
    expect_identical(taskResult(pod), result)
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"done","run":"a1","tag":7', fixed = TRUE)

    expect_identical(podRequest(pod, "DELETE", "/result")$status, 204L)
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"idle"', fixed = TRUE)
    expect_identical(podRequest(pod, "GET", "/result?wait=5")$status, 404L)
    expect_identical(podRequest(pod, "DELETE", "/result")$status, 204L)
})

test_that("worker reuses the static part of a task within a run", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    FUN <- function(x) x + 100

    first <- workerTask(list(1), FUN, run = "r1", tag = 1L)
    expect_identical(podRequest(pod, "POST", "/task", first)$status, 202L)
    expect_identical(taskResult(pod)$value$value, list(101))

    second <- workerTask(list(2, 3), FUN, run = "r1", tag = 2L, dynamic = TRUE)
    expect_identical(podRequest(pod, "POST", "/task", second)$status, 202L)
    result <- taskResult(pod)
    expect_identical(result$tag, 2L)
    expect_identical(result$value$value, list(102, 103))

    other <- workerTask(list(4), FUN, run = "r2", tag = 1L, dynamic = TRUE)
    res <- podRequest(pod, "POST", "/task", other)
    expect_identical(res$status, 422L)
    expect_match(podText(res), "no static part cached for this run")
})

test_that("worker stays responsive while a task runs", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    slow <- workerTask(list(1), function(x) {
        Sys.sleep(2)
        x
    })

    expect_identical(podRequest(pod, "POST", "/task", slow)$status, 202L)
    expect_identical(podRequest(pod, "GET", "/result?wait=0")$status, 204L)
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"busy"', fixed = TRUE)
    expect_identical(podRequest(pod, "POST", "/task", slow)$status, 409L)
    expect_identical(podRequest(pod, "GET", "/healthz")$status, 200L)
    expect_identical(podRequest(pod, "GET", "/result?wait=soon")$status, 400L)
    expect_identical(taskResult(pod, wait = 20)$value$value, list(1))
})

test_that("worker connections close while a task runs", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    task <- workerTask(list(1), function(x) Sys.sleep(10))
    head <- sprintf(
        "POST /task HTTP/1.1\r\nX-BiocKube-Token: %s\r\n%s: %d\r\n\r\n",
        pod$token, "Content-Length", length(task)
    )

    elapsed <- system.time(
        res <- rawExchange(pod, c(charToRaw(head), task))
    )[["elapsed"]]
    expect_match(res, "^HTTP/1.1 202 ")
    expect_lt(elapsed, 5)
})

test_that("worker cancels a running task", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    stuck <- workerTask(list(1), function(x) Sys.sleep(60))

    expect_identical(podRequest(pod, "POST", "/task", stuck)$status, 202L)
    expect_identical(podRequest(pod, "POST", "/cancel")$status, 204L)
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"idle"', fixed = TRUE)
    expect_identical(podRequest(pod, "GET", "/result?wait=1")$status, 404L)
    expect_match(podLog(pod), "cancelled task 1")

    task <- workerTask(list(5), function(x) x, tag = 2L)
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    expect_identical(taskResult(pod)$value$value, list(5))
})

test_that("worker ends every process of a cancelled task", {
    skip_if_not(file.exists("/proc/self/stat"))
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    spawner <- function(file, tag) {
        workerTask(list(file), function(f) {
            script <- paste("trap '' TERM; echo $$ >", shQuote(f),
                            "; exec sleep 60")
            system2("sh", c("-c", shQuote(script)))
        }, tag = tag)
    }
    files <- tempfile(c("cancel", "done"))
    on.exit(unlink(files), add = TRUE)

    task <- spawner(files[1L], 1L)
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    pid <- waitPid(files[1L])
    expect_true(processAlive(pid))
    expect_identical(podRequest(pod, "POST", "/cancel")$status, 204L)
    expect_false(processAlive(pid))
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"idle"', fixed = TRUE)

    task <- spawner(files[2L], 2L)
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    pid <- waitPid(files[2L])
    expect_identical(podRequest(pod, "POST", "/done")$status, 204L)
    expect_identical(waitExit(pod), 0L)
    expect_false(processAlive(pid))
})

test_that("worker returns task errors per element", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))

    failing <- workerTask(list(1, 2), function(x) stop("boom"))
    expect_identical(podRequest(pod, "POST", "/task", failing)$status, 202L)
    value <- taskResult(pod)$value
    expect_false(value$success)
    expect_s3_class(value$value[[1L]], "remote_error")

    crash <- workerTask(list(1, 2, 3), function(x) {
        tools::pskill(Sys.getpid(), tools::SIGKILL)
    }, tag = 2L)
    expect_identical(podRequest(pod, "POST", "/task", crash)$status, 202L)
    value <- taskResult(pod)$value
    expect_false(value$success)
    expect_length(value$value, 3L)
    expect_s3_class(value$value[[3L]], "worker_comm_error")
    expect_match(conditionMessage(value$value[[1L]]),
                 "task process ended without a result")
})

test_that("worker evaluates tasks without the token and marked as a worker", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    task <- workerTask(list("BIOCKUBE_TOKEN", "BIOCKUBE_WORKER"), Sys.getenv)

    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    expect_identical(taskResult(pod)$value$value, list("", "1"))
})

test_that("worker rejects tasks it cannot read", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))

    res <- podRequest(pod, "POST", "/task", charToRaw("not a task"))
    expect_identical(res$status, 400L)
    expect_match(podText(res), "cannot read task")
    res <- podRequest(pod, "POST", "/task", serialize(list(run = 1), NULL))
    expect_identical(res$status, 400L)
    expect_match(podText(res), "task must be")
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"idle"', fixed = TRUE)
})

test_that("worker exits when told it is done", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))

    expect_identical(podRequest(pod, "POST", "/done")$status, 204L)
    expect_identical(waitExit(pod), 0L)
})

test_that("worker exits after the idle timeout", {
    pod <- startPod("worker.R", env = c(BIOCKUBE_IDLE = "1"), ready = FALSE)
    on.exit(stopPod(pod))

    expect_identical(waitExit(pod), 0L)
    expect_match(podLog(pod), "no requests for 1 seconds, exiting")
})

test_that("worker is not idle while a task runs or a poll is parked", {
    pod <- startPod("worker.R", env = c(BIOCKUBE_IDLE = "3"))
    on.exit(stopPod(pod))
    sleeper <- function(seconds, tag) {
        workerTask(list(seconds), function(s) Sys.sleep(s), tag = tag)
    }

    expect_identical(podRequest(pod, "POST", "/task", sleeper(4, 1L))$status,
                     202L)
    Sys.sleep(5)
    expect_identical(taskResult(pod, wait = 0)$tag, 1L)
    expect_identical(podRequest(pod, "DELETE", "/result")$status, 204L)

    expect_identical(podRequest(pod, "POST", "/task", sleeper(5, 2L))$status,
                     202L)
    expect_identical(taskResult(pod, wait = 10)$tag, 2L)
    expect_identical(podRequest(pod, "DELETE", "/result")$status, 204L)

    expect_identical(waitExit(pod), 0L)
    expect_match(podLog(pod), "no requests for 3 seconds, exiting")
})

test_that("worker keeps a result nobody collects for the idle timeout", {
    pod <- startPod("worker.R", env = c(BIOCKUBE_IDLE = "3"))
    on.exit(stopPod(pod))

    started <- Sys.time()
    task <- workerTask(list(4), function(s) Sys.sleep(s))
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    expect_identical(waitExit(pod), 0L)
    ## Four seconds for the task, then three without a request.
    expect_gte(as.numeric(Sys.time() - started, units = "secs"), 6.5)
    expect_match(podLog(pod), "no requests for 3 seconds, exiting")
})

test_that("worker does not start without a token", {
    pod <- startPod("worker.R", token = "", ready = FALSE)
    on.exit(stopPod(pod))

    expect_identical(waitExit(pod), 1L)
    expect_match(podLog(pod), "BIOCKUBE_TOKEN is not set")
})
