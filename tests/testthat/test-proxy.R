test_that("proxy paths name the pod, its port and the endpoint", {
    expect_identical(.proxyPath("bioc", "worker-1", 8080L, "result"),
                     "/api/v1/namespaces/bioc/pods/worker-1:8080/proxy/result")
    expect_identical(.proxyPath("bioc", "worker-1", 8080L, "/result"),
                     "/api/v1/namespaces/bioc/pods/worker-1:8080/proxy/result")
    expect_identical(.proxyPath("bioc", "job-x", 9000, ""),
                     "/api/v1/namespaces/bioc/pods/job-x:9000/proxy/")
    expect_identical(
        .proxyPath("bioc", "job-x", 8080L, "inputs/counts matrix"),
        paste0("/api/v1/namespaces/bioc/pods/job-x:8080/proxy/inputs/",
               "counts%20matrix")
    )
})

test_that("proxy paths refuse segments that would leave the endpoint", {
    expect_error(.proxyPath("bioc", "a/b", 8080L, "x"), "`pod` must be")
    expect_error(.proxyPath("..", "a", 8080L, "x"), "`namespace` must be")
    expect_error(.proxyPath("bioc", "a", 8080L, "../pods"), "`path` must be")
    expect_error(.proxyPath("bioc", "a", 0L, "x"), "`port` must be")
    expect_error(.proxyPath("bioc", "a", "http", "x"), "`port` must be")
})

test_that("a pod answers requests sent through the API server", {
    pod <- startPod("worker.R", token = "token-for-the-session")
    on.exit(stopPod(pod))
    api <- fakeApi(pod = pod)
    call <- function(...) .proxyRequest(api$client, "bioc", "worker-1",
                                        8080L, ...)

    status <- call("status", token = pod$token, as = "parsed")
    expect_identical(status$state, "idle")
    expect_identical(status$biocparallel,
                     as.character(packageVersion("BiocParallel")))
    expect_identical(
        lastRequest(api)$path,
        "/api/v1/namespaces/bioc/pods/worker-1:8080/proxy/status"
    )

    expect_identical(trimws(call("healthz", as = "text")), "ok")
    expect_null(requestHeader(lastRequest(api), "X-BiocKube-Token"))

    expect_error(call("status", token = "wrong"),
                 "Pod bioc/worker-1: unauthorized",
                 class = "biockube_error_unauthorized")
    expect_error(call("status", token = "wrong"), class = "kube_error")
})

test_that("a task and its result travel through the proxy as bytes", {
    pod <- startPod("worker.R", token = "token-for-the-session")
    on.exit(stopPod(pod))
    api <- fakeApi(pod = pod)
    call <- function(...) .proxyRequest(api$client, "bioc", "worker-1",
                                        8080L, token = pod$token, ...)

    task <- workerTask(list(1, 2), function(x) {
        Sys.sleep(2)
        x * 2
    }, run = "r1", tag = 3L)
    accepted <- call("task", method = "POST", body = task, as = "response")
    expect_identical(accepted$status_code, 202L)
    sent <- lastRequest(api)
    expect_identical(requestHeader(sent, "Content-Length"),
                     format(length(task)))
    expect_identical(requestHeader(sent, "Content-Type"),
                     "application/octet-stream")

    started <- Sys.time()
    empty <- call("result", query = list(wait = 1), as = "response")
    expect_identical(empty$status_code, 204L)
    expect_gte(as.numeric(Sys.time() - started, units = "secs"), 1)

    result <- unserialize(call("result", query = list(wait = 20),
                               timeout = 30))
    expect_identical(result$run, "r1")
    expect_identical(result$tag, 3L)
    expect_identical(result$value$value, list(2, 4))

    acknowledged <- call("result", method = "DELETE", as = "response")
    expect_identical(acknowledged$status_code, 204L)
    expect_null(requestHeader(lastRequest(api), "Content-Length"))

    busy <- call("task", method = "POST", body = task, as = "response")
    expect_identical(busy$status_code, 202L)
    expect_error(call("result", query = list(wait = 10), timeout = 1),
                 "timed out", class = "kube_error_timeout")
    expect_error(call("result", query = list(wait = 10), timeout = 1),
                 class = "biockube_pod_error")
})

test_that("the pool returns the request that completes first", {
    quick <- startPod("worker.R", token = "quick")
    on.exit(stopPod(quick), add = TRUE)
    slow <- startPod("worker.R", token = "slow")
    on.exit(stopPod(slow), add = TRUE)
    quickApi <- fakeApi(pod = quick)
    slowApi <- fakeApi(pod = slow)
    submit <- function(api, pod, task) {
        .proxyRequest(api$client, "bioc", "worker-1", 8080L, "task",
                      method = "POST", body = task, token = pod$token,
                      as = "response")
    }
    expect_identical(
        submit(quickApi, quick, workerTask(list(1), function(x) x))$status_code,
        202L
    )
    expect_identical(
        submit(slowApi, slow, workerTask(list(1), function(x) {
            Sys.sleep(30)
            x
        }))$status_code,
        202L
    )

    pool <- .newPool(2L)
    answers <- list()
    poll <- function(api, pod, name) {
        request <- .proxyHandle(api$client, "bioc", "worker-1", 8080L,
                                "result", token = pod$token,
                                query = list(wait = 30))
        curl::multi_add(request$handle, pool = pool,
                        done = function(response) {
                            answers[[name]] <<- response
                        },
                        fail = function(message) {
                            answers[[name]] <<- message
                        })
        request$handle
    }
    poll(quickApi, quick, "quick")
    pending <- poll(slowApi, slow, "slow")
    run <- .poll(pool, timeout = 60)
    curl::multi_cancel(pending)

    expect_identical(run$success, 1L)
    expect_identical(run$pending, 1L)
    expect_named(answers, "quick")
    expect_identical(answers$quick$status_code, 200L)
    expect_identical(unserialize(answers$quick$content)$value$value, list(1))
})

test_that("responses collected from the pool become pod conditions", {
    busy <- list(status_code = 409L, content = charToRaw("busy\n"))
    condition <- .podResponseError(busy, "bioc", "worker-1")
    expect_s3_class(condition, "biockube_error_conflict")
    expect_s3_class(condition, "biockube_pod_error")
    expect_identical(conditionMessage(condition), "Pod bioc/worker-1: busy")
    expect_identical(condition$status, 409L)

    quiet <- list(status_code = 500L, content = raw())
    expect_identical(conditionMessage(.podResponseError(quiet, "bioc", "w")),
                     "Pod bioc/w: the request failed with status 500")
    expect_s3_class(.podResponseError(quiet, "bioc", "w"),
                    "biockube_error_podfailure")

    expect_null(.podResponseError(list(status_code = 204L), "bioc", "w"))
})

test_that("pod status tells a working pod from a dead one", {
    api <- fakeApi(list(
        "GET /api/v1/namespaces/bioc/pods/working" =
            jsonHandler(podObject("working", restarts = 2L)),
        "GET /api/v1/namespaces/bioc/pods/killed" =
            jsonHandler(podObject("killed", phase = "Failed", ready = "False",
                                  state = list(terminated = list(
                                      exitCode = 137L, reason = "OOMKilled"
                                  )))),
        "GET /api/v1/namespaces/bioc/pods/pulling" =
            jsonHandler(podObject("pulling", phase = "Pending",
                                  ready = "False",
                                  state = list(waiting = list(
                                      reason = "ImagePullBackOff",
                                      message = "manifest unknown"
                                  )))),
        "GET /api/v1/namespaces/bioc/pods/homeless" =
            jsonHandler(podObject("homeless", phase = "Pending",
                                  ready = "False",
                                  state = list(waiting = list(
                                      reason = "ContainerCreating"
                                  )),
                                  conditions = list(list(
                                      type = "PodScheduled", status = "False",
                                      reason = "Unschedulable",
                                      message = "0/3 nodes are available"
                                  )))),
        "GET /api/v1/namespaces/bioc/pods/gone" =
            statusHandler(404L, "NotFound", "pods \"gone\" not found")
    ))
    status <- function(name) .podStatus(api$client, "bioc", name)

    working <- status("working")
    expect_true(working$exists)
    expect_true(working$ready)
    expect_false(working$failed)
    expect_identical(working$phase, "Running")
    expect_identical(working$restarts, 2L)

    killed <- status("killed")
    expect_true(killed$failed)
    expect_false(killed$ready)
    expect_identical(killed$reason, "OOMKilled")

    pulling <- status("pulling")
    expect_true(pulling$failed)
    expect_identical(pulling$reason, "ImagePullBackOff")
    expect_identical(pulling$message, "manifest unknown")

    ## A cluster that adds nodes on demand places it once the node is up.
    homeless <- status("homeless")
    expect_false(homeless$failed)
    expect_identical(homeless$reason, NA_character_)
    expect_identical(homeless$scheduling,
                     "Unschedulable: 0/3 nodes are available")
    expect_false(.podEnded(homeless))
    expect_identical(working$scheduling, NA_character_)

    gone <- status("gone")
    expect_false(gone$exists)
    expect_true(gone$failed)
    expect_identical(gone$reason, "NotFound")

    expect_true(.podEnded(killed))
    expect_true(.podEnded(gone))
    expect_false(.podEnded(working))
    expect_true(.podEnded(.podState(podObject(
        "exited", phase = "Succeeded", ready = "False",
        state = list(terminated = list(exitCode = 0L, reason = "Completed"))
    ))))
})

test_that("a gated pod is waiting to be scheduled, not failed", {
    gated <- .podState(podObject(
        "gated", phase = "Pending", ready = "False",
        state = list(waiting = list(reason = "ContainerCreating")),
        conditions = list(list(type = "PodScheduled", status = "False",
                               reason = "SchedulingGated"))
    ))
    expect_false(gated$failed)
    expect_identical(gated$scheduling, "SchedulingGated")
})
