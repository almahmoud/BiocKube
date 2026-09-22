httpStatus <- function(expr)
{
    tryCatch(expr, httpError = function(e) e$status)
}

test_that("request heads are parsed", {
    pod <- podSource("server.R")
    head <- paste(
        "GET /outputs/a%20b?wait=25&x=1+2&flag HTTP/1.1",
        "Host: pod", "X-BiocKube-Token:  secret ", "Content-Length: 12",
        sep = "\r\n"
    )
    req <- pod$.parseHead(head)

    expect_identical(req$method, "GET")
    expect_identical(req$path, "/outputs/a b")
    expect_identical(req$query, c(wait = "25", x = "1 2", flag = ""))
    expect_identical(req$headers[["x-biockube-token"]], "secret")
    expect_identical(req$length, 12)
})

test_that("malformed request heads are rejected with a status", {
    pod <- podSource("server.R")
    line <- "POST /task HTTP/1.1"

    expect_identical(httpStatus(pod$.parseHead("garbage")), 400L)
    expect_identical(httpStatus(pod$.parseHead("get / HTTP/1.1")), 400L)
    expect_identical(httpStatus(pod$.parseHead("GET x HTTP/1.1")), 400L)
    expect_identical(httpStatus(pod$.parseHead("GET / HTTP/2")), 400L)
    expect_identical(httpStatus(pod$.parseHead("GET /%zz HTTP/1.1")), 400L)
    expect_identical(
        httpStatus(pod$.parseHead(paste0(line, "\r\nno colon"))), 400L
    )
    expect_identical(
        httpStatus(pod$.parseHead(paste0(line, "\r\nContent-Length: -1"))),
        400L
    )
    expect_identical(
        httpStatus(pod$.parseHead(paste0(
            line, "\r\nContent-Length: 1\r\nContent-Length: 2"
        ))),
        400L
    )
    expect_identical(
        httpStatus(pod$.parseHead(paste0(
            line, "\r\nTransfer-Encoding: chunked"
        ))),
        411L
    )
})

test_that("flat lists are encoded as JSON", {
    pod <- podSource("server.R")
    x <- list(state = "idle", run = NULL, tag = 3L, ok = TRUE, na = NA,
              num = 0.5, inf = Inf, vec = c("a", "b"),
              text = "q\"b\\s\n\t\u00e9")

    expect_identical(
        pod$.toJSON(x),
        paste0(
            '{"state":"idle","run":null,"tag":3,"ok":true,"na":null,',
            '"num":0.5,"inf":null,"vec":["a","b"],',
            '"text":"q\\"b\\\\s\\u000a\\u0009\u00e9"}'
        )
    )
    expect_identical(pod$.toJSON(list()), "{}")
})

test_that("tokens are compared exactly", {
    pod <- podSource("server.R")

    expect_true(pod$.secureEqual("abc123", "abc123"))
    expect_false(pod$.secureEqual("abc123", "abc124"))
    expect_false(pod$.secureEqual("abc123", "abc12"))
    expect_false(pod$.secureEqual("", "abc"))
})

test_that("environment settings must be positive numbers", {
    pod <- podSource("server.R")
    on.exit(Sys.unsetenv("BIOCKUBE_TEST_NUMBER"))

    expect_identical(pod$.envNumber("BIOCKUBE_TEST_NUMBER", 8080L), 8080L)
    Sys.setenv(BIOCKUBE_TEST_NUMBER = "30")
    expect_identical(pod$.envNumber("BIOCKUBE_TEST_NUMBER", 1), 30)
    Sys.setenv(BIOCKUBE_TEST_NUMBER = "soon")
    expect_error(pod$.envNumber("BIOCKUBE_TEST_NUMBER", 1),
                 "BIOCKUBE_TEST_NUMBER must be a positive number")
})

test_that("only health checks are served without the token", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))

    health <- podRequest(pod, "GET", "/healthz", token = NULL)
    expect_identical(health$status, 200L)
    expect_identical(podText(health), "ok")
    expect_identical(podRequest(pod, "GET", "/status", token = NULL)$status,
                     401L)
    expect_identical(podRequest(pod, "GET", "/status", token = "wrong")$status,
                     401L)
    expect_identical(podRequest(pod, "POST", "/done", token = "")$status, 401L)
    expect_true(pod$proc$is_alive())
})

test_that("unknown paths and methods are rejected", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))

    expect_identical(podRequest(pod, "GET", "/nothing")$status, 404L)
    res <- podRequest(pod, "PUT", "/result")
    expect_identical(res$status, 405L)
    expect_identical(res$headers$allow, "GET, DELETE")
})

test_that("malformed requests do not stop the server", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))

    expect_match(rawExchange(pod, "garbage\r\n\r\n"), "^HTTP/1.1 400 ")
    expect_match(
        rawExchange(pod, "GET /status HTTP/1.1\r\nbad header\r\n\r\n"),
        "^HTTP/1.1 400 "
    )
    expect_match(
        rawExchange(pod, paste0(
            "POST /task HTTP/1.1\r\nX-BiocKube-Token: test-token\r\n",
            "Transfer-Encoding: chunked\r\n\r\n"
        )),
        "^HTTP/1.1 411 "
    )
    long <- paste0("GET /status HTTP/1.1\r\nX-Long: ",
                   strrep("a", 17000L), "\r\n\r\n")
    expect_match(rawExchange(pod, long), "^HTTP/1.1 431 ")

    expect_identical(podRequest(pod, "GET", "/healthz")$status, 200L)
    expect_match(podLog(pod), "rejected request: 400 malformed request line")
})

test_that("clients expecting 100 Continue are asked for the body", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    con <- socketConnection("127.0.0.1", pod$port, open = "r+b",
                            blocking = TRUE, timeout = 20)
    on.exit(close(con), add = TRUE)
    writeBin(charToRaw(paste0(
        "POST /task HTTP/1.1\r\nX-BiocKube-Token: test-token\r\n",
        "Expect: 100-continue\r\nContent-Length: 10\r\n\r\n"
    )), con)

    expect_identical(rawToChar(readBin(con, "raw", 25L)),
                     "HTTP/1.1 100 Continue\r\n\r\n")
    writeBin(charToRaw("not a task"), con)
    expect_match(rawToChar(readBin(con, "raw", 1024L)), "^HTTP/1.1 400 ")
})

test_that("incomplete request heads do not hold up other requests", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    task <- workerTask(list(1), function(x) Sys.sleep(30))
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    stalled <- lapply(1:3, function(i) {
        podSocket(pod, "GET /status HTTP/1.1\r\n")
    })
    on.exit(for (con in stalled) close(con), add = TRUE)
    Sys.sleep(0.5)

    elapsed <- system.time({
        status <- podRequest(pod, "GET", "/status")
        poll <- podRequest(pod, "GET", "/result?wait=1")
    })[["elapsed"]]
    expect_identical(status$status, 200L)
    expect_identical(poll$status, 204L)
    expect_lt(elapsed, 3)
    for (con in stalled)
        expect_match(rawToChar(readBin(con, "raw", 1024L)), "^HTTP/1.1 408 ")
})

test_that("request bodies are read before a refusal closes the connection", {
    pod <- startPod("worker.R")
    on.exit(stopPod(pod))
    task <- workerTask(list(1), function(x) Sys.sleep(30))
    expect_identical(podRequest(pod, "POST", "/task", task)$status, 202L)
    post <- function(size, token = pod$token, expect = FALSE) {
        head <- paste0(
            "POST /task HTTP/1.1\r\nX-BiocKube-Token: ", token, "\r\n",
            if (expect) "Expect: 100-continue\r\n",
            "Content-Length: ", size, "\r\n\r\n"
        )
        body <- as.raw(sample(0:255, size, replace = TRUE))
        rawExchange(pod, c(charToRaw(head), body))
    }

    expect_match(post(8000000L, expect = TRUE),
                 "^HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 409 ")
    expect_match(post(8000000L), "^HTTP/1.1 409 ")
    expect_match(post(256000L, token = "wrong"), "^HTTP/1.1 401 ")
    expect_match(podText(podRequest(pod, "GET", "/status")),
                 '"state":"busy"', fixed = TRUE)
})
