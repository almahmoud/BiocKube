## HTTP/1.1 server shared by worker.R and runner.R. Base R only: pod images
## need not have any other package. Request heads are read without blocking
## from all open connections; a request is handled once its head is complete,
## reading its body as the handler needs it, and every response closes its
## connection. Long polls are parked and answered from the loop.

.headerTimeout <- 5
.bodyTimeout <- 30
.writeTimeout <- 30
.lingerTimeout <- 2
.killGrace <- 2
.maxHeaderBytes <- 16384L
.maxConnections <- 32L
.drainLimit <- 1048576L
.chunkSize <- 1048576L
.tick <- 0.2

.statusText <- c(
    "200" = "OK", "201" = "Created", "202" = "Accepted", "204" = "No Content",
    "400" = "Bad Request", "401" = "Unauthorized",
    "404" = "Not Found", "405" = "Method Not Allowed",
    "408" = "Request Timeout", "409" = "Conflict", "411" = "Length Required",
    "422" = "Unprocessable Content",
    "431" = "Request Header Fields Too Large",
    "500" = "Internal Server Error"
)

.now <- function() as.numeric(Sys.time())

.log <- function(...)
{
    cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S "), ..., "\n", sep = "",
        file = stderr())
}

.takeToken <- function()
{
    token <- Sys.getenv("BIOCKUBE_TOKEN")
    ## Keep the token out of the environment of evaluated code.
    Sys.unsetenv("BIOCKUBE_TOKEN")
    if (!nzchar(token))
        stop("BIOCKUBE_TOKEN is not set", call. = FALSE)
    token
}

.envNumber <- function(name, default)
{
    value <- Sys.getenv(name)
    if (!nzchar(value))
        return(default)
    number <- suppressWarnings(as.numeric(value))
    if (is.na(number) || number <= 0)
        stop(name, " must be a positive number, not '", value, "'",
             call. = FALSE)
    number
}

.secureEqual <- function(a, b)
{
    a <- charToRaw(a)
    b <- charToRaw(b)
    if (length(a) != length(b))
        return(FALSE)
    sum(as.integer(xor(a, b))) == 0L
}

## JSON

.jsonString <- function(x)
{
    x <- gsub("\\", "\\\\", enc2utf8(as.character(x)), fixed = TRUE)
    x <- gsub("\"", "\\\"", x, fixed = TRUE)
    for (code in seq_len(31L))
        x <- gsub(intToUtf8(code), sprintf("\\u%04x", code), x, fixed = TRUE)
    paste0("\"", x, "\"")
}

.jsonValue <- function(x)
{
    if (is.null(x))
        return("null")
    if (length(x) != 1L) {
        values <- vapply(x, .jsonValue, character(1), USE.NAMES = FALSE)
        return(paste0("[", paste(values, collapse = ","), "]"))
    }
    if (is.na(x))
        "null"
    else if (is.logical(x))
        if (x) "true" else "false"
    else if (is.numeric(x))
        if (is.finite(x)) format(x, digits = 15L, trim = TRUE) else "null"
    else
        .jsonString(x)
}

## Encodes a named list of NULL or atomic values as a JSON object.
.toJSON <- function(x)
{
    if (!length(x))
        return("{}")
    values <- vapply(x, .jsonValue, character(1), USE.NAMES = FALSE)
    fields <- paste0(.jsonString(names(x)), ":", values, collapse = ",")
    paste0("{", fields, "}")
}

## Conditions

.httpError <- function(status, message)
{
    structure(
        class = c("httpError", "error", "condition"),
        list(message = message, call = NULL, status = as.integer(status))
    )
}

.clientGone <- function()
{
    structure(
        class = c("httpClosed", "error", "condition"),
        list(message = "client closed the connection", call = NULL)
    )
}

## Responses

.response <- function(status, body = NULL, type = NULL, file = NULL,
                      headers = character(), then = NULL)
{
    if (is.character(body)) {
        body <- charToRaw(enc2utf8(paste0(body, "\n")))
        if (is.null(type))
            type <- "text/plain; charset=utf-8"
    }
    if (is.null(type))
        type <- "application/octet-stream"
    list(status = as.integer(status), body = body, type = type, file = file,
         headers = headers, then = then)
}

.json <- function(x)
{
    .response(200L, charToRaw(enc2utf8(.toJSON(x))), type = "application/json")
}

## A long poll: `ready()` returns a response or NULL; `otherwise` is sent
## when nothing is ready after `wait` seconds.
.defer <- function(ready, wait, otherwise)
{
    structure(
        list(ready = ready, deadline = .now() + wait, otherwise = otherwise),
        class = "httpDeferred"
    )
}

.route <- function(method, pattern, handler)
{
    list(method = method, pattern = pattern, handler = handler)
}

## Parsing

.urlDecode <- function(x, plus = FALSE)
{
    decode <- function(s) {
        if (grepl("%(?![0-9A-Fa-f]{2})", s, perl = TRUE))
            stop("invalid percent-encoding")
        if (plus)
            s <- gsub("+", " ", s, fixed = TRUE)
        out <- utils::URLdecode(s)
        if (!validUTF8(out))
            stop("invalid UTF-8")
        out
    }
    tryCatch(
        vapply(x, decode, character(1), USE.NAMES = FALSE),
        error = function(e) stop(.httpError(400L, "invalid URL encoding"))
    )
}

.parseQuery <- function(query)
{
    pairs <- strsplit(query, "&", fixed = TRUE)[[1L]]
    pairs <- pairs[nzchar(pairs)]
    eq <- regexpr("=", pairs, fixed = TRUE)
    keys <- ifelse(eq > 0L, substr(pairs, 1L, eq - 1L), pairs)
    values <- ifelse(eq > 0L, substring(pairs, eq + 1L), "")
    stats::setNames(.urlDecode(values, TRUE), .urlDecode(keys, TRUE))
}

## Parses the request line and header fields (without the final blank line).
.parseHead <- function(text)
{
    lines <- strsplit(text, "\r\n", fixed = TRUE)[[1L]]
    parts <- strsplit(lines[1L], " ", fixed = TRUE)[[1L]]
    if (length(parts) != 3L || !grepl("^[A-Z]+$", parts[1L]) ||
        !startsWith(parts[2L], "/") || !grepl("^HTTP/1\\.[01]$", parts[3L]))
        stop(.httpError(400L, "malformed request line"))

    fields <- lines[-1L]
    colon <- regexpr(":", fields, fixed = TRUE)
    names <- tolower(substr(fields, 1L, colon - 1L))
    if (any(colon < 2L) || any(!grepl("^[!#$%&'*+.^_`|~0-9a-z-]+$", names)))
        stop(.httpError(400L, "malformed header field"))
    headers <- stats::setNames(trimws(substring(fields, colon + 1L)), names)

    if ("transfer-encoding" %in% names)
        stop(.httpError(411L, "request bodies need a Content-Length"))
    lengths <- unique(headers[names == "content-length"])
    if (length(lengths) > 1L || !all(grepl("^[0-9]{1,15}$", lengths)))
        stop(.httpError(400L, "invalid Content-Length"))

    target <- parts[2L]
    q <- regexpr("?", target, fixed = TRUE)
    list(
        method = parts[1L],
        target = target,
        path = .urlDecode(if (q > 0L) substr(target, 1L, q - 1L) else target),
        query = if (q > 0L) .parseQuery(substring(target, q + 1L))
                else character(),
        headers = headers,
        length = if (length(lengths)) as.numeric(lengths) else 0
    )
}

## Reading and writing

## Returns the next bytes from `con`, waiting until `deadline` at most.
.readSome <- function(con, deadline, n = 65536L)
{
    repeat {
        wait <- deadline - .now()
        if (wait <= 0)
            stop(.httpError(408L, "timed out reading the request"))
        if (socketSelect(list(con), timeout = wait)) {
            chunk <- readBin(con, "raw", n)
            if (length(chunk))
                return(chunk)
            if (!isIncomplete(con))
                stop(.clientGone())
        }
    }
}

## Reads what has arrived on a connection whose request head is incomplete.
## Returns the request once its head is complete, NULL until then.
.readHead <- function(conn)
{
    chunk <- readBin(conn$con, "raw", 65536L)
    if (!length(chunk) && !isIncomplete(conn$con))
        stop(.clientGone())
    conn$buffer <- c(conn$buffer, chunk)
    end <- grepRaw("\r\n\r\n", conn$buffer, fixed = TRUE)
    if (!length(end) && length(conn$buffer) <= .maxHeaderBytes)
        return(NULL)
    if (!length(end) || end > .maxHeaderBytes)
        stop(.httpError(431L, "request header too large"))
    head <- tryCatch(
        rawToChar(conn$buffer[seq_len(end - 1L)]),
        error = function(e) stop(.httpError(400L, "malformed request header"))
    )
    req <- list2env(.parseHead(head))
    rest <- conn$buffer[-seq_len(end + 3L)]
    req$con <- conn$con
    req$buffer <- rest[seq_len(min(length(rest), req$length))]
    req$unread <- req$length - length(req$buffer)
    req$continued <- FALSE
    req$failed <- FALSE
    req
}

.expectsContinue <- function(req)
{
    identical(tolower(req$headers["expect"][[1L]]), "100-continue")
}

## Asks a client that waits for 100 Continue to send the body.
.continue <- function(req)
{
    if (req$unread > 0 && !req$continued && .expectsContinue(req)) {
        req$continued <- TRUE
        .writeAll(req$con, charToRaw("HTTP/1.1 100 Continue\r\n\r\n"))
    }
}

## Returns the next body bytes. A connection that fails while its body is
## read is not read again.
.receive <- function(req)
{
    chunk <- withCallingHandlers(
        .readSome(req$con, .now() + .bodyTimeout, min(req$unread, .chunkSize)),
        error = function(e) req$failed <- TRUE
    )
    req$unread <- req$unread - length(chunk)
    chunk
}

## Reads the request body into memory, or into the file `path` when given.
## Stalls are bounded by `.bodyTimeout` seconds without data.
.readBody <- function(req, path = NULL)
{
    .continue(req)
    got <- length(req$buffer)
    body <- out <- NULL
    if (is.null(path)) {
        body <- raw(req$length)
        body[seq_len(got)] <- req$buffer
    } else {
        out <- file(path, "wb")
        on.exit(close(out))
        writeBin(req$buffer, out)
    }
    req$buffer <- raw()
    while (req$unread > 0) {
        chunk <- .receive(req)
        if (is.null(out))
            body[got + seq_along(chunk)] <- chunk
        else
            writeBin(chunk, out)
        got <- got + length(chunk)
    }
    body
}

## Reads and discards the rest of the request body, so that closing the
## connection does not reset it before the client has read the response.
.drainBody <- function(req)
{
    if (req$failed || req$unread <= 0)
        return(invisible())
    tryCatch({
        .continue(req)
        while (req$unread > 0)
            .receive(req)
    }, error = function(e) NULL)
}

.writeAll <- function(con, x)
{
    if (!length(x))
        return(invisible())
    for (start in seq(1, length(x), by = .chunkSize)) {
        end <- min(start + .chunkSize - 1, length(x))
        withCallingHandlers(
            writeBin(x[start:end], con),
            warning = function(w) stop(.clientGone())
        )
    }
}

.writeResponse <- function(con, res)
{
    fields <- c(res$headers, Connection = "close")
    if (res$status != 204L) {
        size <- if (is.null(res$file)) length(res$body) else
            file.size(res$file)
        fields <- c(fields, "Content-Type" = res$type,
                    "Content-Length" = format(size, scientific = FALSE))
    }
    head <- paste0(
        "HTTP/1.1 ", res$status, " ", .statusText[[as.character(res$status)]],
        "\r\n", paste0(names(fields), ": ", fields, "\r\n", collapse = ""),
        "\r\n"
    )
    .writeAll(con, charToRaw(head))
    if (res$status == 204L)
        return(invisible())
    if (is.null(res$file)) {
        .writeAll(con, res$body)
        return(invisible())
    }
    input <- file(res$file, "rb")
    on.exit(close(input))
    while (length(chunk <- readBin(input, "raw", .chunkSize)))
        .writeAll(con, chunk)
}

## Dispatch

.dispatch <- function(req, routes, token)
{
    if (req$method == "GET" && req$path == "/healthz")
        return(.response(200L, "ok"))
    given <- req$headers["x-biockube-token"][[1L]]
    if (is.na(given) || !.secureEqual(given, token))
        return(.response(401L, "unauthorized"))
    req$authorised <- TRUE

    matches <- Filter(function(r) grepl(r$pattern, req$path), routes)
    if (!length(matches))
        return(.response(404L, "not found"))
    methods <- vapply(matches, `[[`, character(1), "method")
    if (!req$method %in% methods) {
        allow <- c(Allow = paste(unique(methods), collapse = ", "))
        return(.response(405L, "method not allowed", headers = allow))
    }
    route <- matches[[match(req$method, methods)]]
    args <- regmatches(req$path, regexec(route$pattern, req$path))[[1L]][-1L]
    do.call(route$handler, c(list(req), as.list(args)))
}

## Connections

.conn <- function(con, timeout, left = 0)
{
    list2env(list(con = con, buffer = raw(), left = left,
                  deadline = .now() + timeout), parent = emptyenv())
}

## Adds `conn` to the connection list `set` of `srv`, closing the oldest
## connection of a full list.
.addConn <- function(srv, set, conn)
{
    conns <- srv[[set]]
    if (length(conns) >= .maxConnections) {
        close(conns[[1L]]$con)
        conns <- conns[-1L]
    }
    srv[[set]] <- c(conns, list(conn))
}

## Sends `res` and closes the connection. The unread body of an authorised
## request is read first; that of any other request is drained by the loop
## after the response, up to `.drainLimit` bytes.
.finish <- function(srv, req, res, con = req$con)
{
    authorised <- isTRUE(req$authorised)
    if (authorised)
        .drainBody(req)
    if (!is.null(res))
        tryCatch(.writeResponse(con, res), error = function(e) NULL)
    left <- if (is.null(req) || is.null(res) || authorised || req$failed) 0
            else min(req$unread, .drainLimit)
    if (left > 0)
        .addConn(srv, "closing", .conn(con, .lingerTimeout, left))
    else
        close(con)
    if (authorised)
        srv$lastSeen <- .now()
    if (is.null(res))
        return(invisible())
    if (is.null(req))
        .log("rejected request: ", res$status, " ", trimws(rawToChar(res$body)))
    else if (srv$logRequests && req$path != "/healthz")
        .log(req$method, " ", req$target, " ", res$status)
    if (!is.null(res$then))
        res$then()
}

## Answers a parked request if it is ready or has expired. Returns TRUE when
## the request was answered.
.answerParked <- function(srv, parked)
{
    res <- parked$deferred$ready()
    if (is.null(res) && .now() < parked$deferred$deadline)
        return(FALSE)
    .finish(srv, parked$req,
            if (is.null(res)) parked$deferred$otherwise else res)
    TRUE
}

## Handles a request whose head has been read.
.handle <- function(srv, req)
{
    res <- tryCatch(
        .dispatch(req, srv$routes, srv$token),
        httpError = function(e) .response(e$status, conditionMessage(e)),
        httpClosed = function(e) NULL,
        error = function(e) {
            .log("error: ", conditionMessage(e))
            .response(500L, conditionMessage(e))
        }
    )
    if (!inherits(res, "httpDeferred"))
        return(.finish(srv, req, res))
    parked <- list(req = req, deferred = res)
    if (!.answerParked(srv, parked))
        srv$parked <- c(srv$parked, list(parked))
}

## Reads the request head arriving on `conn`, and handles the request once
## the head is complete. Returns FALSE while the head is incomplete.
.advance <- function(srv, conn, ready)
{
    req <- tryCatch({
        req <- if (ready) .readHead(conn)
        if (is.null(req) && .now() > conn$deadline)
            stop(.httpError(408L, "timed out reading the request"))
        req
    },
    httpError = identity,
    httpClosed = identity,
    error = function(e) .httpError(400L, "malformed request header"))
    if (is.null(req))
        return(FALSE)
    if (inherits(req, "httpClosed"))
        close(conn$con)
    else if (inherits(req, "httpError"))
        .finish(srv, NULL, .response(req$status, conditionMessage(req)),
                conn$con)
    else
        .handle(srv, req)
    TRUE
}

## Discards what has arrived on a closing connection and closes it once the
## body is drained, the client is gone or its time is up. Returns TRUE when
## the connection was closed.
.drainSome <- function(conn, ready)
{
    if (ready)
        tryCatch({
            chunk <- readBin(conn$con, "raw", min(conn$left, 65536L))
            conn$left <- conn$left - length(chunk)
            if (!length(chunk) && !isIncomplete(conn$con))
                conn$left <- 0
        }, error = function(e) conn$left <- 0)
    if (conn$left > 0 && .now() < conn$deadline)
        return(FALSE)
    close(conn$con)
    TRUE
}

.accept <- function(server)
{
    tryCatch(
        socketAccept(server, blocking = FALSE, open = "r+b",
                     timeout = .writeTimeout),
        error = function(e) {
            .log("cannot accept a connection: ", conditionMessage(e))
            Sys.sleep(.tick)
            NULL
        }
    )
}

## Waits up to `.tick` seconds for new connections and request data, then
## handles every request whose head is complete.
.step <- function(srv, server)
{
    opening <- srv$opening
    closing <- srv$closing
    socks <- c(list(server), lapply(c(opening, closing), `[[`, "con"))
    ready <- socketSelect(socks, timeout = .tick)
    readyOpening <- ready[1L + seq_along(opening)]
    readyClosing <- ready[1L + length(opening) + seq_along(closing)]

    closed <- vapply(seq_along(closing), function(i) {
        .drainSome(closing[[i]], readyClosing[i])
    }, logical(1))
    srv$closing <- closing[!closed]
    srv$opening <- list()
    for (i in seq_along(opening))
        if (!.advance(srv, opening[[i]], readyOpening[i]))
            srv$opening <- c(srv$opening, opening[i])
    if (ready[1L]) {
        con <- .accept(server)
        if (!is.null(con))
            .addConn(srv, "opening", .conn(con, .headerTimeout))
    }
}

## Processes

## Fields of /proc/<pid>/stat after the command name, NULL when there is no
## such process.
.procStat <- function(pid)
{
    line <- tryCatch(
        suppressWarnings(readLines(paste0("/proc/", pid, "/stat"), 1L,
                                   warn = FALSE)),
        error = function(e) character()
    )
    if (!length(line))
        return(NULL)
    strsplit(sub("^.*\\) ", "", line), " ", fixed = TRUE)[[1L]]
}

.alive <- function(pid)
{
    stat <- .procStat(pid)
    !is.null(stat) && stat[1L] != "Z"
}

## `pids` and all their descendants, from the parent links in /proc.
.processTree <- function(pids)
{
    all <- suppressWarnings(as.integer(list.files("/proc", "^[0-9]+$")))
    parents <- vapply(all, function(pid) {
        stat <- .procStat(pid)
        if (is.null(stat)) NA_integer_ else as.integer(stat[2L])
    }, integer(1))
    repeat {
        children <- setdiff(all[parents %in% pids], pids)
        if (!length(children))
            return(pids)
        pids <- c(pids, children)
    }
}

## Ends the process tree rooted at `pid` with SIGTERM, and with SIGKILL what
## is left of it after `.killGrace` seconds. The tree is stopped while it is
## collected so that no process can start another unseen.
.killTree <- function(pid)
{
    tree <- integer()
    repeat {
        found <- setdiff(.processTree(c(pid, tree)), tree)
        if (!length(found))
            break
        tools::pskill(found, tools::SIGSTOP)
        tree <- c(tree, found)
    }
    tools::pskill(tree, tools::SIGTERM)
    tools::pskill(tree, tools::SIGCONT)
    deadline <- .now() + .killGrace
    repeat {
        alive <- tree[vapply(tree, .alive, logical(1))]
        if (!length(alive) || .now() > deadline)
            break
        Sys.sleep(0.05)
    }
    if (length(alive))
        tools::pskill(.processTree(alive), tools::SIGKILL)
    invisible(tree)
}

## Sockets inherited by a forked child are closed there so that they do not
## outlive the responses sent by the server.
.fork <- function(expr)
{
    parallel::mcparallel({
        closeAllConnections()
        expr
    })
}

## Ends a job started by .fork() and every process it started, then reaps it.
.stopJob <- function(job)
{
    .killTree(job$pid)
    invisible(suppressWarnings(
        parallel::mccollect(job, wait = FALSE, timeout = .killGrace)
    ))
}

## Serves `routes` on `port` until a handler or `poll` ends the process.
## `poll(idle)` runs about every `.tick` seconds with the number of seconds
## since an authenticated request was last answered, 0 while one is parked.
.serve <- function(routes, token, port, poll = function(idle) NULL,
                   logRequests = TRUE)
{
    server <- serverSocket(as.integer(port))
    on.exit(close(server))
    srv <- list2env(list(routes = routes, token = token, lastSeen = .now(),
                         logRequests = logRequests,
                         opening = list(), closing = list(), parked = list()),
                    parent = emptyenv())
    .log("listening on port ", port)
    repeat {
        .step(srv, server)
        poll(if (length(srv$parked)) 0 else .now() - srv$lastSeen)
        answered <- vapply(srv$parked, function(parked) {
            .answerParked(srv, parked)
        }, logical(1))
        srv$parked <- srv$parked[!answered]
    }
}
