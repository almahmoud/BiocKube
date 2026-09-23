## Requests to a pod through the API server's pod proxy subresource. The API
## server authenticates the caller and then removes the Authorization header,
## so the pod authenticates the session itself with X-BiocKube-Token.

## Seconds for a request that is answered at once. Tasks and results are not
## held to it: their size, and with it the time they need to travel, is
## whatever the call carries.
.requestTimeout <- 30

## Statuses of pod proxy responses, as condition class suffixes.
.podErrorKinds <- c(
    "400" = "badrequest", "401" = "unauthorized", "403" = "forbidden",
    "404" = "notfound", "405" = "methodnotallowed", "409" = "conflict",
    "411" = "lengthrequired", "422" = "unprocessable", "500" = "podfailure"
)

#' Path of a pod proxy endpoint
#'
#' Builds `/api/v1/namespaces/<namespace>/pods/<pod>:<port>/proxy/<path>`.
#' The path is built here rather than with the helpers in `kube`, which
#' escape the `":"` that separates the pod from its port.
#'
#' @param namespace,pod Namespace and name of the pod.
#' @param port Container port the pod listens on.
#' @param path Endpoint under the proxy, such as `"result"` or
#'   `"inputs/counts"`. Each segment is escaped.
#' @return The request path, a string starting with `/`.
#' @noRd
.proxyPath <- function(namespace, pod, port, path = "") {
    paste0(
        "/api/v1/namespaces/", .pathSegment(namespace, "namespace"),
        "/pods/", .pathSegment(pod, "pod"), ":", .proxyPort(port),
        "/proxy", .proxySuffix(path)
    )
}

## Same rule as client-go: "." and ".." would be resolved as relative paths.
.pathSegment <- function(x, arg) {
    if (!is.character(x) || length(x) != 1L || is.na(x) || !nzchar(x) ||
        x %in% c(".", "..") || grepl("[/%]", x))
        stop(sprintf("`%s` must be a non-empty string other than ", arg),
             "\".\" and \"..\", without \"/\" or \"%\".", call. = FALSE)
    curl::curl_escape(x)
}

.proxyPort <- function(port) {
    if (!is.numeric(port) || length(port) != 1L || is.na(port) ||
        port != trunc(port) || port < 1 || port > 65535)
        stop("`port` must be a port number between 1 and 65535.",
             call. = FALSE)
    format(as.integer(port))
}

## The API server redirects "/proxy" to "/proxy/", so an empty path keeps
## the trailing slash.
.proxySuffix <- function(path) {
    if (!is.character(path) || length(path) != 1L || is.na(path))
        stop("`path` must be a string.", call. = FALSE)
    path <- sub("^/", "", path)
    if (!nzchar(path))
        return("/")
    segments <- strsplit(path, "/", fixed = TRUE)[[1L]]
    segments <- vapply(segments, .pathSegment, character(1L), "path",
                       USE.NAMES = FALSE)
    paste0("/", paste(segments, collapse = "/"))
}

#' Send a request to a pod through the API server
#'
#' @param client A `kube_client`.
#' @param namespace,pod,port The pod and the port it listens on.
#' @param path Endpoint under the proxy, such as `"status"`.
#' @param method HTTP method.
#' @param body Request body as a raw vector, or `NULL`.
#' @param token Session token sent as `X-BiocKube-Token`, or `NULL` for the
#'   unauthenticated `/healthz` endpoint.
#' @param query Named list of query parameters.
#' @param as What to return: `"raw"`, `"parsed"`, `"text"` or `"response"`.
#'   `"response"` keeps the status code, which tells an empty body from a
#'   long poll that ended without a result.
#' @param timeout Maximum duration of the request in seconds. Pod proxy
#'   requests are long running, so the API server does not time them out.
#'
#' @return The response body, or the whole response for `as = "response"`.
#' @section Errors:
#' A response with status 400 or above, and a failure to reach the API
#' server, raise a condition of class `biockube_pod_error`. Statuses the pod
#' scripts use add a class of their own, such as `biockube_error_conflict`
#' for the 409 a busy worker answers with, and conditions coming from `kube`
#' keep their own classes, among them `kube_error_timeout`.
#' @noRd
.proxyRequest <- function(client, namespace, pod, port, path,
                          method = "GET", body = NULL, token = NULL,
                          query = NULL, as = "raw", timeout = NULL) {
    as <- match.arg(as, c("raw", "parsed", "text", "response"))
    method <- .proxyMethod(method)
    .checkRawBody(body)
    tryCatch(
        kube::kube_request(
            client, method = method,
            path = .proxyPath(namespace, pod, port, path), query = query,
            body = body,
            content_type = if (length(body)) "application/octet-stream",
            accept = "*/*", as = as,
            headers = .proxyHeaders(token, body, method), timeout = timeout
        ),
        kube_error = function(e) stop(.proxyError(e, namespace, pod)),
        curl_error = function(e) stop(.proxyError(e, namespace, pod))
    )
}

#' Build a curl handle for a pod proxy request
#'
#' The handle is performed by the caller, typically with [curl::multi_add()]
#' in a pool from `.newPool()`. Statuses and transport failures are the
#' caller's to interpret; `.podResponseError()` turns a response into the
#' same conditions `.proxyRequest()` raises.
#'
#' @inheritParams .proxyRequest
#' @param stall Seconds the request may go without sending or receiving a
#'   byte before it fails, or `NULL`. Unlike `timeout`, it leaves a large
#'   response all the time it needs as long as its bytes keep coming.
#' @return A list with elements `handle` and `url`, as [kube::kube_handle()]
#'   returns.
#' @noRd
.proxyHandle <- function(client, namespace, pod, port, path,
                         method = "GET", body = NULL, token = NULL,
                         query = NULL, timeout = NULL, stall = NULL) {
    method <- .proxyMethod(method)
    .checkRawBody(body)
    request <- kube::kube_handle(
        client, method = method,
        path = .proxyPath(namespace, pod, port, path), query = query,
        body = body,
        content_type = if (length(body)) "application/octet-stream",
        accept = "*/*", headers = .proxyHeaders(token, body, method)
    )
    if (!is.null(timeout))
        curl::handle_setopt(request$handle, timeout_ms =
                                round(.seconds(timeout, "timeout") * 1000))
    if (!is.null(stall))
        curl::handle_setopt(request$handle, low_speed_limit = 1L,
                            low_speed_time = ceiling(.seconds(stall, "stall")))
    request
}

.seconds <- function(x, arg) {
    if (!is.numeric(x) || length(x) != 1L || is.na(x) || x <= 0)
        stop(sprintf("`%s` must be a positive number of seconds.", arg),
             call. = FALSE)
    x
}

.proxyMethod <- function(method) {
    if (!is.character(method) || length(method) != 1L || is.na(method) ||
        !grepl("^[A-Za-z]+$", method))
        stop("`method` must be an HTTP method.", call. = FALSE)
    toupper(method)
}

.checkRawBody <- function(body) {
    if (!is.null(body) && !is.raw(body))
        stop("`body` must be a raw vector or NULL.", call. = FALSE)
    invisible(NULL)
}

## The pod server needs a Content-Length on every request that may carry a
## body; it answers 411 to a chunked one.
.proxyHeaders <- function(token, body, method) {
    headers <- character()
    if (length(body) || method %in% c("POST", "PUT", "PATCH"))
        headers <- c(headers, "Content-Length" = format(length(body)))
    if (!is.null(token)) {
        if (!is.character(token) || length(token) != 1L || is.na(token) ||
            !nzchar(token))
            stop("`token` must be a string.", call. = FALSE)
        headers <- c(headers, "X-BiocKube-Token" = token)
    }
    headers
}

#' Conditions for failed pod proxy requests
#'
#' @param status HTTP status, or `NA` when the request never completed.
#' @param message Message from the pod or the API server.
#' @param namespace,pod The pod the request was sent to.
#' @param method,path Method and path of the request.
#' @param reason Kubernetes reason, when the API server gave one.
#' @param classes Classes of the condition this one is built from.
#' @return A condition of class `biockube_pod_error`.
#' @noRd
.podError <- function(status, message, namespace, pod,
                      method = NA_character_, path = NA_character_,
                      reason = NA_character_, classes = character()) {
    status <- as.integer(status)
    kind <- unname(.podErrorKinds[as.character(status)])
    structure(
        class = unique(c(
            if (!is.na(kind)) paste0("biockube_error_", kind),
            "biockube_pod_error",
            setdiff(classes, c("error", "condition")), "error", "condition"
        )),
        list(message = sprintf("Pod %s/%s: %s", namespace, pod, message),
             call = NULL, status = status, reason = reason,
             namespace = namespace, pod = pod, method = method, path = path)
    )
}

.proxyError <- function(e, namespace, pod) {
    if (inherits(e, "kube_error"))
        return(.podError(e$status, e$message, namespace, pod, e$method,
                         e$path, e$reason, class(e)))
    .podError(NA_integer_, conditionMessage(e), namespace, pod,
              classes = class(e))
}

#' Condition for a response collected outside `.proxyRequest()`
#'
#' @param response A response as [curl::multi_add()] passes it to `done`.
#' @param namespace,pod The pod the request was sent to.
#' @return A condition of class `biockube_pod_error`, or `NULL` when the
#'   response succeeded.
#' @noRd
.podResponseError <- function(response, namespace, pod) {
    status <- response$status_code
    if (is.null(status) || status < 400L)
        return(NULL)
    .podError(status, .responseText(response, status), namespace, pod)
}

## Pod error bodies are one line of text; a Status from the API server is
## JSON, which is shown as it came.
.responseText <- function(response, status) {
    text <- tryCatch(
        iconv(rawToChar(response$content), "UTF-8", "UTF-8", sub = "?"),
        error = function(e) NA_character_
    )
    text <- trimws(sub("\n.*", "", text))
    if (is.na(text) || !nzchar(text))
        return(sprintf("the request failed with status %d", status))
    if (nchar(text) > 200L)
        text <- paste0(substr(text, 1L, 200L), "...")
    text
}

#' A curl pool for long polling one endpoint per pod
#'
#' @param size Number of requests the pool keeps outstanding. All of them go
#'   to the API server, and curl's default of six connections per host would
#'   queue the rest.
#' @return A curl pool.
#' @noRd
.newPool <- function(size) {
    size <- max(as.integer(size), 1L)
    curl::new_pool(total_con = size, host_con = size)
}

#' Run a pool until one of its requests completes
#'
#' @param pool A pool from `.newPool()`.
#' @param timeout Seconds to wait for a request to complete.
#' @return The list [curl::multi_run()] returns, with the numbers of
#'   completed, failed and pending requests.
#' @noRd
.poll <- function(pool, timeout = 1) {
    curl::multi_run(timeout = timeout, poll = TRUE, pool = pool)
}

#' State of a pod
#'
#' @param client A `kube_client`.
#' @param namespace,pod The pod to read.
#' @param timeout Seconds to wait for the API server.
#' @return A list with `exists`, `phase`, `ready`, `restarts`, `failed`,
#'   `reason`, `message` and `scheduling`. `failed` is `TRUE` when the pod
#'   cannot become ready again, because a container ended with a non-zero
#'   status or because it is waiting on something that will not resolve,
#'   such as an image that cannot be pulled, and also while a failed pull
#'   is retried, which `.pullRetried()` tells apart. `scheduling` is the
#'   scheduler's reason and message while the pod is not placed on a node,
#'   and `NA` otherwise: a cluster that adds nodes on demand places it once
#'   the node is there.
#' @noRd
.podStatus <- function(client, namespace, pod, timeout = .requestTimeout) {
    object <- tryCatch(
        kube::kube_request(
            client, "GET", paste0("/api/v1/namespaces/",
                                  .pathSegment(namespace, "namespace"),
                                  "/pods/", .pathSegment(pod, "pod")),
            timeout = timeout
        ),
        kube_error_notfound = function(e) NULL
    )
    if (is.null(object))
        return(list(exists = FALSE, phase = NA_character_, ready = FALSE,
                    restarts = 0L, failed = TRUE, reason = "NotFound",
                    message = "the pod does not exist",
                    scheduling = NA_character_))
    .podState(object)
}

.podState <- function(object) {
    status <- object$status %||% list()
    containers <- c(status$containerStatuses %||% list(),
                    status$initContainerStatuses %||% list())
    failure <- .podFailure(status, containers)
    list(
        exists = TRUE,
        phase = status$phase %||% NA_character_,
        ready = identical(.podCondition(status, "Ready")$status, "True"),
        restarts = sum(vapply(containers, function(x)
            as.integer(x$restartCount %||% 0L), integer(1L))),
        failed = !is.null(failure),
        reason = failure$reason %||% NA_character_,
        message = failure$message %||% NA_character_,
        scheduling = .podScheduling(status)
    )
}

.podCondition <- function(status, type) {
    for (condition in status$conditions %||% list())
        if (identical(condition$type, type))
            return(condition)
    NULL
}

## Waiting states that mark a pod as failed. Of these, the kubelet retries
## a failed pull, which succeeds once a registry that timed out or limited
## the rate answers again, but not when it said the image does not exist or
## may not be read.
.podStuckReasons <- c(
    "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull",
    "InvalidImageName", "ImageInspectError", "CreateContainerConfigError",
    "CreateContainerError", "RunContainerError"
)
.pullReasons <- c("ErrImagePull", "ImagePullBackOff")
.pullRefused <- "not ?found|manifest unknown|denied|unauthori[sz]ed|forbidden"

.pullRetried <- function(pod) {
    isTRUE(pod$failed) && isTRUE(pod$reason %in% .pullReasons) &&
        !isTRUE(grepl(.pullRefused, pod$message, ignore.case = TRUE))
}

.podFailure <- function(status, containers) {
    for (container in containers) {
        terminated <- container$state$terminated
        code <- as.numeric(terminated$exitCode %||% 0)
        if (!is.null(terminated) && !identical(code, 0))
            return(list(
                reason = terminated$reason %||% "Error",
                message = terminated$message %||%
                    sprintf("container %s exited with status %s",
                            container$name %||% "?", format(code))
            ))
        waiting <- container$state$waiting
        if ((waiting$reason %||% "") %in% .podStuckReasons)
            return(list(reason = waiting$reason,
                        message = waiting$message %||% NA_character_))
    }
    if (identical(status$phase, "Failed"))
        return(list(reason = status$reason %||% "Failed",
                    message = status$message %||% NA_character_))
    NULL
}

.podScheduling <- function(status) {
    scheduled <- .podCondition(status, "PodScheduled")
    if (!identical(scheduled$status, "False"))
        return(NA_character_)
    reason <- scheduled$reason %||% "NotScheduled"
    if (is.null(scheduled$message)) reason
    else paste0(reason, ": ", scheduled$message)
}

## A pod that has exited, failed or been removed does not answer again.
.podEnded <- function(state) {
    !state$exists || state$failed || identical(state$phase, "Succeeded")
}
