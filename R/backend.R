## The manager side of the worker protocol. A task is posted to a pod
## through the API server's pod proxy and its result is collected by long
## polling, which is what a socket backend gets from select(). Nothing here
## throws when a pod misbehaves: every task that was sent is answered, with
## the worker's result or with one error per element of the task, so that
## the reducer stays aligned with the elements it is reducing.

.workerPort <- 8080L

## Seconds a worker holds a result poll open. Pod proxy requests are long
## running, so the API server does not time them out, but load balancers in
## front of managed clusters drop idle connections after a minute or two.
.pollWait <- 25

## Seconds between two passes over the pool, short enough for an interrupt
## or a time limit to be noticed.
.pollSlice <- 1

## Seconds a pod may go on failing to answer while the API server reports it
## as running before the task it holds counts as lost, and seconds a pod
## that answers may go on failing to take a task or to hand over a result
## before the task fails. Failing requests are retried after a delay that
## doubles up to `.maxBackoff` seconds.
.troubleWindow <- 120
.maxBackoff <- 10

#' The backend of a running KubeParam
#'
#' @param client A `kube_client`, or `NULL` for the default client.
#' @param namespace Namespace the pods run in.
#' @param session The session from `.createSession()`, which owns the pods
#'   and holds the names `bpstop()` deletes.
#' @param pods Names of the worker pods, one per node.
#' @param port Port the workers listen on.
#' @param token Session token the workers authenticate with.
#' @param idle Seconds without a request after which a pod exits, or `NA`
#'   when unknown.
#' @return An environment of class `KubeBackend`.
#' @noRd
.kubeBackend <- function(client, namespace, session, pods, port, token,
                         idle = NA_real_) {
    n <- length(pods)
    backend <- new.env(parent = emptyenv())
    backend$client <- client
    backend$namespace <- namespace
    backend$session <- session
    backend$pods <- pods
    backend$port <- port
    backend$token <- token
    backend$idle <- as.numeric(idle)
    backend$lost <- logical(n)
    backend$ended <- logical(n)
    backend$cause <- vector("list", n)
    backend$why <- rep(NA_character_, n)
    backend$polls <- vector("list", n)
    .clearRun(backend, NA_character_)
    ## One poll and one other request per pod, all to the API server; curl
    ## would otherwise queue them behind its per-host limit.
    backend$pool <- .newPool(2L * n)
    structure(backend, class = "KubeBackend")
}

.clearRun <- function(backend, run) {
    n <- length(backend$pods)
    backend$run <- run
    backend$static <- vector("list", n)
    backend$inflight <- vector("list", n)
    backend$trouble <- vector("list", n)
    backend$acks <- logical(n)
    backend$failures <- integer(n)
    backend$troubleSince <- rep(NA_real_, n)
    backend$retryAt <- numeric(n)
    backend$inbox <- list()
    backend$aliveAt <- .now() + backend$idle / 4
    invisible(backend)
}

#' Begin a run
#'
#' Tags start again at 1 on every `bplapply()` call, so a result of an
#' earlier run would be taken for a result of this one. Each run has an id
#' of its own, which is sent with every task and checked on every result,
#' and a pod holds the static part of a task per run, so a new run also
#' clears what the pods cached.
#'
#' @param backend A `KubeBackend`.
#' @return The new run id, invisibly.
#' @noRd
.startRun <- function(backend) {
    .cancelPolls(backend)
    stale <- .inflightNodes(backend)
    .clearRun(backend, .newToken(4L))
    .checkPods(backend, stale)
    invisible(backend$run)
}

#' Check the pods at the start of a run
#'
#' Every pod that may still answer is sent a request, which also restarts
#' its idle timeout: a pod that held a task of an interrupted run is told
#' to cancel it, the others are asked for their state. A lost pod that
#' answers again takes tasks again, and a pod that does not answer is lost
#' once the API server reports it has ended.
#'
#' @param backend A `KubeBackend`.
#' @param stale Which pods held a task of the run before.
#' @return `NULL`, invisibly.
#' @noRd
.checkPods <- function(backend, stale) {
    nodes <- which(!backend$ended)
    answers <- vector("list", length(backend$pods))
    record <- function(node) {
        force(node)
        list(done = function(response) answers[[node]] <<- response,
             fail = function(message) answers[[node]] <<- simpleError(message))
    }
    pool <- .newPool(length(nodes))
    for (node in nodes) {
        callbacks <- record(node)
        .quietly(.addRequest(
            backend, node, pool, if (stale[node]) "cancel" else "status",
            method = if (stale[node]) "POST" else "GET",
            timeout = .requestTimeout, done = callbacks$done,
            fail = callbacks$fail
        ))
    }
    .drain(pool, .requestTimeout)
    for (node in nodes) {
        answer <- answers[[node]]
        if (!inherits(answer, "error") && !is.null(answer) &&
            answer$status_code < 300L) {
            backend$lost[node] <- FALSE
            next
        }
        state <- tryCatch(.podStatus(backend$client, backend$namespace,
                                     backend$pods[node]),
                          error = function(e) NULL)
        if (!is.null(state) && .podEnded(state))
            .markLost(backend, node, .answerError(backend, node, answer),
                      .podTrouble(state, backend$idle), ended = TRUE)
    }
    invisible(NULL)
}

.answerError <- function(backend, node, answer) {
    if (inherits(answer, "error"))
        return(answer)
    if (is.null(answer))
        return(simpleError("the pod did not answer"))
    .podResponseError(answer, backend$namespace, backend$pods[node])
}

#' Report the pods a run starts without
#'
#' @param backend A `KubeBackend`.
#' @param before Which pods were lost before the run was checked.
#' @return `NULL`, invisibly, with a warning naming the pods found ended
#'   now. A run that has no pod left is an error.
#' @noRd
.reportLost <- function(backend, before) {
    names <- sprintf("%s/%s", backend$namespace, backend$pods)
    if (all(backend$lost))
        stop(paste(c(
            paste("No worker pod of this KubeParam is left; restart them",
                  "with bpstop() and bpstart()."),
            sprintf("  %s: %s", names, backend$why)
        ), collapse = "\n"), call. = FALSE)
    new <- which(backend$lost & !before)
    if (length(new))
        warning(sprintf(paste(
            "%s lost: %s. This run uses the other %d; bpstop() and",
            "bpstart() start them all again."
        ), paste(ngettext(length(new), "Worker pod", "Worker pods"),
                 paste(names[new], collapse = ", "),
                 ngettext(length(new), "is", "are")),
        paste(unique(backend$why[new]), collapse = "; "),
        sum(!backend$lost)), call. = FALSE)
    invisible(NULL)
}

## The manager sends a task to a pod that holds none and is not lost. With
## every pod lost, it still takes tasks, which are answered at once, so that
## the run ends with an error for each element.
setOldClass("KubeManager")

.kubeManager <- function(BPPARAM, backend) {
    manager <- new.env(parent = emptyenv())
    manager$BPPARAM <- BPPARAM
    manager$backend <- backend
    structure(manager, class = "KubeManager")
}

setMethod(".manager_capacity", "KubeManager", function(manager) {
    max(sum(!manager$backend$lost), 1L)
})

setMethod(".manager_send", "KubeManager", function(manager, value, ...) {
    backend <- manager$backend
    .keepAlive(backend, wait = TRUE)
    free <- which(!backend$lost & !.inflightNodes(backend))
    .send_to(backend, if (length(free)) free[1L] else 1L, value)
    manager
})

setMethod(".manager_recv", "KubeManager", function(manager) {
    list(.recv_any(manager$backend))
})

setMethod(".send_to", "KubeBackend", function(backend, node, value) {
    node <- as.integer(node)
    if (identical(value$type, "DONE")) {
        .quietly(.podRequest(backend, node, "done", method = "POST",
                             timeout = .requestTimeout))
        return(TRUE)
    }
    if (!identical(value$type, "EXEC"))
        stop("A KubeParam worker takes EXEC and DONE requests only.",
             call. = FALSE)
    .sendTask(backend, node, value)
    TRUE
})

setMethod(".recv_any", "KubeBackend", function(backend) {
    repeat {
        .acknowledge(backend)
        if (length(backend$inbox))
            return(.takeResult(backend))
        if (!any(.inflightNodes(backend)))
            stop("No task is running on the worker pods.", call. = FALSE)
        .armPolls(backend)
        .keepAlive(backend)
        .wait(backend)
        .resolveTrouble(backend)
    }
})

## DONE reaches every pod at once; each one answers and then exits. When
## bpstop() sends it, half the time bpstop() has is left for the API server.
setMethod(".send_all", "KubeBackend", function(backend, value) {
    if (!identical(value$type, "DONE"))
        return(callNextMethod())
    pool <- .newPool(length(backend$pods))
    for (node in which(!backend$lost))
        .quietly(.addRequest(backend, node, pool, "done", method = "POST",
                             timeout = .requestTimeout))
    timeout <- if (is.null(backend$deadline)) .requestTimeout
        else max((backend$deadline - .now()) / 2, 0.1)
    .drain(pool, timeout)
})

## One result per task still in flight, rather than one per pod: a pod that
## was not given a task has nothing to say.
setMethod(".recv_all", "KubeBackend", function(backend) {
    results <- list()
    while (length(backend$inbox) || any(.inflightNodes(backend)))
        results <- c(results, list(.recv_any(backend)))
    results
})

#' Post one task to a pod
#'
#' The static part of a task, the function and the arguments that do not
#' change between the tasks of a run, is sent once per pod and per run; the
#' pod keeps it and rebuilds the following tasks from it.
#'
#' The task stays in flight, with the message, until it is answered; `sent`
#' tells whether the pod is known to hold it.
#'
#' @param backend A `KubeBackend`.
#' @param node Index of the pod.
#' @param value An `EXEC` message.
#' @return `NULL`, invisibly. A task that cannot be sent is answered from
#'   the inbox instead, like one whose pod is lost.
#' @noRd
.sendTask <- function(backend, node, value) {
    backend$inflight[[node]] <- list(tag = value$data$tag,
                                     size = length(value$data$args$X),
                                     run = backend$run, value = value,
                                     sent = FALSE, error = NULL)
    if (backend$lost[node])
        return(.failTask(backend, node, backend$cause[[node]],
                         if (all(backend$lost)) paste(
                             "no worker pod is left; restart them with",
                             "bpstop() and bpstart()"
                         ) else "no further task is sent to a lost pod"))
    static <- .task_const(value)
    if (!is.null(static) && identical(static, backend$static[[node]])) {
        error <- .tryPost(backend, node, .task_dynamic(value))
        ## A 422 says the pod kept no static part for this run after all.
        if (!inherits(error, "biockube_error_unprocessable"))
            return(.posted(backend, node, error))
    }
    backend$static[node] <- list(NULL)
    error <- .tryPost(backend, node, value)
    if (is.null(error))
        backend$static[node] <- list(static)
    .posted(backend, node, error)
}

.tryPost <- function(backend, node, value) {
    tryCatch({
        body <- serialize(list(run = backend$run,
                               tag = backend$inflight[[node]]$tag,
                               msg = value), NULL, xdr = FALSE)
        .podRequest(backend, node, "task", method = "POST", body = body)
        NULL
    }, error = function(e) e)
}

.posted <- function(backend, node, error) {
    if (is.null(error))
        return(.sent(backend, node))
    backend$inflight[[node]]$error <- error
    .deliver(backend, node)
}

.sent <- function(backend, node) {
    backend$inflight[[node]]$sent <- TRUE
    backend$inflight[[node]]$error <- NULL
    invisible(NULL)
}

#' Deliver a task the pod is not known to hold
#'
#' A post that failed may have reached the pod although its answer was lost
#' on the way back, so the pod is asked for its state first. A pod that
#' does not hold the task is sent it again, after cancelling what it is
#' still busy with. A pod that cannot be reached is tried again as long as
#' `.unreachable()` allows, and a post that fails on the way as long as
#' `.troubleWindow` allows; a post the pod refuses fails the task.
#'
#' @inheritParams .sendTask
#' @return `NULL`, invisibly.
#' @noRd
.deliver <- function(backend, node) {
    task <- backend$inflight[[node]]
    status <- .askPod(backend, node)
    if (inherits(status, "error"))
        return(.unreachable(backend, node, task$error, status))
    if (.holdsTask(backend, node, status))
        return(.sent(backend, node))
    if (identical(status$state, "busy"))
        .quietly(.podRequest(backend, node, "cancel", method = "POST",
                             timeout = .requestTimeout))
    error <- .tryPost(backend, node, task$value)
    if (is.null(error)) {
        backend$static[node] <- list(.task_const(task$value))
        return(.sent(backend, node))
    }
    backend$inflight[[node]]$error <- error
    if (.refused(error))
        return(.failTask(backend, node, error, "the task could not be sent"))
    .persist(backend, node, error, "the task could not be sent")
}

## The pod answered the post itself, and would answer it the same way again.
.refused <- function(error) {
    status <- error$status
    isTRUE(status >= 400L && status < 500L) &&
        !status %in% c(408L, 409L, 429L)
}

#' Retry an exchange with a pod that answers, for a while
#'
#' @inheritParams .lose
#' @return `NULL`, invisibly. The task fails once `.troubleWindow` seconds
#'   have gone by since the pod first failed to answer as expected.
#' @noRd
.persist <- function(backend, node, condition, reason) {
    if (.troubleTime(backend, node) >= .troubleWindow)
        return(.failTask(backend, node, condition, reason))
    .backOff(backend, node)
}

## Seconds since the pod first failed to answer as expected.
.troubleTime <- function(backend, node) {
    now <- .now()
    if (is.na(backend$troubleSince[node]))
        backend$troubleSince[node] <- now
    now - backend$troubleSince[node]
}

#' Keep one result poll outstanding per pod that owes a result
#'
#' @param backend A `KubeBackend`.
#' @return `NULL`, invisibly. A task the pod is not known to hold is
#'   delivered first. A pod waiting to be retried is left alone until its
#'   time comes.
#' @noRd
.armPolls <- function(backend) {
    for (node in which(.inflightNodes(backend) & !backend$lost)) {
        if (!is.null(backend$polls[[node]]) || backend$retryAt[node] > .now())
            next
        if (!backend$inflight[[node]]$sent)
            .deliver(backend, node)
        if (isTRUE(backend$inflight[[node]]$sent))
            .armPoll(backend, node)
    }
    invisible(NULL)
}

## The poll may take as long as its result needs to come through, but not
## a long wait without a byte.
.armPoll <- function(backend, node) {
    force(node)
    backend$polls[[node]] <- .addRequest(
        backend, node, backend$pool, "result",
        query = list(wait = .pollWait), stall = .pollWait + .requestTimeout,
        done = function(response) .polled(backend, node, response),
        fail = function(message) .pollFailed(backend, node, message)
    )
    invisible(NULL)
}

.cancelPolls <- function(backend) {
    for (node in seq_along(backend$polls))
        .cancelPoll(backend, node)
    invisible(NULL)
}

.cancelPoll <- function(backend, node) {
    if (!is.null(backend$polls[[node]]))
        .quietly(curl::multi_cancel(backend$polls[[node]]))
    backend$polls[node] <- list(NULL)
    invisible(NULL)
}

## Sends every pod a request, so that the idle timeout of a pod that has no
## task, or whose answered poll has not been read yet, does not run out
## during a long run. While tasks are sent, nothing runs the pool, so the
## requests are waited for.
.keepAlive <- function(backend, wait = FALSE) {
    if (is.na(backend$aliveAt) || .now() < backend$aliveAt)
        return(invisible(NULL))
    backend$aliveAt <- .now() + backend$idle / 4
    nodes <- which(!backend$lost)
    pool <- if (wait) .newPool(length(nodes)) else backend$pool
    for (node in nodes)
        .quietly(.addRequest(backend, node, pool, "status",
                             timeout = .requestTimeout))
    if (wait)
        .drain(pool, .requestTimeout)
    invisible(NULL)
}

## Runs the pool until a request completes, or waits for the first pod
## that is due to be retried when no request is outstanding.
.wait <- function(backend) {
    if (.poll(backend$pool, timeout = .pollSlice)$pending > 0L)
        return(invisible(NULL))
    waiting <- .inflightNodes(backend) & !backend$lost
    delay <- min(backend$retryAt[waiting] - .now(), .pollSlice)
    if (delay > 0)
        Sys.sleep(delay)
    invisible(NULL)
}

## Runs inside curl's callback, so it records what it sees and leaves the
## requests that follow to .recv_any().
.polled <- function(backend, node, response) {
    backend$polls[node] <- list(NULL)
    if (is.null(backend$inflight[[node]]))
        return(invisible(NULL))
    status <- response$status_code
    if (identical(status, 204L))
        return(.answering(backend, node))
    if (identical(status, 200L))
        return(.received(backend, node, response$content))
    backend$trouble[[node]] <- .podResponseError(response, backend$namespace,
                                                 backend$pods[node])
    invisible(NULL)
}

.pollFailed <- function(backend, node, message) {
    backend$polls[node] <- list(NULL)
    if (!is.null(backend$inflight[[node]]))
        backend$trouble[[node]] <- .podError(
            NA_integer_, message, backend$namespace, backend$pods[node]
        )
    invisible(NULL)
}

.answering <- function(backend, node) {
    backend$failures[node] <- 0L
    backend$troubleSince[node] <- NA_real_
    backend$retryAt[node] <- 0
    invisible(NULL)
}

## A result of another task, one whose acknowledgement was lost, says the
## pod does not hold this task: the stale result is acknowledged and the
## task sent again.
.received <- function(backend, node, content) {
    result <- tryCatch(unserialize(content), error = function(e) e)
    backend$acks[node] <- TRUE
    if (inherits(result, "error"))
        return(.failTask(backend, node, result,
                         "its result could not be read"))
    inflight <- backend$inflight[[node]]
    if (!identical(result$run, inflight$run) ||
        !identical(result$tag, inflight$tag))
        return(.resend(backend, node))
    .answering(backend, node)
    backend$inflight[node] <- list(NULL)
    backend$inbox <- c(backend$inbox,
                       list(list(node = node, value = result$value)))
    invisible(NULL)
}

.resend <- function(backend, node) {
    condition <- simpleError("it answered with another task's result")
    if (.troubleTime(backend, node) >= .troubleWindow)
        return(.lose(backend, node, condition,
                     "the pod lost track of the run"))
    backend$inflight[[node]]$sent <- FALSE
    backend$inflight[[node]]$error <- condition
    backend$retryAt[node] <- 0
    invisible(NULL)
}

#' Tell the pods the results they hold have been taken
#'
#' A worker keeps a result until it is acknowledged. The next task to the
#' same pod would acknowledge it, and would then be at risk of having its
#' own result dropped by a late acknowledgement, so this happens before the
#' result is handed to the caller.
#'
#' @param backend A `KubeBackend`.
#' @return `NULL`, invisibly.
#' @noRd
.acknowledge <- function(backend) {
    for (node in which(backend$acks)) {
        backend$acks[node] <- FALSE
        .quietly(.podRequest(backend, node, "result", method = "DELETE",
                             timeout = .requestTimeout))
    }
    invisible(NULL)
}

#' Decide what a pod that did not answer as expected is doing
#'
#' The pod is asked for its state with a request that, unlike a poll, gets
#' fresh credentials when the API server refuses the old ones. A pod that
#' still runs its task is polled again. A pod that has finished it is
#' polled again until `.troubleWindow` seconds have gone by without its
#' result coming through. When the pod cannot be reached, the API server is
#' asked about it: a pod that has ended loses its task at once, and one
#' reported as running is retried until `.troubleWindow` seconds have gone
#' by. Polls that keep failing are retried after a delay that grows with
#' each failure.
#'
#' @param backend A `KubeBackend`.
#' @return `NULL`, invisibly.
#' @noRd
.resolveTrouble <- function(backend) {
    for (node in which(!vapply(backend$trouble, is.null, logical(1L)))) {
        condition <- backend$trouble[[node]]
        backend$trouble[node] <- list(NULL)
        if (is.null(backend$inflight[[node]]))
            next
        status <- .askPod(backend, node)
        if (inherits(status, "error")) {
            .unreachable(backend, node, condition, status)
        } else if (identical(status$state, "done") &&
                   .holdsTask(backend, node, status)) {
            .persist(backend, node, condition,
                     "its result could not be fetched")
            ## The pod keeps a result until it is acknowledged.
            if (is.null(backend$inflight[[node]]))
                backend$acks[node] <- TRUE
        } else if (.holdsTask(backend, node, status)) {
            backend$troubleSince[node] <- NA_real_
            .backOff(backend, node)
        } else {
            .failTask(backend, node, condition,
                      "the pod does not hold the task")
        }
    }
    invisible(NULL)
}

.askPod <- function(backend, node) {
    tryCatch(.podRequest(backend, node, "status", as = "parsed",
                         timeout = .requestTimeout),
             error = function(e) e)
}

.holdsTask <- function(backend, node, status) {
    inflight <- backend$inflight[[node]]
    is.list(status) && isTRUE(status$state %in% c("busy", "done")) &&
        identical(status$run, inflight$run) &&
        isTRUE(as.numeric(status$tag) == as.numeric(inflight$tag))
}

.unreachable <- function(backend, node, condition, status) {
    if (isTRUE(status$status %in% c(401L, 403L)))
        return(.lose(backend, node, status, "the request was refused"))
    state <- tryCatch(.podStatus(backend$client, backend$namespace,
                                 backend$pods[node]),
                      error = function(e) NULL)
    if (!is.null(state) && .podEnded(state))
        return(.lose(backend, node, condition,
                     .podTrouble(state, backend$idle), ended = TRUE))
    ## Time without the API server counts too, but only its word that the
    ## pod runs and cannot be reached gives the task up.
    elapsed <- .troubleTime(backend, node)
    if (!is.null(state) && elapsed >= .troubleWindow)
        return(.lose(backend, node, condition, "the pod stopped answering"))
    .backOff(backend, node)
}

.backOff <- function(backend, node) {
    backend$failures[node] <- backend$failures[node] + 1L
    backend$retryAt[node] <- .now() +
        min(2^(backend$failures[node] - 1L), .maxBackoff)
    invisible(NULL)
}

.podTrouble <- function(state, idle = NA_real_) {
    if (!state$exists)
        return("the pod is gone")
    if (identical(state$phase, "Succeeded") && !isTRUE(state$failed))
        return(if (is.na(idle)) "the pod has exited" else sprintf(paste(
            "the pod has exited, as a worker does after %s seconds",
            "without a request (idleTimeout)"
        ), format(idle)))
    if (is.na(state$reason))
        "the pod failed"
    else paste0("the pod failed with ", state$reason)
}

#' Give up on a pod
#'
#' @param backend A `KubeBackend`.
#' @param node Index of the pod.
#' @param condition The condition that ended it.
#' @param reason What happened, as the first line of the error the caller
#'   sees.
#' @param ended Whether the API server reports the pod has ended. A pod
#'   lost for another reason is asked again when the next run starts.
#' @return `NULL`, invisibly.
#' @noRd
.lose <- function(backend, node, condition, reason, ended = FALSE) {
    .markLost(backend, node, condition, reason, ended)
    .failTask(backend, node, condition, reason)
}

.markLost <- function(backend, node, condition, reason, ended = FALSE) {
    backend$lost[node] <- TRUE
    backend$ended[node] <- ended
    backend$cause[[node]] <- condition
    backend$why[node] <- reason
    .cancelPoll(backend, node)
    invisible(NULL)
}

#' Answer a task its pod will not answer
#'
#' @inheritParams .lose
#' @return `NULL`, invisibly. The value carries one error per element of
#'   the task, as a worker's own value would, so that the reducer places
#'   the following results where they belong.
#' @noRd
.failTask <- function(backend, node, condition, reason) {
    inflight <- backend$inflight[[node]]
    backend$inflight[node] <- list(NULL)
    error <- .error_worker_comm(condition, sprintf(
        "worker pod %s/%s: %s", backend$namespace, backend$pods[node], reason
    ))
    value <- list(type = "VALUE", tag = inflight$tag,
                  value = rep(list(error), inflight$size), success = FALSE,
                  time = NA, log = NULL, sout = NULL)
    backend$inbox <- c(backend$inbox,
                       list(list(node = node, value = value)))
    invisible(NULL)
}

.takeResult <- function(backend) {
    result <- backend$inbox[[1L]]
    backend$inbox <- backend$inbox[-1L]
    result
}

.inflightNodes <- function(backend) {
    !vapply(backend$inflight, is.null, logical(1L))
}

.podRequest <- function(backend, node, path, ...) {
    .proxyRequest(backend$client, backend$namespace, backend$pods[node],
                  backend$port, path, token = backend$token, ...)
}

.addRequest <- function(backend, node, pool, path, ..., done = NULL,
                        fail = NULL) {
    request <- .proxyHandle(backend$client, backend$namespace,
                            backend$pods[node], backend$port, path,
                            token = backend$token, ...)
    curl::multi_add(request$handle, pool = pool,
                    done = done %||% function(response) NULL,
                    fail = fail %||% function(message) NULL)
    request$handle
}

#' Run a pool until every request in it has finished
#'
#' @param pool A pool from `.newPool()`.
#' @param timeout Seconds to wait.
#' @return `NULL`, invisibly.
#' @noRd
.drain <- function(pool, timeout) {
    deadline <- .now() + timeout
    repeat {
        left <- deadline - .now()
        if (left <= 0)
            return(invisible(NULL))
        if (.poll(pool, timeout = min(left, .pollSlice))$pending == 0L)
            return(invisible(NULL))
    }
}

## Best effort: a pod that cannot be reached is either about to be deleted
## or already answered for by the task it holds.
.quietly <- function(expr) {
    tryCatch(expr, error = function(e) NULL)
}
