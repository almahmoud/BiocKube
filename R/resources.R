## The cluster side of a session: the ConfigMap holding the pod scripts, the
## Secret holding its token, the pods that run them, and their removal. The
## ConfigMap owns the Secret and the pods, so deleting it removes the
## session.

## Labels every object of a session carries, so that what a session left
## behind can be found and removed by hand.
.managedBy <- c("app.kubernetes.io/managed-by" = "BiocKube")
.sessionLabel <- "biockube.bioconductor.org/session"

## Seconds bpstop() spends on the pods and the API server, so that R exits
## promptly when it stops a session over a connection that has gone.
.stopTimeout <- 20

.now <- function() {
    as.numeric(Sys.time())
}

#' A session token
#'
#' @param bytes Number of random bytes.
#' @return The bytes as a hexadecimal string. The bytes come from a
#'   cryptographic source, never from R's random number generator, which is
#'   seeded and reproducible.
#' @noRd
.newToken <- function(bytes = 32L) {
    random <- if (requireNamespace("openssl", quietly = TRUE))
        openssl::rand_bytes(bytes)
    else .urandom(bytes)
    paste(random, collapse = "")
}

.urandom <- function(bytes) {
    if (!file.exists("/dev/urandom"))
        stop("Install the openssl package to create session tokens.",
             call. = FALSE)
    con <- file("/dev/urandom", "rb")
    on.exit(close(con))
    random <- readBin(con, "raw", bytes)
    if (length(random) != bytes)
        stop("Could not read ", bytes, " bytes from /dev/urandom.",
             call. = FALSE)
    random
}

#' The pod scripts, as ConfigMap data
#'
#' @param scripts Names of the scripts to read. `server.R` is sourced by
#'   both `worker.R` and `runner.R`, so it belongs in every ConfigMap.
#' @return A named list with the text of each script.
#' @noRd
.podScripts <- function(scripts = c("server.R", "worker.R", "runner.R")) {
    data <- lapply(scripts, function(name) {
        path <- system.file("pod", name, package = "BiocKube")
        if (!nzchar(path))
            stop(sprintf("BiocKube has no pod script \"%s\".", name),
                 call. = FALSE)
        paste0(paste(readLines(path, encoding = "UTF-8"), collapse = "\n"),
               "\n")
    })
    names(data) <- scripts
    data
}

#' Create the objects a session's pods read
#'
#' @param client A `kube_client`.
#' @param namespace Namespace to create the objects in.
#' @param name Name shared by the ConfigMap and the Secret.
#' @param scripts Pod scripts, from `.podScripts()`.
#' @param token Session token, from `.newToken()`.
#' @param labels Named strings to label both objects with.
#' @return A list with the `namespace`, the `configMap` and `secret` names
#'   and the `uid` of the ConfigMap, which owns everything else in the
#'   session. When the rest of the session cannot be created, the
#'   ConfigMap is deleted again before the error is raised.
#' @noRd
.createSession <- function(client, namespace, name, scripts, token,
                           labels = list()) {
    .dnsName(name, "name")
    labels <- .namedStrings(labels, "labels")
    created <- .create(client, list(
        apiVersion = "v1", kind = "ConfigMap",
        metadata = .compact(list(name = name, namespace = namespace,
                                 labels = labels)),
        data = scripts
    ))
    complete <- FALSE
    on.exit(if (!complete)
        .deleteObject(client, "configmaps", name, namespace, .stopTimeout))
    uid <- created$metadata$uid
    if (!is.character(uid) || length(uid) != 1L)
        stop(sprintf("The API server returned no uid for ConfigMap \"%s\".",
                     name), call. = FALSE)
    session <- list(namespace = namespace, configMap = name, secret = name,
                    uid = uid)
    .create(client, list(
        apiVersion = "v1", kind = "Secret",
        metadata = .compact(list(
            name = name, namespace = namespace, labels = labels,
            ownerReferences = list(.sessionOwner(session))
        )),
        type = "Opaque",
        stringData = list(token = token)
    ))
    complete <- TRUE
    session
}

#' An ownerReferences entry
#'
#' @param kind,name,uid The owning object.
#' @param apiVersion API version of the owner.
#' @return A list for `metadata$ownerReferences`. The owner is not a
#'   controller, and `blockOwnerDeletion` stays off because it needs
#'   permission on the owner's `finalizers` subresource.
#' @noRd
.ownerReference <- function(kind, name, uid, apiVersion = "v1") {
    list(apiVersion = apiVersion, kind = kind, name = name, uid = uid,
         controller = FALSE, blockOwnerDeletion = FALSE)
}

.sessionOwner <- function(session) {
    .ownerReference("ConfigMap", session$configMap, session$uid)
}

#' Manifest of one worker pod
#'
#' @param name Name of the pod.
#' @param namespace Namespace to create it in.
#' @param image Container image.
#' @param session The session from `.createSession()`, which holds the
#'   scripts, the token and the ownership of the pod.
#' @param port Port the script listens on.
#' @param cpu,memory Resource request and limit for the container. A number
#'   of cores, or of gibibytes for `memory`, or a Kubernetes quantity such
#'   as `"500m"` or `"4Gi"`. `NULL` leaves the field out.
#' @param packages Packages the pod installs before it serves requests.
#' @param env Named strings added to the container's environment. Names
#'   starting with `BIOCKUBE_` belong to the protocol and are refused.
#' @param idle Seconds without a request after which the pod exits.
#' @param labels Named strings to label the pod with.
#' @return The pod manifest, as a list.
#' @noRd
.podManifest <- function(name, namespace, image, session, port = 8080L,
                         cpu = NULL, memory = NULL, packages = character(),
                         env = character(), idle = 3600L, labels = list()) {
    .dnsName(name, "name")
    if (!is.character(image) || length(image) != 1L || is.na(image) ||
        !nzchar(image))
        stop("`image` must be a container image.", call. = FALSE)
    list(
        apiVersion = "v1", kind = "Pod",
        metadata = .compact(list(
            name = name, namespace = namespace,
            labels = .namedStrings(labels, "labels"),
            ownerReferences = list(.sessionOwner(session))
        )),
        spec = list(
            restartPolicy = "Never",
            automountServiceAccountToken = FALSE,
            enableServiceLinks = FALSE,
            containers = list(.compact(list(
                name = "worker",
                image = image,
                command = I(c("Rscript", "/biockube/worker.R")),
                env = .podEnv(session, port, idle, packages, env),
                ports = list(list(name = "biockube",
                                  containerPort = as.integer(port),
                                  protocol = "TCP")),
                resources = .podResources(cpu, memory),
                readinessProbe = list(
                    httpGet = list(path = "/healthz",
                                   port = as.integer(port)),
                    periodSeconds = 2L
                ),
                volumeMounts = list(list(name = "scripts",
                                         mountPath = "/biockube",
                                         readOnly = TRUE)),
                securityContext = .podSecurityContext
            ))),
            volumes = list(list(
                name = "scripts",
                configMap = list(name = session$configMap)
            ))
        )
    )
}

## The Bioconductor images run as root and install packages, so only
## privileges the pod never needs are dropped.
.podSecurityContext <- list(
    allowPrivilegeEscalation = FALSE,
    capabilities = list(drop = I("ALL")),
    seccompProfile = list(type = "RuntimeDefault")
)

.podEnv <- function(session, port, idle, packages, env) {
    env <- .envVariables(env)
    values <- c(
        list(BIOCKUBE_PORT = format(as.integer(port)),
             BIOCKUBE_IDLE = .number(idle, "idle")),
        if (length(packages))
            list(BIOCKUBE_PACKAGES = paste(packages, collapse = ",")),
        env
    )
    entries <- lapply(names(values), function(name)
        list(name = name, value = values[[name]]))
    c(list(list(
        name = "BIOCKUBE_TOKEN",
        valueFrom = list(secretKeyRef = list(name = session$secret,
                                             key = "token"))
    )), entries)
}

#' Check environment variables given for the pods
#'
#' @param env Named strings.
#' @return `env` as a named list, or `NULL` when it is empty. Names that
#'   are not environment variable names, or that start with `BIOCKUBE_`,
#'   which the pod scripts read, are an error.
#' @noRd
.envVariables <- function(env) {
    env <- .namedStrings(env, "env")
    reserved <- grep("^BIOCKUBE_", names(env), value = TRUE)
    if (length(reserved))
        stop(sprintf("`env` must not set %s.",
                     paste(reserved, collapse = ", ")), call. = FALSE)
    if (!all(.isEnvName(names(env))))
        stop("`env` names must be environment variable names.",
             call. = FALSE)
    env
}

.isEnvName <- function(x) {
    grepl("^[A-Za-z_][A-Za-z0-9_]*$", x)
}

## Requests and limits are the same, which gives the pod a guaranteed share
## of its node.
.podResources <- function(cpu, memory) {
    quantities <- .compact(list(cpu = .quantity(cpu, "cpu"),
                                memory = .quantity(memory, "memory", "Gi")))
    if (!length(quantities))
        return(NULL)
    list(requests = quantities, limits = quantities)
}

.quantity <- function(x, arg, unit = "") {
    if (is.null(x))
        return(NULL)
    if (is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x))
        return(x)
    if (is.numeric(x) && length(x) == 1L && !is.na(x) && x > 0)
        return(paste0(format(x, scientific = FALSE, trim = TRUE), unit))
    stop(sprintf("`%s` must be a positive number or a quantity such as %s.",
                 arg, if (nzchar(unit)) "\"4Gi\"" else "\"500m\""),
         call. = FALSE)
}

#' Wait for pods to be ready
#'
#' Fails as soon as a pod cannot become ready, rather than waiting for
#' `timeout`, and reports why, with the last lines of the pod's log when the
#' container ran at all. A pod the scheduler has not placed is waited for,
#' as a cluster that adds nodes on demand places it once the node is there,
#' and so is a pod whose image pull failed for a reason other than the
#' registry refusing the image, which the kubelet retries; when `timeout`
#' runs out, the scheduler's or the pull's last message is reported.
#'
#' @param client A `kube_client`.
#' @param namespace Namespace the pods are in.
#' @param names Names of the pods to wait for.
#' @param timeout Seconds to wait.
#' @param selector Label selector narrowing the pods watched, as the session
#'   label does.
#' @return The names, invisibly.
#' @noRd
.waitReady <- function(client, namespace, names, timeout = 600,
                       selector = NULL) {
    if (!length(names))
        return(invisible(names))
    if (!is.numeric(timeout) || length(timeout) != 1L || is.na(timeout) ||
        timeout <= 0)
        stop("`timeout` must be a positive number of seconds.", call. = FALSE)
    state <- new.env(parent = emptyenv())
    state$ready <- character()
    state$failed <- NULL
    state$scheduling <- list()
    state$pulling <- list()
    kube::kube_watch("pods", .readyCallback(state, names),
                     namespace = namespace, selector = selector,
                     timeout = timeout, client = client)
    if (!is.null(state$failed))
        stop(.failedPodMessage(client, state$failed, namespace),
             call. = FALSE)
    missing <- setdiff(names, state$ready)
    if (length(missing)) {
        unplaced <- intersect(missing, names(state$scheduling))
        pulling <- intersect(missing, names(state$pulling))
        stop(paste(c(
            sprintf("Pods %s were not ready within %s seconds.",
                    paste(missing, collapse = ", "), format(timeout)),
            sprintf("Pod %s/%s is not scheduled: %s", namespace, unplaced,
                    unlist(state$scheduling[unplaced], use.names = FALSE)),
            sprintf("Pod %s/%s could not pull its image: %s", namespace,
                    pulling,
                    unlist(state$pulling[pulling], use.names = FALSE))
        ), collapse = "\n"), call. = FALSE)
    }
    invisible(names)
}

.readyCallback <- function(state, names) {
    function(type, object) {
        name <- object$metadata$name %||% ""
        if (!name %in% names)
            return(TRUE)
        pod <- .podState(object)
        retried <- .pullRetried(pod)
        failure <- if (identical(type, "DELETED"))
            list(reason = "Deleted", message = "the pod was deleted")
        else if (pod$failed && !retried)
            list(reason = pod$reason, message = pod$message)
        else if (identical(pod$phase, "Succeeded"))
            list(reason = "Completed",
                 message = "the pod exited before it was ready")
        if (!is.null(failure)) {
            state$failed <- c(failure, list(object = object))
            return(FALSE)
        }
        state$scheduling[[name]] <- if (!is.na(pod$scheduling))
            pod$scheduling
        state$pulling[[name]] <- if (retried)
            .pullMessage(pod, state$pulling[[name]])
        if (isTRUE(pod$ready))
            state$ready <- union(state$ready, name)
        !setequal(state$ready, names)
    }
}

## The back-off that follows a failed pull says less than the failure.
.pullMessage <- function(pod, last) {
    if (identical(pod$reason, "ImagePullBackOff") && !is.null(last))
        return(last)
    if (is.na(pod$message) || !nzchar(pod$message)) pod$reason
    else paste0(pod$reason, ": ", pod$message)
}

.failedPodMessage <- function(client, failed, namespace) {
    name <- failed$object$metadata$name %||% "?"
    lines <- sprintf("Pod %s/%s is not ready: %s.", namespace, name,
                     failed$reason)
    message <- failed$message %||% NA_character_
    if (!is.na(message) && nzchar(message))
        lines <- c(lines, message)
    log <- .podLogs(client, failed$object)
    if (!is.null(log))
        lines <- c(lines, "Last lines of its log:", log)
    paste(lines, collapse = "\n")
}

.podLogs <- function(client, object, lines = 20L) {
    text <- tryCatch(
        kube::kube_logs(object, tail = lines, client = client),
        error = function(e) NULL
    )
    if (!length(text))
        return(NULL)
    paste(as.character(text), collapse = "\n")
}

#' Remove a session
#'
#' Deletes the ConfigMap, which the pods and the Secret belong to, and the
#' Secret, which is not waited for. Objects already gone are not an error;
#' anything left behind is reported as a warning, because the pods stay
#' until their owner is gone.
#'
#' @param client A `kube_client`.
#' @param namespace,name The session.
#' @param timeout Seconds the two requests may take together, give or take
#'   one: a session is also removed while R exits.
#' @return `TRUE` when nothing was left behind, invisibly.
#' @noRd
.deleteSession <- function(client, namespace, name, timeout = .stopTimeout) {
    deadline <- .now() + timeout
    deleted <- vapply(c("configmaps", "secrets"), function(type)
        .deleteObject(client, type, name, namespace,
                      max(deadline - .now(), 1)), logical(1L))
    invisible(all(deleted))
}

.deleteObject <- function(client, type, name, namespace, timeout) {
    path <- paste0("/api/v1/namespaces/", .pathSegment(namespace, "namespace"),
                   "/", type, "/", .pathSegment(name, "name"))
    tryCatch({
        kube::kube_request(client, "DELETE", path,
                           query = list(propagationPolicy = "Background"),
                           timeout = timeout)
        TRUE
    },
    kube_error_notfound = function(e) TRUE,
    error = function(e) {
        warning(sprintf("Could not delete %s \"%s\" in namespace \"%s\": %s",
                        type, name, namespace, conditionMessage(e)),
                call. = FALSE)
        FALSE
    })
}

.create <- function(client, manifest) {
    suppressMessages(kube::kube_create(manifest, client = client))
}

.compact <- function(x) {
    x[lengths(x) > 0L]
}

.number <- function(x, arg) {
    if (!is.numeric(x) || length(x) != 1L || is.na(x) || x <= 0)
        stop(sprintf("`%s` must be a positive number of seconds.", arg),
             call. = FALSE)
    format(x, scientific = FALSE, trim = TRUE)
}

.namedStrings <- function(x, arg) {
    if (!length(x))
        return(NULL)
    x <- unlist(x)
    if (!is.character(x) || anyNA(x) || is.null(names(x)) ||
        !all(nzchar(names(x))) || anyDuplicated(names(x)))
        stop(sprintf("`%s` must be uniquely named strings.", arg),
             call. = FALSE)
    as.list(x)
}

.dnsName <- function(x, arg) {
    if (!is.character(x) || length(x) != 1L || is.na(x) || nchar(x) > 63L ||
        !grepl("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", x))
        stop(sprintf("`%s` must be a name of at most 63 lower case ", arg),
             "letters, digits and dashes.", call. = FALSE)
    invisible(x)
}
