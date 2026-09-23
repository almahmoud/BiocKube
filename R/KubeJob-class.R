#' Jobs running on a cluster
#'
#' A `KubeJob` refers to a job that [submitJob()]
#' started: the Kubernetes Job, its pod and the token the pod accepts.
#' `KubeJob()` finds a job again by name, for example from another R
#' session. It reads the token from the Job's Secret, which needs
#' permission to get Secrets in the namespace, and refuses a Job BiocKube
#' did not submit.
#'
#' @param name Name of the Kubernetes Job.
#' @param namespace Namespace the Job is in. Defaults to the client's
#'   namespace.
#' @param client A `kube_client`, or `NULL` for the default client.
#' @param object A `KubeJob`.
#'
#' @slot name Name of the Kubernetes Job.
#' @slot namespace Its namespace.
#' @slot uid Its uid, which tells it apart from a later Job of the same
#'   name.
#' @slot pod Name of the pod running the job.
#' @slot token Token the pod accepts. It is never printed.
#' @slot package Package declaring the job.
#' @slot job Name of the job in that package.
#' @slot outputs Named paths of the job's outputs in the container.
#' @slot client The `kube_client`, or `NULL` for the default
#'   client.
#'
#' @return `KubeJob()` returns a `KubeJob`.
#' @seealso [submitJob()] to start a job,
#'   [jobStatus()] to follow it and collect its outputs.
#' @examplesIf interactive()
#' job <- KubeJob("deseq2-differential-expression-3f9a1c07d2")
#' job
#' jobOutputs(job, dir = "results")
#' @export
setClass("KubeJob", slots = c(
    name = "character", namespace = "character", uid = "character",
    pod = "character", token = "character", package = "character",
    job = "character", outputs = "character", client = "ANY"
))

setValidity("KubeJob", function(object) {
    problems <- character()
    for (name in c("name", "namespace", "uid", "pod", "token", "package",
                   "job")) {
        if (!.isString(slot(object, name)))
            problems <- c(problems, sprintf("`%s` must be a string.", name))
    }
    if (!.validOutputs(object@outputs))
        problems <- c(problems, "`outputs` must be named paths.")
    if (!is.null(object@client) && !inherits(object@client, "kube_client"))
        problems <- c(problems, "`client` must be a kube_client or NULL.")
    if (length(problems)) problems else TRUE
})

#' @rdname KubeJob-class
#' @export
KubeJob <- function(name, namespace = NULL, client = NULL) {
    .dnsName(name, "name")
    namespace <- namespace %||% kube::kube_namespace(client)
    object <- kube::kube_get("jobs", name, namespace = namespace,
                             output = "object", client = client)
    config <- .jobConfig(object)
    uid <- .objectUid(object, "Job", name)
    new("KubeJob",
        name = name, namespace = namespace, uid = uid,
        pod = .sessionPod(client, namespace, name, uid),
        token = .sessionToken(client, namespace, name, uid),
        package = config$package, job = config$job,
        outputs = config$outputs, client = client)
}

#' @rdname KubeJob-class
#' @export
setMethod("show", "KubeJob", function(object) {
    state <- tryCatch(.jobState(object, timeout = .showTimeout)$state,
                      error = function(e) "unknown")
    cat("KubeJob", object@name, "\n")
    cat("  job:      ", paste(object@package, object@job, sep = "/"), "\n")
    cat("  namespace:", object@namespace, "\n")
    cat("  pod:      ", object@pod, "\n")
    cat("  state:    ", state, "\n")
    cat("  outputs:  ", paste(names(object@outputs), collapse = ", "), "\n")
})

.checkJob <- function(x) {
    if (!is(x, "KubeJob"))
        stop("`x` must be a KubeJob, as submitJob() returns.", call. = FALSE)
    invisible(x)
}

.isString <- function(x) {
    is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x)
}

## Output names become a URL segment and file names a local path, so both
## are plain names; the container paths are absolute.
.plainName <- "^[A-Za-z0-9][A-Za-z0-9_.-]*$"

.validOutputs <- function(outputs) {
    if (!length(outputs))
        return(is.character(outputs))
    names <- names(outputs)
    is.character(outputs) && !anyNA(outputs) && !is.null(names) &&
        all(grepl(.plainName, names)) && !anyDuplicated(names) &&
        all(startsWith(outputs, "/")) && !any(endsWith(outputs, "/")) &&
        all(grepl(.plainName, basename(outputs))) &&
        !anyDuplicated(basename(outputs))
}

.objectUid <- function(object, kind, name) {
    uid <- object[["metadata"]][["uid"]]
    if (!.isString(uid))
        stop(sprintf("The API server returned no uid for %s \"%s\".", kind,
                     name), call. = FALSE)
    uid
}

#' What a Job carries for later sessions
#'
#' @param object The Kubernetes Job, as the API server returns it.
#' @return A list with the `package`, the `job` and the `outputs`.
#' @noRd
.jobConfig <- function(object) {
    metadata <- object[["metadata"]] %||% list()
    name <- metadata[["name"]] %||% "?"
    text <- (metadata[["annotations"]] %||% list())[[.jobAnnotation]]
    if (is.null(text))
        stop(sprintf(
            "Job \"%s\" has no %s annotation; BiocKube did not submit it.",
            name, .jobAnnotation
        ), call. = FALSE)
    tryCatch(.decodeJobConfig(text), error = function(e)
        stop(sprintf("Job \"%s\" has an unreadable %s annotation: %s", name,
                     .jobAnnotation, conditionMessage(e)), call. = FALSE))
}

.encodeJobConfig <- function(package, job, outputs) {
    outputs <- as.list(outputs)
    names(outputs) <- names(outputs) %||% character()
    as.character(jsonlite::toJSON(
        list(package = package, job = job, outputs = outputs),
        auto_unbox = TRUE
    ))
}

## The annotation is anyone's to edit who may edit the Job, so it is parsed
## as data and checked before use.
.decodeJobConfig <- function(text) {
    config <- if (.isString(text))
        tryCatch(jsonlite::fromJSON(text, simplifyVector = FALSE),
                 error = function(e) NULL)
    if (!is.list(config) || is.null(names(config)))
        stop("it is not a JSON object.", call. = FALSE)
    if (!.isString(config$package) || !.isString(config$job))
        stop("it does not name a package and a job.", call. = FALSE)
    outputs <- config$outputs %||% list()
    if (!is.list(outputs) || !all(vapply(outputs, .isString, logical(1L))))
        stop("its outputs are not named container paths.", call. = FALSE)
    outputs <- if (length(outputs))
        vapply(outputs, identity, character(1L))
    else character()
    if (!.validOutputs(outputs))
        stop("its outputs are not named container paths.", call. = FALSE)
    list(package = config$package, job = config$job, outputs = outputs)
}

## Whether `object` lists the object `uid` among its owners, as its
## controller when `controller` is TRUE.
.ownedBy <- function(object, uid, controller = FALSE) {
    owners <- object[["metadata"]][["ownerReferences"]] %||% list()
    any(vapply(owners, function(owner)
        identical(owner[["uid"]], uid) &&
            (!controller || isTRUE(owner[["controller"]])),
        logical(1L)))
}

#' The pod running a job
#'
#' @param client A `kube_client`.
#' @param namespace,name The job.
#' @param uid Its uid; only a pod the Job controls counts.
#' @return The name of the pod. A Job that does not retry has one pod, and
#'   the newest wins while an earlier one is still being collected.
#' @noRd
.sessionPod <- function(client, namespace, name, uid) {
    pods <- kube::kube_get("pods", namespace = namespace,
                           selector = .sessionSelector(name),
                           output = "object", client = client)
    pods <- Filter(function(pod) .ownedBy(pod, uid, controller = TRUE), pods)
    if (!length(pods))
        stop(sprintf("Job \"%s\" in namespace \"%s\" has no pod.", name,
                     namespace), call. = FALSE)
    created <- vapply(pods, function(pod)
        pod$metadata$creationTimestamp %||% "", character(1L))
    pods[[order(created, decreasing = TRUE)[1L]]]$metadata$name
}

#' The session token of a job
#'
#' @param client A `kube_client`.
#' @param namespace,name The job, and the Secret named after it.
#' @param uid Its uid, which the Secret must name as its owner.
#' @return The token. Secret values travel base64 encoded.
#' @noRd
.sessionToken <- function(client, namespace, name, uid) {
    secret <- kube::kube_get("secrets", name, namespace = namespace,
                             output = "object", client = client)
    if (!.ownedBy(secret, uid))
        stop(sprintf(
            "Secret \"%s\" in namespace \"%s\" does not belong to Job \"%s\".",
            name, namespace, name
        ), call. = FALSE)
    encoded <- (secret[["data"]] %||% list())[["token"]]
    if (!is.character(encoded) || length(encoded) != 1L)
        stop(sprintf("Secret \"%s\" in namespace \"%s\" has no token.", name,
                     namespace), call. = FALSE)
    token <- rawToChar(.decodeBase64(encoded))
    if (!nzchar(token))
        stop(sprintf("Secret \"%s\" in namespace \"%s\" has an empty token.",
                     name, namespace), call. = FALSE)
    token
}

.base64Alphabet <- c(LETTERS, letters, as.character(0:9), "+", "/")

#' Decode base64 text
#'
#' @param text Base64 text, as the API server returns Secret values.
#' @return The bytes it encodes, as a raw vector.
#' @noRd
.decodeBase64 <- function(text) {
    symbols <- strsplit(gsub("[[:space:]]", "", text), "")[[1L]]
    values <- match(symbols[symbols != "="], .base64Alphabet) - 1L
    if (anyNA(values))
        stop("The value is not base64 text.", call. = FALSE)
    ## Six bits per symbol, most significant first, regrouped into bytes;
    ## packBits() reads each byte least significant bit first.
    bits <- rawToBits(as.raw(values))
    dim(bits) <- c(8L, length(values))
    bits <- as.vector(bits[6:1, , drop = FALSE])
    length(bits) <- 8L * (length(bits) %/% 8L)
    dim(bits) <- c(8L, length(bits) %/% 8L)
    packBits(as.vector(bits[8:1, , drop = FALSE]), "raw")
}
