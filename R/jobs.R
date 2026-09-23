## Job dispatch: a job declared with BiocJobs becomes a Kubernetes Job
## whose pod runs runner.R, which takes the inputs the session uploads,
## runs the job's own command and serves its outputs until they are
## collected.

.jobPort <- 8080L
.jobStartTimeout <- 600
.jobAnnotation <- "biockube.bioconductor.org/job"

## Hexadecimal digits of the random part of a generated Job name.
.nameSuffix <- 10L
## Seconds between two looks at the Job while its pod is awaited.
.jobCheckInterval <- 5
## Seconds a request about a job's state may take; show() waits less.
.statusTimeout <- 30
.showTimeout <- 10
## Seconds without data after which an upload or a download is abandoned.
.stallTimeout <- 300L
## The line runner.R logs once the job's command has exited.
.exitLine <- "^[0-9-]+ [0-9:]+ command exited with status [0-9]+$"
## Seconds before a followed log whose stream has ended is read again.
.logPause <- 1
## The time the kubelet puts before each line of a log, RFC 3339.
.logStamp <- paste0("^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})",
                    "(\\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$")

#' Run a BiocJobs job on a cluster
#'
#' Runs a job declared with BiocJobs as a Kubernetes Job, built with
#' `BiocJobs::kubernetesJob()`. Inputs given as local files are uploaded to
#' the job's pod, which runs the job and keeps its outputs until
#' [jobOutputs()] collects them.
#'
#' `submitJob()` waits up to ten minutes for the pod, uploads the inputs
#' and starts the job's command. It fails early when the pod cannot become
#' ready or the cluster refuses to create it, for instance because of a Pod
#' Security policy; a pod kept out by a ResourceQuota is waited for. A
#' submission that fails or is interrupted before the command starts
#' deletes its Job. The Job owns a ConfigMap and a Secret, which are
#' deleted with it.
#'
#' Anyone who can create or modify pods in the namespace can pose as the
#' job's pod and receive its inputs, so run jobs only in namespaces whose
#' pod creators you trust.
#'
#' @param package A `BiocJob`, or the name of, or path to, a package
#'   declaring jobs.
#' @param job Name of the job to run. Not needed when `package` is a
#'   `BiocJob`.
#' @param values Named list of input and option values. An input is a
#'   local file, which is uploaded, or an `http`, `https` or `ftp`
#'   URL, which the pod downloads with `curl`. Options left out take the
#'   defaults the job declares.
#' @param image Container image with R, BiocJobs, the package declaring the
#'   job and, for URL inputs, `curl`. Defaults to the job's `container`;
#'   required when the job declares none.
#' @param namespace Namespace to run in. Defaults to the client's
#'   namespace.
#' @param name Name of the Kubernetes Job, at most 63 lower case letters,
#'   digits and dashes. Defaults to the job's name, lower-cased and
#'   shortened to fit, a dash and ten random hexadecimal digits.
#' @param keep Seconds the pod keeps its outputs after the job has
#'   finished, unless [jobOutputs()] collects them earlier.
#' @param client A `kube_client`, or `NULL` for the default client.
#'
#' @return A [KubeJob].
#' @seealso [jobStatus()] to follow a job and collect its outputs,
#'   [KubeJob()] to reach one from another session, and the
#'   script `system.file("examples", "deseq2-job.R", package = "BiocKube")`.
#' @examplesIf interactive()
#' spec <- BiocJobs::readJob("deseq2-differential-expression.yaml")
#' job <- submitJob(spec, values = list(
#'     counts = "counts.tsv", coldata = "coldata.tsv",
#'     contrast_factor = "condition",
#'     contrast_numerator = "treated", contrast_denominator = "control"
#' ))
#' waitJob(job)
#' jobOutputs(job, dir = "results")
#' @export
submitJob <- function(package, job, values = list(), image = NULL,
                      namespace = NULL, name = NULL, keep = 86400L,
                      client = NULL) {
    spec <- .jobSpec(package, if (missing(job)) NULL else job)
    values <- .splitValues(spec, values)
    namespace <- namespace %||% kube::kube_namespace(client)
    created <- .createJob(client, spec, namespace, name, values, image, keep)
    name <- created$name
    complete <- FALSE
    on.exit(if (!complete)
        .discardJob(client, namespace, name,
                    created$object[["metadata"]][["uid"]]))
    uid <- .objectUid(created$object, "Job", name)
    token <- .newToken()
    .createJobResources(client, namespace, name, token,
                        .ownerReference("Job", name, uid, "batch/v1"))
    .resumeJob(client, namespace, name)
    x <- new("KubeJob", name = name, namespace = namespace, uid = uid,
             pod = .waitJobPod(client, namespace, name, uid,
                               .jobStartTimeout),
             token = token, package = as.character(spec$package),
             job = as.character(spec$name),
             outputs = .jobConfig(created$manifest)$outputs, client = client)
    .uploadInputs(x, values$uploads)
    .proxyRequest(client, namespace, x@pod, .jobPort, "start",
                  method = "POST", token = token)
    complete <- TRUE
    x
}

#' Follow a job on a cluster
#'
#' Functions for jobs started with [submitJob()].
#' `jobStatus()` returns the state of a job, from its pod or, when the pod
#' does not answer, from the cluster. `waitJob()` waits for the job to
#' end, asking again after transient failures to reach the cluster.
#' `jobLogs()` returns or prints what the job printed. `jobOutputs()`
#' downloads the outputs and lets the pod exit. `deleteJob()` deletes the
#' Kubernetes Job, its pod, ConfigMap and Secret, and `listJobs()` lists
#' the Jobs BiocKube submitted.
#'
#' `jobOutputs()` fails for a job that has not finished, and warns about
#' outputs the job did not write, as when it failed early. When a download
#' fails, the pod keeps the outputs and `jobOutputs()` can be called again.
#'
#' @param x A [KubeJob].
#' @param timeout Seconds to wait for the job to end.
#' @param interval Seconds between two status checks.
#' @param follow Print the log as it grows, until the job's command has
#'   exited, instead of returning what it has printed so far.
#' @param dir Directory to write the outputs to. It is created if needed.
#' @param release Let the pod exit once the outputs are downloaded.
#' @param namespace Namespace to list. Defaults to the client's namespace.
#' @param client A `kube_client`, or `NULL` for the default client.
#'
#' @return
#' - `jobStatus()`: `"waiting"`, `"running"`, `"succeeded"`, `"failed"` or
#'   `"unknown"`.
#' - `waitJob()`: `x`, invisibly, once the job succeeds. Errors of class
#'   `biockube_job_failed` (field `status`: the command's exit status) if it
#'   fails, `biockube_job_timeout` if it does not end within `timeout`, and
#'   a plain error if the job is `"unknown"`.
#' - `jobLogs()`: the log lines, a character vector that prints as the log;
#'   `NULL` invisibly with `follow = TRUE`.
#' - `jobOutputs()`: local paths named after the outputs, `NA` for outputs
#'   the job did not write.
#' - `deleteJob()`: `x`, invisibly. `listJobs()`: a table of Jobs.
#' @examplesIf interactive()
#' job <- KubeJob("deseq2-differential-expression-3f9a1c07d2")
#' jobStatus(job)
#' waitJob(job, timeout = 3600)
#' jobLogs(job)
#' outputs <- jobOutputs(job, dir = "results")
#'
#' listJobs()
#' deleteJob(job)
#' @export
jobStatus <- function(x) {
    .checkJob(x)
    .jobState(x, timeout = .statusTimeout)$state
}

#' @rdname jobStatus
#' @export
waitJob <- function(x, timeout = Inf, interval = 2) {
    .checkJob(x)
    if (!is.numeric(timeout) || length(timeout) != 1L || is.na(timeout) ||
        timeout <= 0)
        stop("`timeout` must be a positive number of seconds.", call. = FALSE)
    if (!is.numeric(interval) || length(interval) != 1L ||
        !is.finite(interval) || interval <= 0)
        stop("`interval` must be a positive number of seconds.",
             call. = FALSE)
    deadline <- Sys.time() + timeout
    trouble <- NULL
    repeat {
        remaining <- as.numeric(deadline - Sys.time(), units = "secs")
        state <- tryCatch(
            .jobState(x, timeout = max(1, min(remaining, .statusTimeout))),
            error = function(e) {
                if (!.transient(e))
                    stop(e)
                trouble <<- e
                NULL
            }
        )
        if (!is.null(state)) {
            trouble <- NULL
            if (identical(state$state, "succeeded"))
                return(invisible(x))
            if (identical(state$state, "failed"))
                stop(.jobFailure(x, state$exit))
            if (identical(state$state, "unknown"))
                stop(sprintf("Job \"%s\" in namespace \"%s\" ", x@name,
                             x@namespace),
                     "cannot be reached: the Job is gone, or its pod has ",
                     "gone and the Job records no outcome.", call. = FALSE)
        }
        remaining <- as.numeric(deadline - Sys.time(), units = "secs")
        if (remaining <= 0)
            stop(.jobTimeout(x, timeout, trouble))
        Sys.sleep(min(interval, remaining))
    }
}

#' @rdname jobStatus
#' @export
jobLogs <- function(x, follow = FALSE) {
    .checkJob(x)
    if (!isTRUE(follow) && !isFALSE(follow))
        stop("`follow` must be TRUE or FALSE.", call. = FALSE)
    if (!follow)
        return(kube::kube_logs(x@pod, container = "job",
                               namespace = x@namespace, client = x@client))
    .followLog(x)
}

#' @rdname jobStatus
#' @export
jobOutputs <- function(x, dir = ".", release = TRUE) {
    .checkJob(x)
    if (!is.character(dir) || length(dir) != 1L || is.na(dir) || !nzchar(dir))
        stop("`dir` must be a directory path.", call. = FALSE)
    if (!isTRUE(release) && !isFALSE(release))
        stop("`release` must be TRUE or FALSE.", call. = FALSE)
    state <- .jobState(x, timeout = .statusTimeout)
    if (!state$state %in% c("succeeded", "failed"))
        stop(sprintf(
            "Job \"%s\" is %s; wait for it to finish with waitJob().",
            x@name, state$state
        ), call. = FALSE)
    if (!state$answered)
        stop(sprintf("Job \"%s\" has finished, ", x@name),
             "but its pod no longer serves the outputs: they were ",
             "released, or kept for `keep` seconds only.", call. = FALSE)
    if (!dir.exists(dir) && !dir.create(dir, recursive = TRUE))
        stop(sprintf("Cannot create directory \"%s\".", dir), call. = FALSE)
    paths <- vapply(names(x@outputs), function(name) {
        path <- file.path(dir, basename(x@outputs[[name]]))
        tryCatch({
            .downloadOutput(x, name, path)
            path
        }, biockube_error_notfound = function(e) NA_character_)
    }, character(1L))
    missing <- names(paths)[is.na(paths)]
    if (length(missing))
        warning(sprintf("Job \"%s\" did not write %s.", x@name,
                        paste0("\"", missing, "\"", collapse = ", ")),
                call. = FALSE)
    if (release)
        .proxyRequest(x@client, x@namespace, x@pod, .jobPort, "release",
                      method = "POST", token = x@token)
    paths
}

#' @rdname jobStatus
#' @export
deleteJob <- function(x) {
    .checkJob(x)
    tryCatch(
        .deleteJob(x@client, x@namespace, x@name, x@uid),
        kube_error_conflict = function(e)
            stop(sprintf("Job \"%s\" in namespace \"%s\" ", x@name,
                         x@namespace),
                 "is a later Job that took the name of this one; it was ",
                 "not deleted.", call. = FALSE)
    )
    invisible(x)
}

#' @rdname jobStatus
#' @export
listJobs <- function(namespace = NULL, client = NULL) {
    kube::kube_get("jobs", namespace = namespace, selector = .managedBy,
                   client = client)
}

#' The job a submission refers to
#'
#' @param package A `BiocJob`, or a package name or path.
#' @param job Name of the job, or `NULL` when `package` is a `BiocJob`.
#' @return The `BiocJob`.
#' @noRd
.jobSpec <- function(package, job) {
    .requireBiocJobs()
    if (inherits(package, "BiocJob")) {
        if (!is.null(job) && !identical(as.character(job),
                                        as.character(package$name)))
            stop(sprintf("`package` is the job \"%s\", not \"%s\".",
                         package$name, job), call. = FALSE)
        return(package)
    }
    if (!is.character(package) || length(package) != 1L || is.na(package) ||
        !nzchar(package))
        stop("`package` must be a BiocJob, a package name or a path.",
             call. = FALSE)
    if (is.null(job) || !is.character(job) || length(job) != 1L || is.na(job))
        stop(sprintf("`job` must name one of the jobs of \"%s\".", package),
             call. = FALSE)
    jobs <- BiocJobs::findJobs(package)
    if (!job %in% names(jobs))
        stop(sprintf("Package \"%s\" declares %s.", package,
                     if (length(jobs))
                         sprintf("no job \"%s\", but %s", job,
                                 paste(names(jobs), collapse = ", "))
                     else "no jobs"), call. = FALSE)
    jobs[[job]]
}

.requireBiocJobs <- function() {
    if (!requireNamespace("BiocJobs", quietly = TRUE))
        stop("submitJob() needs the BiocJobs package.", call. = FALSE)
    if (!"kubernetesJob" %in% getNamespaceExports("BiocJobs"))
        stop("submitJob() needs a BiocJobs version with kubernetesJob().",
             call. = FALSE)
    invisible(NULL)
}

#' Sort the values a submission was given
#'
#' @param spec A `BiocJob`.
#' @param values Named input and option values.
#' @return A list with the `inputs` staged from a URL, the `uploads` as
#'   local paths, and the `options`.
#' @noRd
.splitValues <- function(spec, values) {
    if (!is.list(values))
        values <- as.list(values)
    given <- names(values)
    if (length(values) && (is.null(given) || !all(nzchar(given)) ||
                           anyDuplicated(given)))
        stop("`values` must be uniquely named.", call. = FALSE)
    inputs <- .specNames(spec$inputs)
    options <- .specNames(spec$options)
    unknown <- setdiff(given, c(inputs, options))
    if (length(unknown))
        stop(sprintf("Job \"%s\" has no value %s. It takes %s.", spec$name,
                     paste0("\"", unknown, "\"", collapse = " or "),
                     paste(c(inputs, options), collapse = ", ")),
             call. = FALSE)
    inputs <- intersect(inputs, given)
    paths <- vapply(inputs, function(name) .inputValue(values[[name]], name),
                    character(1L))
    ## The same schemes BiocJobs::kubernetesJob() stages.
    staged <- grepl("^(https?|ftp)://", paths)
    other <- !staged & grepl("^[A-Za-z][A-Za-z0-9+.-]*://", paths)
    if (any(other))
        stop(sprintf(
            "Input %s must be a local file or an http, https or ftp URL.",
            paste0("\"", names(paths)[other], "\"", collapse = ", ")
        ), call. = FALSE)
    list(inputs = paths[staged], uploads = .localInputs(paths[!staged]),
         options = values[intersect(options, given)])
}

.inputValue <- function(value, name) {
    if (!is.character(value) || length(value) != 1L || is.na(value) ||
        !nzchar(value))
        stop(sprintf("Input \"%s\" must be a file path or a URL.", name),
             call. = FALSE)
    value
}

.localInputs <- function(paths) {
    absent <- paths[!file.exists(paths) | dir.exists(paths)]
    if (length(absent))
        stop(sprintf("Input file %s does not exist.",
                     paste0("\"", absent, "\"", collapse = ", ")),
             call. = FALSE)
    if (!length(paths))
        return(character())
    vapply(paths, normalizePath, character(1L), winslash = "/")
}

.specNames <- function(x) {
    vapply(x, function(e) as.character(e$name), character(1L))
}

#' The name of a submission
#'
#' @param spec A `BiocJob`.
#' @param name The name asked for, or `NULL`.
#' @return A DNS-1123 name of at most 63 characters. The random part comes
#'   from the same source as the session token, so two sessions do not pick
#'   the same name even when both have set a seed.
#' @noRd
.jobName <- function(spec, name) {
    if (!is.null(name)) {
        .dnsName(name, "name")
        return(name)
    }
    label <- .dnsLabel(spec$name, 62L - .nameSuffix)
    paste0(if (nzchar(label)) label else "biocjob", "-",
           .newToken(.nameSuffix %/% 2L))
}

.dnsLabel <- function(x, max) {
    x <- gsub("[^a-z0-9-]", "-", tolower(as.character(x)))
    sub("[^a-z0-9]+$", "", substr(sub("^[^a-z0-9]+", "", x), 1L, max))
}

#' Create the Kubernetes Job of a submission
#'
#' @param client A `kube_client`.
#' @param spec A `BiocJob`.
#' @param namespace Namespace to create it in.
#' @param name The name asked for, or `NULL` for a generated one, which is
#'   drawn again when a Job of that name exists.
#' @param values The values from `.splitValues()`.
#' @param image,keep As for `.jobManifest()`.
#' @param attempts Number of generated names to try.
#' @return A list with the `name`, the `manifest` and the created `object`.
#' @noRd
.createJob <- function(client, spec, namespace, name, values, image, keep,
                       attempts = 3L) {
    for (attempt in seq_len(attempts)) {
        jobName <- .jobName(spec, name)
        manifest <- .jobManifest(spec, namespace, jobName,
                                 inputs = values$inputs,
                                 uploads = names(values$uploads),
                                 options = values$options, image = image,
                                 keep = keep)
        object <- tryCatch(.create(client, manifest),
                           kube_error_alreadyexists = function(e) {
            if (!is.null(name))
                stop(sprintf("A Job named \"%s\" already exists in ", name),
                     sprintf("namespace \"%s\"; give another `name`.",
                             namespace), call. = FALSE)
            if (attempt == attempts)
                stop(e)
            NULL
        })
        if (!is.null(object))
            return(list(name = jobName, manifest = manifest, object = object))
    }
}

#' Adapt the manifest BiocJobs generates
#'
#' @param spec A `BiocJob`.
#' @param namespace,name Namespace and name of the Kubernetes Job.
#' @param inputs Named URLs of the inputs the pod downloads itself.
#' @param uploads Names of the inputs the session uploads.
#' @param options Option values for the job.
#' @param image Container image, or `NULL` for the job's own.
#' @param keep Seconds the pod keeps its outputs.
#' @param port Port the pod listens on.
#' @return The Job manifest, suspended.
#' @noRd
.jobManifest <- function(spec, namespace, name, inputs = character(),
                         uploads = character(), options = list(),
                         image = NULL, keep = 86400L, port = .jobPort) {
    .requireBiocJobs()
    manifest <- unclass(BiocJobs::kubernetesJob(spec, inputs = inputs,
                                                options = options,
                                                image = image))
    pod <- manifest$spec$template$spec
    index <- .jobContainer(pod$containers)
    container <- pod$containers[[index]]
    argv <- as.character(unlist(container$command))
    pod$initContainers <- .stagedInputs(pod$initContainers, spec, uploads)
    .checkComplete(spec, argv, pod$initContainers)

    config <- list(
        command = argv,
        uploads = .commandPaths(argv, uploads, "input"),
        outputs = .commandPaths(argv, .specNames(spec$outputs), "output")
    )
    container$command <- I(c("Rscript", "/biockube/runner.R"))
    container$env <- .runnerEnv(config, name, keep, port)
    container$ports <- list(list(name = "biockube",
                                 containerPort = as.integer(port),
                                 protocol = "TCP"))
    container$readinessProbe <- list(
        httpGet = list(path = "/healthz", port = as.integer(port)),
        periodSeconds = 2L
    )
    container$volumeMounts <- c(container$volumeMounts, list(list(
        name = "scripts", mountPath = "/biockube", readOnly = TRUE)))
    pod$containers[[index]] <- container
    pod$volumes <- c(pod$volumes, list(list(
        name = "scripts", configMap = list(name = name))))

    labels <- c(manifest$metadata$labels, .jobLabels(name))
    labels <- labels[!duplicated(names(labels), fromLast = TRUE)]
    annotations <- manifest$metadata$annotations
    annotations[[.jobAnnotation]] <- .encodeJobConfig(
        as.character(spec$package), as.character(spec$name), config$outputs)
    manifest$metadata <- list(name = name, namespace = namespace,
                              labels = labels, annotations = annotations)
    manifest$spec$template$metadata$labels <- labels
    manifest$spec$template$spec <- pod
    ## The pod must not start before the ConfigMap and the Secret it needs
    ## exist; submitJob() resumes the Job once they do.
    manifest$spec$suspend <- TRUE
    manifest
}

.jobContainer <- function(containers) {
    index <- which(vapply(containers, function(x) identical(x$name, "job"),
                          logical(1L)))
    if (length(index) != 1L)
        stop("BiocJobs::kubernetesJob() produced no \"job\" container.",
             call. = FALSE)
    index
}

## One init container stages each declared input, in declaration order.
## The inputs the session uploads need none.
.stagedInputs <- function(initContainers, spec, uploads) {
    inputs <- .specNames(spec$inputs)
    if (length(initContainers) != length(inputs))
        stop(sprintf("BiocJobs::kubernetesJob() staged %d of %d inputs.",
                     length(initContainers), length(inputs)), call. = FALSE)
    kept <- initContainers[!inputs %in% uploads]
    if (length(kept)) kept else NULL
}

## Inputs and options BiocJobs could not fill in are left as placeholders.
.checkComplete <- function(spec, argv, initContainers) {
    staged <- unlist(lapply(initContainers, function(x)
        as.character(unlist(x$command))))
    left <- grep("^\\{\\{.*\\}\\}$", c(argv, staged), value = TRUE)
    if (!length(left))
        return(invisible(NULL))
    left <- unique(sub("\\.url$", "", gsub("[{}]", "", left)))
    stop(sprintf("Job \"%s\" needs values for %s.", spec$name,
                 paste(left, collapse = ", ")), call. = FALSE)
}

#' Paths the job command gives its inputs and outputs
#'
#' @param argv The command BiocJobs generated.
#' @param params Names of the inputs or outputs to look up.
#' @param what What they are, for the error message.
#' @return The container paths, named after the inputs or outputs.
#' @noRd
.commandPaths <- function(argv, params, what) {
    if (!length(params))
        return(character())
    positions <- match(paste0("--", params), argv)
    found <- !is.na(positions) & positions < length(argv)
    if (!all(found))
        stop(sprintf("The job command carries no path for %s %s.", what,
                     paste0("\"", params[!found], "\"", collapse = ", ")),
             call. = FALSE)
    paths <- argv[positions + 1L]
    names(paths) <- params
    paths
}

.runnerEnv <- function(config, secret, keep, port) {
    values <- list(BIOCKUBE_RUNNER = .encodeConfig(config),
                   BIOCKUBE_KEEP = .number(keep, "keep"),
                   BIOCKUBE_PORT = format(as.integer(port)))
    entries <- lapply(names(values), function(name)
        list(name = name, value = values[[name]]))
    c(list(list(
        name = "BIOCKUBE_TOKEN",
        valueFrom = list(secretKeyRef = list(name = secret, key = "token"))
    )), entries)
}

.jobLabels <- function(name) {
    .namedStrings(c(.managedBy, .sessionSelector(name)), "labels")
}

.sessionSelector <- function(name) {
    structure(name, names = .sessionLabel)
}

#' Create the ConfigMap and the Secret the pod mounts
#'
#' @param client A `kube_client`.
#' @param namespace,name The job; both objects are named after it.
#' @param token Session token.
#' @param owner Reference to the Job, which owns both objects, so that
#'   deleting the Job removes them.
#' @return `NULL`, invisibly.
#' @noRd
.createJobResources <- function(client, namespace, name, token, owner) {
    metadata <- list(name = name, namespace = namespace,
                     labels = .jobLabels(name),
                     ownerReferences = list(owner))
    .createDependent(client, list(
        apiVersion = "v1", kind = "ConfigMap", metadata = metadata,
        data = .podScripts(c("server.R", "runner.R"))
    ), owner)
    .createDependent(client, list(
        apiVersion = "v1", kind = "Secret", metadata = metadata,
        type = "Opaque", stringData = list(token = token)
    ), owner)
    invisible(NULL)
}

## The garbage collector removes what a deleted Job owned some time after
## the Job has gone, so an object a deleted Job of the same name left is
## removed first.
.createDependent <- function(client, manifest, owner) {
    tryCatch(.create(client, manifest), kube_error_alreadyexists = function(e) {
        .removeLeftover(client, manifest, owner)
        .create(client, manifest)
    })
}

.removeLeftover <- function(client, manifest, owner) {
    kind <- manifest$kind
    name <- manifest$metadata$name
    namespace <- manifest$metadata$namespace
    path <- paste0("/api/v1/namespaces/", .pathSegment(namespace, "namespace"),
                   "/", tolower(kind), "s/", .pathSegment(name, "name"))
    found <- .getObject(client, path, .statusTimeout)
    if (is.null(found))
        return(invisible(NULL))
    if (!.leftBy(found, owner))
        stop(sprintf("%s \"%s\" already exists in namespace \"%s\"; ", kind,
                     name, namespace),
             "remove it or give another `name`.", call. = FALSE)
    tryCatch(
        kube::kube_request(client, "DELETE", path, body = .deleteOptions(
            found[["metadata"]][["uid"]])),
        kube_error_notfound = function(e) NULL
    )
    invisible(NULL)
}

## Whether a Job named as `owner` but other than it owns `object`. Job names
## are unique, so that Job has been deleted.
.leftBy <- function(object, owner) {
    owners <- object[["metadata"]][["ownerReferences"]] %||% list()
    any(vapply(owners, function(x)
        identical(x[["kind"]], "Job") && identical(x[["name"]], owner$name) &&
            !identical(x[["uid"]], owner$uid),
        logical(1L)))
}

.resumeJob <- function(client, namespace, name) {
    suppressMessages(kube::kube_patch(
        "jobs", name, patch = list(spec = list(suspend = FALSE)),
        namespace = namespace, client = client
    ))
    invisible(NULL)
}

.discardJob <- function(client, namespace, name, uid = NULL) {
    tryCatch(
        .deleteJob(client, namespace, name, uid),
        kube_error_notfound = function(e) NULL,
        kube_error_conflict = function(e) NULL,
        error = function(e) .reportLeftover(namespace, name)
    )
    invisible(NULL)
}

## Deleting the Job takes its pod, ConfigMap and Secret with it. With `uid`,
## a later Job of the same name is left alone.
.deleteJob <- function(client, namespace, name, uid = NULL) {
    kube::kube_request(client, "DELETE", .jobPath(namespace, name),
                       body = .deleteOptions(uid, "Background"))
    invisible(NULL)
}

## The API server reads no query parameters of a DELETE that has a body.
.deleteOptions <- function(uid = NULL, propagation = NULL) {
    .compact(list(propagationPolicy = propagation,
                  preconditions = if (.isString(uid)) list(uid = uid)))
}

.reportLeftover <- function(namespace, name) {
    message(sprintf("Job \"%s\" is still on the cluster; remove it with ",
                    name),
            sprintf("kube::kube_delete(\"jobs\", \"%s\", namespace = \"%s\").",
                    name, namespace))
}

#' Wait for the pod of a job to be ready
#'
#' Pods carrying the session label count only when the Job controls them.
#' Between spells of watching them, the Job is read for a `Failed`
#' condition and, until its pod appears, for `FailedCreate` events, which
#' is all the Job controller records when admission refuses the pod.
#'
#' @param client A `kube_client`.
#' @param namespace,name The job, whose session label its pod carries.
#' @param uid The uid of the Job.
#' @param timeout Seconds to wait.
#' @return The name of the pod. A pod that cannot become ready, or that
#'   admission refuses for good, ends the wait at once. A refusal the Job
#'   controller may overcome by trying again is reported if the wait times
#'   out.
#' @noRd
.waitJobPod <- function(client, namespace, name, uid, timeout) {
    state <- new.env(parent = emptyenv())
    state$pod <- NULL
    state$failed <- NULL
    state$seen <- FALSE
    state$pending <- NULL
    refused <- NULL
    callback <- .jobPodCallback(state, uid, namespace)
    deadline <- as.numeric(Sys.time()) + timeout
    version <- NULL
    repeat {
        problem <- .jobProblem(client, namespace, name, uid,
                               events = !state$seen)
        if (isTRUE(problem$final))
            stop(sprintf("Job \"%s\" cannot run its pod: %s", name,
                         problem$message), call. = FALSE)
        if (!is.null(problem))
            refused <- sprintf("The cluster refused to create it: %s",
                               problem$message)
        remaining <- deadline - as.numeric(Sys.time())
        if (remaining <= 0)
            break
        version <- kube::kube_watch(
            "pods", callback, namespace = namespace,
            selector = .sessionSelector(name), resource_version = version,
            timeout = min(remaining, .jobCheckInterval), client = client
        )
        if (!is.null(state$failed))
            stop(.failedPodMessage(client, state$failed, namespace),
                 call. = FALSE)
        if (!is.null(state$pod))
            return(state$pod)
    }
    stop(paste(c(
        sprintf("The pod of job \"%s\" was not ready within %s seconds.",
                name, format(timeout)),
        if (state$seen) state$pending else refused
    ), collapse = "\n"), call. = FALSE)
}

.jobPodCallback <- function(state, uid, namespace) {
    function(type, object) {
        if (!.ownedBy(object, uid, controller = TRUE))
            return(TRUE)
        state$seen <- TRUE
        pod <- .podState(object)
        failure <- if (identical(type, "DELETED"))
            list(reason = "Deleted", message = "the pod was deleted")
        else if (pod$failed)
            list(reason = pod$reason, message = pod$message)
        else if (identical(pod$phase, "Succeeded"))
            list(reason = "Completed",
                 message = "the pod exited before it was ready")
        if (!is.null(failure)) {
            state$failed <- c(failure, list(object = object))
            return(FALSE)
        }
        if (isTRUE(pod$ready)) {
            state$pod <- object$metadata$name
            return(FALSE)
        }
        state$pending <- .pendingReason(object, pod, namespace)
        TRUE
    }
}

## What a pod that is not ready waits for, as the scheduler or the kubelet
## reports it.
.pendingReason <- function(object, pod, namespace) {
    label <- sprintf("Pod %s/%s", namespace, object$metadata$name %||% "?")
    scheduling <- pod$scheduling %||% NA_character_
    if (!is.na(scheduling))
        return(sprintf("%s is not scheduled: %s", label, scheduling))
    status <- object$status %||% list()
    for (container in c(status$initContainerStatuses,
                        status$containerStatuses)) {
        waiting <- container$state$waiting
        if (!is.null(waiting$reason))
            return(sprintf("%s is waiting: %s", label,
                           paste(c(waiting$reason, waiting$message),
                                 collapse = ": ")))
    }
    NULL
}

#' Why a Job cannot run its pod
#'
#' @param client A `kube_client`.
#' @param namespace,name,uid The Job.
#' @param events Also look for `FailedCreate` events.
#' @return `NULL`, or a list with the `message` and whether the problem is
#'   `final`: a `Failed` condition, or a refusal the Job controller cannot
#'   overcome. Failing to read the Job or its events, for want of
#'   permission for instance, is not a problem.
#' @noRd
.jobProblem <- function(client, namespace, name, uid, events = TRUE) {
    tryCatch({
        job <- .getObject(client, .jobPath(namespace, name), .statusTimeout)
        failed <- .conditionIn(job$status, "Failed")
        if (!is.null(failed))
            return(list(message = paste(c(failed$reason, failed$message),
                                        collapse = ": "),
                        final = TRUE))
        if (!events)
            return(NULL)
        found <- kube::kube_request(
            client, "GET", paste0("/api/v1/namespaces/",
                                  .pathSegment(namespace, "namespace"),
                                  "/events"),
            query = list(fieldSelector = paste0("involvedObject.uid=", uid,
                                                ",reason=FailedCreate")),
            timeout = .statusTimeout
        )$items
        if (!length(found))
            return(NULL)
        seen <- vapply(found, function(event)
            as.character(event$lastTimestamp %||% event$eventTime %||%
                             event$metadata$creationTimestamp %||% ""),
            character(1L))
        message <- found[[order(seen, decreasing = TRUE)[1L]]]$message %||%
            "FailedCreate"
        list(message = message, final = .finalRefusal(message))
    }, kube_error = function(e) NULL)
}

## The Job controller keeps trying to create a pod the cluster refuses.
## Admission refuses for good a pod that breaks a policy, but not one that
## a quota in use by other pods or a service account yet to be created
## keeps out, nor one that an admission webhook failed to review.
.finalRefusal <- function(message) {
    grepl("is forbidden|denied the request", message) &&
        !grepl("exceeded quota|error looking up service account", message)
}

.conditionIn <- function(status, type) {
    for (condition in status$conditions %||% list())
        if (identical(condition$type, type) &&
            identical(condition$status, "True"))
            return(condition)
    NULL
}

.jobPath <- function(namespace, name) {
    paste0("/apis/batch/v1/namespaces/", .pathSegment(namespace, "namespace"),
           "/jobs/", .pathSegment(name, "name"))
}

.podPath <- function(namespace, name) {
    paste0("/api/v1/namespaces/", .pathSegment(namespace, "namespace"),
           "/pods/", .pathSegment(name, "pod"))
}

.getObject <- function(client, path, timeout = NULL) {
    tryCatch(kube::kube_request(client, "GET", path, timeout = timeout),
             kube_error_notfound = function(e) NULL)
}

#' Print a job's log as it grows
#'
#' The runner outlives the command to serve its outputs, so the log is
#' followed until the runner reports the command's exit. A stream that ends
#' before, as when a proxy closes an idle connection, is read again from the
#' last line printed, for as long as the job has not finished.
#'
#' @param x A `KubeJob`.
#' @return `NULL`, invisibly.
#' @noRd
.followLog <- function(x) {
    path <- paste0(.podPath(x@namespace, x@pod), "/log")
    last <- list(second = NULL, key = "", count = 0L)
    repeat {
        from <- last
        skipped <- 0L
        exited <- FALSE
        kube::kube_request(
            x@client, "GET", path, accept = "text/plain, */*",
            query = list(container = "job", follow = TRUE, timestamps = TRUE,
                         sinceTime = from$second),
            stream = function(line) {
                entry <- .logEntry(line)
                if (!is.na(entry$key)) {
                    ## The stream starts again at the second of the last
                    ## line printed.
                    if (entry$key < from$key)
                        return(TRUE)
                    if (entry$key == from$key && skipped < from$count) {
                        skipped <<- skipped + 1L
                        return(TRUE)
                    }
                    last <<- list(second = entry$second, key = entry$key,
                                  count = if (entry$key == last$key)
                                      last$count + 1L else 1L)
                }
                cat(entry$text, "\n", sep = "")
                exited <<- grepl(.exitLine, entry$text)
                !exited
            }
        )
        if (exited || !.jobState(x, timeout = .statusTimeout)$state %in%
            c("waiting", "running"))
            return(invisible(NULL))
        Sys.sleep(.logPause)
    }
}

#' A line of a log read with timestamps
#'
#' @param line The line.
#' @return A list with the `text` of the line and, when it has a timestamp,
#'   its `second` as the API server takes it back in `sinceTime`, and a
#'   `key` of digits that sorts as the time does. Lines from one node share
#'   the UTC offset, which is left out of the key.
#' @noRd
.logEntry <- function(line) {
    stamp <- sub(" .*", "", line)
    parts <- regmatches(stamp, regexec(.logStamp, stamp))[[1L]]
    if (!length(parts))
        return(list(second = NULL, key = NA_character_, text = line))
    fraction <- substr(paste0(sub(".", "", parts[3L], fixed = TRUE),
                              "000000000"), 1L, 9L)
    list(second = paste0(parts[2L], parts[4L]),
         key = paste0(gsub("[^0-9]", "", parts[2L]), fraction),
         text = substring(line, nchar(stamp) + 2L))
}

.uploadInputs <- function(x, uploads) {
    for (name in names(uploads))
        .uploadInput(x, name, uploads[[name]])
    invisible(NULL)
}

## The file is read as curl sends it. Its size goes ahead as
## Content-Length: the pod refuses chunked bodies.
.uploadInput <- function(x, name, path) {
    con <- file(path, "rb")
    on.exit(close(con))
    request <- .transferRequest(x, "PUT", paste0("inputs/", name))
    curl::handle_setopt(request$handle, upload = TRUE,
                        infilesize_large = file.size(path),
                        readfunction = function(n) readBin(con, "raw", n),
                        seekfunction = function(offset) seek(con, offset))
    response <- .transfer(x, curl::curl_fetch_memory(request$url,
                                                     request$handle))
    if (response$status_code != 201L)
        stop(.transferError(x, response$status_code, response$content))
    invisible(NULL)
}

## The body goes to a file next to `path`, which it replaces once complete.
.downloadOutput <- function(x, name, path) {
    partial <- tempfile(".download-", tmpdir = dirname(path))
    on.exit(unlink(partial))
    request <- .transferRequest(x, "GET", paste0("outputs/", name))
    response <- .transfer(x, curl::curl_fetch_disk(request$url, partial,
                                                   request$handle))
    if (response$status_code != 200L)
        stop(.transferError(x, response$status_code,
                            readBin(partial, "raw", 4096L)))
    if (!file.rename(partial, path))
        stop(sprintf("Cannot write \"%s\".", path), call. = FALSE)
    invisible(path)
}

.transferRequest <- function(x, method, path) {
    request <- kube::kube_handle(
        x@client, method = method,
        path = .proxyPath(x@namespace, x@pod, .jobPort, path),
        content_type = if (method == "PUT") "application/octet-stream",
        accept = "*/*", headers = c("X-BiocKube-Token" = x@token)
    )
    curl::handle_setopt(request$handle, low_speed_limit = 1L,
                        low_speed_time = .stallTimeout)
    request
}

.transfer <- function(x, expr) {
    tryCatch(expr, error = function(e)
        stop(.proxyError(e, x@namespace, x@pod)))
}

.transferError <- function(x, status, content) {
    .podResponseError(list(status_code = status, content = content),
                      x@namespace, x@pod) %||%
        .podError(status, sprintf("unexpected status %d", status),
                  x@namespace, x@pod)
}

#' How far a job has got
#'
#' @param x A `KubeJob`.
#' @param timeout Seconds each request may take, or `NULL`.
#' @return A list with the `state`, the `exit` status of the job's command
#'   when known, and whether the pod `answered`.
#' @noRd
.jobState <- function(x, timeout = NULL) {
    status <- tryCatch(
        .proxyRequest(x@client, x@namespace, x@pod, .jobPort, "status",
                      token = x@token, as = "parsed", timeout = timeout),
        biockube_error_unauthorized = function(e) stop(e),
        kube_error_forbidden = function(e) stop(e),
        biockube_pod_error = function(e) NULL
    )
    if (is.list(status) && is.character(status[["state"]]))
        return(list(state = status[["state"]],
                    exit = .exitStatus(status[["exit_code"]]),
                    answered = TRUE))
    c(.jobOutcome(x, timeout), answered = FALSE)
}

.exitStatus <- function(x) {
    if (is.numeric(x) && length(x) == 1L) as.integer(x) else NA_integer_
}

## Once the pod no longer answers, what the cluster records: the Job's
## conditions and counts, which lag behind the pod, then the pod's phase
## and the exit status of its container, which the runner exits with.
.jobOutcome <- function(x, timeout = NULL) {
    job <- .getObject(x@client, .jobPath(x@namespace, x@name), timeout)
    if (!identical(job[["metadata"]][["uid"]], x@uid))
        return(list(state = "unknown", exit = NA_integer_))
    pod <- .getObject(x@client, .podPath(x@namespace, x@pod), timeout)
    if (!.ownedBy(pod, x@uid, controller = TRUE))
        pod <- NULL
    status <- job[["status"]] %||% list()
    phase <- pod[["status"]][["phase"]] %||% ""
    state <- if (!is.null(.conditionIn(status, "Complete")) ||
                 .count(status[["succeeded"]]) > 0L) "succeeded"
        else if (!is.null(.conditionIn(status, "Failed")) ||
                 .count(status[["failed"]]) > 0L) "failed"
        else if (phase == "Succeeded") "succeeded"
        else if (phase == "Failed") "failed"
        else if (phase == "Running") "running"
        else if (phase == "Pending") "waiting"
        else if (.count(status[["active"]]) > 0L) "running"
        else "unknown"
    list(state = state, exit = .containerExit(pod, "job"))
}

.containerExit <- function(pod, container) {
    for (status in pod[["status"]][["containerStatuses"]] %||% list())
        if (identical(status[["name"]], container))
            return(.exitStatus(status[["state"]][["terminated"]][["exitCode"]]))
    NA_integer_
}

.count <- function(x) {
    if (is.numeric(x) && length(x) == 1L && !is.na(x)) as.integer(x) else 0L
}

## Failures to reach the cluster that pass: a lost connection, a timeout,
## throttling or an unavailable server.
.transient <- function(e) {
    status <- e[["status"]]
    inherits(e, c("kube_error_connection", "kube_error_timeout")) ||
        (inherits(e, "kube_error") && length(status) == 1L &&
             !is.na(status) && (status == 429L || status >= 500L))
}

.jobFailure <- function(x, exit) {
    detail <- if (is.na(exit)) "." else sprintf(" with exit status %d.", exit)
    structure(
        class = c("biockube_job_failed", "error", "condition"),
        list(message = sprintf("Job \"%s\" in namespace \"%s\" failed%s",
                               x@name, x@namespace, detail),
             call = NULL, status = exit)
    )
}

.jobTimeout <- function(x, timeout, trouble = NULL) {
    message <- sprintf("Job \"%s\" did not finish within %s seconds.",
                       x@name, format(timeout))
    if (!is.null(trouble))
        message <- paste(message, "The cluster did not answer:",
                         conditionMessage(trouble))
    structure(
        class = c("biockube_job_timeout", "error", "condition"),
        list(message = message, call = NULL)
    )
}

## The pod scripts use base R only, so the runner configuration travels as
## the hexadecimal text of a serialized object.
.encodeConfig <- function(x) {
    paste(serialize(x, NULL), collapse = "")
}
