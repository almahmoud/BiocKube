## The cluster side of a running param lives in its backend; a param that is
## not running holds the null backend, which is what bpisup() tests.

setOldClass("KubeBackend")

.kubeBackendNULL <- function() {
    structure(list(), class = "KubeBackend")
}

#' @noRd
#' @exportS3Method
length.KubeBackend <- function(x) {
    length(x$pods)
}

.KubeParam_prototype <- c(
    list(
        backend = .kubeBackendNULL(),
        image = NA_character_,
        namespace = NA_character_,
        cpu = character(),
        memory = character(),
        packages = character(),
        env = character(),
        idleTimeout = 3600L,
        startTimeout = 600L,
        client = NULL
    ),
    .BiocParallelParam_prototype
)

.KubeParam <- setRefClass("KubeParam",
    contains = "BiocParallelParam",
    fields = list(
        backend = "KubeBackend",
        image = "character",
        namespace = "character",
        cpu = "character",
        memory = "character",
        packages = "character",
        env = "character",
        idleTimeout = "integer",
        startTimeout = "integer",
        client = "ANY"
    ),
    methods = list(
        show = function() {
            callSuper()
            cat("  image: ", image,
                "\n  namespace: ", .shownNamespace(.self),
                "\n  cpu: ", .orUnset(cpu),
                "; memory: ", .orUnset(memory),
                "\n  packages: ", .shownList(packages),
                "\n  env: ", .shownList(names(env)),
                "\n  idleTimeout: ", idleTimeout,
                "; startTimeout: ", startTimeout, "\n", sep = "")
        }
    )
)

#' BiocParallel workers as Kubernetes pods
#'
#' A `KubeParam` runs each BiocParallel worker in a pod of its own.
#' [`bpstart()`][BiocParallel::bpstart] creates the pods,
#' [`bplapply()`][BiocParallel::bplapply] and the other BiocParallel
#' functions send them tasks through the API server's pod proxy, and
#' [`bpstop()`][BiocParallel::bpstop] deletes them. `KubeParam` is a
#' reference class that extends `BiocParallelParam`.
#'
#' Tasks carry the function, its arguments and the session's global
#' options in R's serialization format, so the image should have the
#' session's versions of R, BiocParallel and the packages the function
#' uses. `bpstart()` warns when the pods run other versions of R or
#' BiocParallel.
#'
#' Anyone who can create or modify pods in the namespace can pose as a
#' worker, whose results the session unserializes, so use only namespaces
#' whose pod creators you trust.
#'
#' @param workers Number of worker pods.
#' @param image Container image of the pods. Defaults to
#'   `ghcr.io/bioconductor/bioconductor:RELEASE_3_23` in a Bioconductor 3.23
#'   session and `:devel` in a devel session. When BiocManager cannot tell
#'   devel from release, as offline, the `RELEASE_` tag is used, so give
#'   `image` in an offline devel session. Required when BiocManager is not
#'   installed.
#' @param namespace Namespace of the pods. `NULL` uses the client's
#'   namespace.
#' @param cpu,memory CPU and memory each pod requests and is limited to: a
#'   number of cores, a number of gibibytes, or a Kubernetes quantity such
#'   as `"500m"` or `"4Gi"`. `NULL` sets no request or limit.
#' @param packages Packages the pods install with BiocManager at startup
#'   when the image lacks them.
#' @param env Named strings added to each pod's environment. Names must be
#'   environment variable names not starting with `BIOCKUBE_`. The values
#'   are stored in plain text in the pod spec, so they must not be secrets.
#' @param idleTimeout Seconds without a request after which a pod exits; at
#'   least 60. Pods stay up while running a task and during a run. Uploading
#'   one task, or one `ITER()` call of [`bpiterate()`][BiocParallel::bpiterate],
#'   must take less than three quarters of it.
#' @param startTimeout Seconds [`bpstart()`][BiocParallel::bpstart] waits
#'   for the pods to be scheduled and ready.
#' @param client A `kube_client`, or `NULL` for the default client at the
#'   time of each request.
#' @param ... Further fields of `BiocParallelParam`, such as
#'   `resultdir`.
#' @param tasks,stop.on.error,progressbar,RNGseed,timeout Arguments of
#'   `BiocParallelParam`; see
#'   [BiocParallel::BiocParallelParam].
#' @param exportglobals,exportvariables,log,threshold,logdir Arguments of
#'   `BiocParallelParam`; see
#'   [BiocParallel::BiocParallelParam].
#' @param jobname,force.GC Arguments of `BiocParallelParam`; see
#'   [BiocParallel::BiocParallelParam].
#' @param fallback Whether BiocParallel may use a `SerialParam` instead of a
#'   param that is not started. Defaults to `FALSE`, so even one worker runs
#'   on the cluster.
#'
#' @return A `KubeParam` object.
#'
#' @seealso [`bpstart()`][BiocParallel::bpstart] and the other methods in
#'   [KubeParam-methods], and the script
#'   `system.file("examples", "bplapply.R", package = "BiocKube")`.
#'
#' @examples
#' image <- "ghcr.io/bioconductor/bioconductor:RELEASE_3_23"
#' param <- KubeParam(workers = 4, memory = 8, image = image)
#' param
#'
#' try(KubeParam(idleTimeout = 30, image = image))
#'
#' @examplesIf interactive()
#' ## Needs access to a cluster.
#' param <- KubeParam(workers = 4, memory = 8)
#' bpstart(param)
#' bplapply(1:8, function(i) Sys.info()[["nodename"]], BPPARAM = param)
#' bpstop(param)
#' @aliases KubeParam-class
#' @exportClass KubeParam
#' @export
KubeParam <- function(workers = 2L, image = NULL, namespace = NULL,
                      cpu = NULL, memory = NULL, packages = character(),
                      env = character(), idleTimeout = 3600L,
                      startTimeout = 600L, client = NULL, ...,
                      tasks = 0L, stop.on.error = TRUE, progressbar = FALSE,
                      RNGseed = NULL, timeout = NA_integer_,
                      exportglobals = TRUE, exportvariables = TRUE,
                      log = FALSE, threshold = "INFO",
                      logdir = NA_character_, jobname = "BPJOB",
                      force.GC = FALSE, fallback = FALSE) {
    if (!is.null(RNGseed))
        RNGseed <- as.integer(RNGseed)
    prototype <- .prototype_update(
        .KubeParam_prototype,
        workers = as.integer(workers),
        image = .defaultImage(image),
        namespace = .namespaceOrNA(namespace),
        cpu = .quantityOrNone(cpu, "cpu"),
        memory = .quantityOrNone(memory, "memory", "Gi"),
        packages = as.character(packages),
        env = .envStrings(env),
        idleTimeout = as.integer(idleTimeout),
        startTimeout = as.integer(startTimeout),
        client = .checkClient(client),
        tasks = as.integer(tasks),
        stop.on.error = as.logical(stop.on.error),
        progressbar = as.logical(progressbar),
        RNGseed = RNGseed,
        timeout = as.integer(timeout),
        exportglobals = as.logical(exportglobals),
        exportvariables = as.logical(exportvariables),
        log = as.logical(log),
        threshold = as.character(threshold),
        logdir = as.character(logdir),
        jobname = as.character(jobname),
        force.GC = as.logical(force.GC),
        fallback = as.logical(fallback),
        ...
    )
    param <- do.call(.KubeParam, prototype)
    validObject(param)
    param
}

setValidity("KubeParam", function(object) {
    msg <- NULL
    workers <- object$workers
    if (!is.numeric(workers) || length(workers) != 1L || is.na(workers) ||
        workers < 1L)
        msg <- c(msg, "'workers' must be a number of pods, at least 1")
    if (length(object$image) != 1L || is.na(object$image) ||
        !nzchar(object$image))
        msg <- c(msg, "'image' must be a container image")
    if (length(object$namespace) != 1L)
        msg <- c(msg, "'namespace' must be a namespace name or NA")
    if (!.isSeconds(object$idleTimeout) ||
        object$idleTimeout < .minIdleTimeout)
        msg <- c(msg, sprintf("'idleTimeout' must be at least %d seconds",
                              .minIdleTimeout))
    if (!.isSeconds(object$startTimeout))
        msg <- c(msg, "'startTimeout' must be a positive number of seconds")
    if (anyNA(object$packages) || !all(nzchar(object$packages)))
        msg <- c(msg, "'packages' must be package names")
    if (length(object$env) &&
        (is.null(names(object$env)) || anyNA(object$env) ||
         !all(.isEnvName(names(object$env))) ||
         any(startsWith(names(object$env), "BIOCKUBE_"))))
        msg <- c(msg, paste("'env' must be strings named after environment",
                            "variables, none starting with BIOCKUBE_"))
    if (!is.null(object$client) && !inherits(object$client, "kube_client"))
        msg <- c(msg, "'client' must be a kube_client or NULL")
    if (is.null(msg)) TRUE else msg
})

.isSeconds <- function(x) {
    is.numeric(x) && length(x) == 1L && !is.na(x) && x > 0L
}

## A pod counts a parked result poll as a request, and during a run every
## pod is sent one each quarter of the idle timeout; a minute leaves room
## for answers that are slow to come.
.minIdleTimeout <- 60L

## Bioconductor tags the image of a release after its branch, RELEASE_3_23
## for 3.23; the version in development only has the devel image.
.defaultImage <- function(image) {
    if (!is.null(image)) {
        if (!is.character(image) || length(image) != 1L || is.na(image) ||
            !nzchar(image))
            stop("`image` must be a container image, or NULL.", call. = FALSE)
        return(image)
    }
    bioc <- .biocVersion()
    if (is.null(bioc))
        stop("`image` must be given when BiocManager cannot tell the ",
             "Bioconductor version of the session, as when it is not ",
             "installed.", call. = FALSE)
    if (bioc$status %in% c("devel", "future"))
        return("ghcr.io/bioconductor/bioconductor:devel")
    parts <- unlist(unclass(bioc$version))
    sprintf("ghcr.io/bioconductor/bioconductor:RELEASE_%d_%d",
            parts[1L], parts[2L])
}

.biocCache <- new.env(parent = emptyenv())

#' The Bioconductor version of the session
#'
#' @return `NULL` when BiocManager is not installed or reports no version,
#'   else a list with the `version` BiocManager reports and its `status`
#'   from `.biocStatus()`. The version map is read from the web with a short
#'   timeout, unless BiocManager is set up to work offline, and a known
#'   status is kept for the rest of the session.
#' @noRd
.biocVersion <- function() {
    if (!is.null(.biocCache$bioc))
        return(.biocCache$bioc)
    if (!requireNamespace("BiocManager", quietly = TRUE))
        return(NULL)
    quietly <- function(expr) tryCatch(
        suppressMessages(suppressWarnings(expr)), error = function(e) NULL
    )
    version <- quietly(BiocManager::version())
    ## BiocManager answers a version of NA when it cannot tell.
    if (!inherits(version, "package_version") || length(version) != 1L ||
        is.na(version))
        return(NULL)
    old <- options(timeout = 5)
    on.exit(options(old))
    map <- quietly(utils::getFromNamespace(".version_map", "BiocManager")())
    bioc <- list(version = version, status = .biocStatus(map, version))
    if (!is.na(bioc$status))
        .biocCache$bioc <- bioc
    bioc
}

#' Status of a Bioconductor version
#'
#' @param map BiocManager's version map, a data frame with columns `Bioc`,
#'   `R` and `BiocStatus` and a row per pair of versions. The version in
#'   development has two rows: `"devel"` for the R it is built with, and
#'   `"future"` for the next R.
#' @param version A Bioconductor version.
#' @param r The R version of the session.
#' @return `"release"`, `"devel"`, `"out-of-date"` or `"future"`, or `NA`
#'   when the map is not available, as offline, or has no row for the pair.
#' @noRd
.biocStatus <- function(map, version, r = getRversion()) {
    if (!is.data.frame(map) ||
        !all(c("Bioc", "R", "BiocStatus") %in% names(map)))
        return(NA_character_)
    r <- package_version(r)[, 1:2]
    status <- as.character(map$BiocStatus[map$Bioc == version & map$R == r])
    if (length(status) == 1L) status else NA_character_
}

.namespaceOrNA <- function(namespace) {
    if (is.null(namespace))
        return(NA_character_)
    .dnsName(namespace, "namespace")
    namespace
}

.quantityOrNone <- function(x, arg, unit = "") {
    value <- .quantity(x, arg, unit)
    if (is.null(value)) character() else value
}

.envStrings <- function(env) {
    if (!length(env))
        return(character())
    unlist(.envVariables(env))
}

.checkClient <- function(client) {
    if (!is.null(client) && !inherits(client, "kube_client"))
        stop("`client` must be a kube_client, or NULL for the default one.",
             call. = FALSE)
    client
}

.orNull <- function(x) {
    if (length(x)) x else NULL
}

.orUnset <- function(x) {
    if (length(x)) x else "unset"
}

.shownNamespace <- function(x) {
    if (bpisup(x))
        return(bpbackend(x)$namespace)
    if (is.na(x$namespace)) "client default" else x$namespace
}

.shownList <- function(x, most = 5L) {
    if (!length(x))
        return("none")
    shown <- paste(utils::head(x, most), collapse = ", ")
    if (length(x) > most)
        shown <- sprintf("%s and %d more", shown, length(x) - most)
    shown
}
