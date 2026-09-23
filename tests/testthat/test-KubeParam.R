## Empties the cached Bioconductor version until the test ends.
localBiocCache <- function(.local_envir = parent.frame())
{
    cached <- .biocCache$bioc
    withr::defer(assign("bioc", cached, envir = .biocCache),
                 envir = .local_envir)
    assign("bioc", NULL, envir = .biocCache)
}

## BiocManager's version map, read by BiocManager from a copy of the
## config.yaml Bioconductor publishes, with `release` and `devel` built
## with the R of this session.
versionMap <- function(release = "3.23", devel = "3.24")
{
    r <- format(getRversion()[, 1:2])
    config <- tempfile(fileext = ".yaml")
    writeLines(c(
        sprintf('release_version: "%s"', release),
        sprintf('devel_version: "%s"', devel),
        "r_ver_for_bioc_ver:",
        '  "3.10": "3.6"',
        sprintf('  "%s": "%s"', c(release, devel), r)
    ), config)
    read <- utils::getFromNamespace(".version_map_get_online", "BiocManager")
    read(paste0("file://", config))
}

test_that("a param keeps what it was given and coerces the rest", {
    param <- KubeParam(
        workers = 4, image = "bioconductor/bioconductor:devel",
        namespace = "bioc", cpu = 2, memory = 8,
        packages = c("DESeq2", "edgeR"), env = c(OMP_NUM_THREADS = "2"),
        idleTimeout = 60, startTimeout = 120, tasks = 3, timeout = 30,
        jobname = "kube", RNGseed = 7
    )
    expect_s4_class(param, "KubeParam")
    expect_identical(bpnworkers(param), 4L)
    expect_identical(param$image, "bioconductor/bioconductor:devel")
    expect_identical(param$namespace, "bioc")
    expect_identical(param$cpu, "2")
    expect_identical(param$memory, "8Gi")
    expect_identical(param$packages, c("DESeq2", "edgeR"))
    expect_identical(param$env, c(OMP_NUM_THREADS = "2"))
    expect_identical(param$idleTimeout, 60L)
    expect_identical(param$startTimeout, 120L)
    expect_identical(bptasks(param), 3L)
    expect_identical(bptimeout(param), 30L)
    expect_identical(bpjobname(param), "kube")
    expect_identical(bpRNGseed(param), 7L)
})

test_that("a param asks for nothing it was not given", {
    param <- KubeParam()
    expect_identical(bpnworkers(param), 2L)
    expect_identical(param$cpu, character())
    expect_identical(param$memory, character())
    expect_identical(param$packages, character())
    expect_identical(param$env, character())
    expect_identical(param$namespace, NA_character_)
    expect_null(param$client)
    expect_identical(param$idleTimeout, 3600L)
    expect_identical(param$startTimeout, 600L)
    expect_false(bpisup(param))
    expect_identical(length(bpbackend(param)), 0L)
})

test_that("a param stays on the cluster when it has one worker", {
    expect_false(bpfallback(KubeParam(workers = 1)))
    expect_true(bpfallback(KubeParam(workers = 1, fallback = TRUE)))
})

test_that("further fields of BiocParallelParam are passed through", {
    param <- KubeParam(resultdir = NA_character_, stop.on.error = FALSE)
    expect_false(bpstopOnError(param))
    expect_error(KubeParam(nonesuch = 1),
                 "names\\(args\\) %in% names\\(prototype\\)")
})

test_that("the image defaults to the Bioconductor version of the session", {
    expect_identical(.defaultImage("quay.io/me/image:1"),
                     "quay.io/me/image:1")
    expect_error(.defaultImage(""), "`image` must be a container image")
    expect_error(KubeParam(image = NA_character_), "container image")

    session <- function(version, status) {
        force(status)
        function() list(version = package_version(version), status = status)
    }
    local_mocked_bindings(.biocVersion = session("3.23", "release"))
    expect_identical(.defaultImage(NULL),
                     "ghcr.io/bioconductor/bioconductor:RELEASE_3_23")
    local_mocked_bindings(.biocVersion = session("3.22", "out-of-date"))
    expect_identical(.defaultImage(NULL),
                     "ghcr.io/bioconductor/bioconductor:RELEASE_3_22")
    local_mocked_bindings(.biocVersion = session("3.24", "devel"))
    expect_identical(.defaultImage(NULL),
                     "ghcr.io/bioconductor/bioconductor:devel")
    expect_identical(KubeParam()$image,
                     "ghcr.io/bioconductor/bioconductor:devel")
    local_mocked_bindings(.biocVersion = session("3.25", "future"))
    expect_identical(.defaultImage(NULL),
                     "ghcr.io/bioconductor/bioconductor:devel")
    local_mocked_bindings(.biocVersion = session("3.24", NA_character_))
    expect_identical(.defaultImage(NULL),
                     "ghcr.io/bioconductor/bioconductor:RELEASE_3_24")

    local_mocked_bindings(.biocVersion = function() NULL)
    expect_error(KubeParam(), "`image` must be given when BiocManager")
    expect_error(KubeParam(), "as when it is not installed")
    expect_identical(KubeParam(image = "me/image:1")$image, "me/image:1")
})

test_that("the Bioconductor version is looked up without the network", {
    skip_if_not_installed("BiocManager")
    withr::local_options(BIOCONDUCTOR_ONLINE_VERSION_DIAGNOSIS = FALSE)
    localBiocCache()

    expect_silent(bioc <- .biocVersion())
    expect_identical(bioc$version, BiocManager::version())
    expect_identical(bioc$status, NA_character_)
    ## An unknown status is looked up again, as the session may be online
    ## by then.
    expect_null(.biocCache$bioc)
    expect_identical(.biocVersion(), bioc)
})

test_that("the status of a version is read for the R of the session", {
    skip_if_not_installed("BiocManager")
    map <- versionMap()
    r <- getRversion()[, 1:2]
    following <- package_version(paste(unlist(r) + 0:1, collapse = "."))

    ## The devel version has a row for this R and one for the next.
    expect_identical(sum(map$Bioc == "3.24"), 2L)
    expect_identical(.biocStatus(map, package_version("3.24")), "devel")
    expect_identical(.biocStatus(map, package_version("3.24"), following),
                     "future")
    expect_identical(.biocStatus(map, package_version("3.23")), "release")
    expect_identical(.biocStatus(map, package_version("3.10"), "3.6"),
                     "out-of-date")
    expect_identical(.biocStatus(map, package_version("3.10")),
                     NA_character_)
    expect_identical(.biocStatus(NULL, package_version("3.24")),
                     NA_character_)
})

test_that("a devel session gets the devel image", {
    skip_if_not_installed("BiocManager")
    map <- versionMap()
    local_mocked_bindings(.version_map = function() map,
                          .package = "BiocManager")
    localBiocCache()

    withr::local_envvar(R_BIOC_VERSION = "3.24")
    expect_identical(.biocVersion()$status, "devel")
    expect_identical(KubeParam()$image,
                     "ghcr.io/bioconductor/bioconductor:devel")

    assign("bioc", NULL, envir = .biocCache)
    withr::local_envvar(R_BIOC_VERSION = "3.23")
    expect_identical(KubeParam()$image,
                     "ghcr.io/bioconductor/bioconductor:RELEASE_3_23")
    expect_identical(.biocCache$bioc$status, "release")
})

test_that("a session whose version BiocManager cannot tell needs an image", {
    skip_if_not_installed("BiocManager")
    sentinel <- utils::getFromNamespace(".version_sentinel", "BiocManager")
    noMap <- utils::getFromNamespace(".VERSION_MAP_SENTINEL", "BiocManager")
    local_mocked_bindings(
        .version_BiocVersion = function()
            sentinel("BiocVersion is not installed"),
        .version_map = function() noMap,
        .package = "BiocManager"
    )
    withr::local_envvar(R_BIOC_VERSION = NA)
    localBiocCache()

    expect_null(.biocVersion())
    expect_error(KubeParam(), "`image` must be given when BiocManager")
    expect_null(.biocCache$bioc)
    expect_identical(KubeParam(image = "me/image:1")$image, "me/image:1")
})

test_that("a param refuses what the cluster would refuse", {
    expect_error(KubeParam(workers = 0), "at least 1")
    expect_error(KubeParam(namespace = "Bioc"), "`namespace` must be a name")
    expect_error(KubeParam(cpu = -1), "`cpu` must be a positive number")
    expect_error(KubeParam(memory = TRUE), "`memory` must be")
    expect_error(KubeParam(idleTimeout = 0), "at least 60 seconds")
    expect_error(KubeParam(idleTimeout = 20), "at least 60 seconds")
    expect_error(KubeParam(startTimeout = -1), "positive number of seconds")
    expect_error(KubeParam(env = "OMP_NUM_THREADS"),
                 "`env` must be uniquely named strings")
    expect_error(KubeParam(env = c(BIOCKUBE_IDLE = "1")),
                 "`env` must not set BIOCKUBE_IDLE")
    expect_error(KubeParam(env = c("OMP THREADS" = "2")),
                 "`env` names must be environment variable names")
    expect_error(KubeParam(client = "https://cluster"),
                 "`client` must be a kube_client")
    expect_error(KubeParam(packages = c("DESeq2", NA)),
                 "'packages' must be package names")
})

test_that("a param shows itself without asking the cluster", {
    param <- KubeParam(workers = 3, memory = 4,
                       client = kubeClient("http://127.0.0.1:1"))
    text <- paste(capture.output(param), collapse = "\n")
    expect_match(text, "class: KubeParam")
    expect_match(text, "bpisup: FALSE; bpnworkers: 3")
    expect_match(text, "image: ghcr.io/bioconductor/bioconductor:")
    expect_match(text, "namespace: client default\n", fixed = TRUE)
    expect_match(text, "cpu: unset; memory: 4Gi")
    expect_match(text, "packages: none\n  env: none", fixed = TRUE)
    expect_match(text, "idleTimeout: 3600; startTimeout: 600")

    param <- KubeParam(namespace = "bioc", packages = letters[1:7],
                       env = c(TOKEN = "s3cret", OMP_NUM_THREADS = "2"),
                       idleTimeout = 120, startTimeout = 60)
    text <- paste(capture.output(param), collapse = "\n")
    expect_match(text, "namespace: bioc\n", fixed = TRUE)
    expect_match(text, "packages: a, b, c, d, e and 2 more", fixed = TRUE)
    expect_match(text, "env: TOKEN, OMP_NUM_THREADS\n", fixed = TRUE)
    expect_no_match(text, "s3cret")
    expect_match(text, "idleTimeout: 120; startTimeout: 60")

    param$backend <- .kubeBackend(NULL, "team", list(), "w-1", 8080L, "t")
    expect_match(paste(capture.output(param), collapse = "\n"),
                 "namespace: team\n", fixed = TRUE)
})

test_that("the number of workers changes only while the pods are stopped", {
    param <- KubeParam()
    bpworkers(param) <- 5
    expect_identical(bpnworkers(param), 5L)
    expect_error(bpworkers(param) <- 0, "at least 1")
    expect_identical(bpnworkers(param), 5L)

    param$backend <- .kubeBackend(NULL, "bioc", list(), c("w-1", "w-2"),
                                  8080L, "token")
    expect_true(bpisup(param))
    expect_error(bpworkers(param) <- 2, "Stop the worker pods")
})

test_that("logging is switched on and off on a param", {
    param <- KubeParam()
    bplog(param) <- TRUE
    bpthreshold(param) <- "WARN"
    expect_true(bplog(param))
    expect_identical(bpthreshold(param), "WARN")
    expect_error(KubeParam(threshold = "LOUD"),
                 "'bpthreshold' must be one of")
})

test_that("a param sends no work from inside a worker pod", {
    param <- KubeParam()
    expect_true(bpschedule(param))
    expect_false(bpfallback(param))
    withr::local_envvar(BIOCKUBE_WORKER = "1")
    expect_false(bpschedule(param))
    expect_true(bpfallback(param))
})

test_that("a param used inside a worker pod runs serially there", {
    ## Any request to this client fails at once.
    client <- kubeClient("http://127.0.0.1:1")
    fresh <- KubeParam(namespace = "bioc", client = client)
    started <- KubeParam(namespace = "bioc", client = client)
    started$backend <- .kubeBackend(client, "bioc",
                                    list(configMap = "biockube-1"),
                                    c("biockube-1-1", "biockube-1-2"),
                                    8080L, "token")
    withr::local_envvar(BIOCKUBE_WORKER = "1")

    expect_identical(bplapply(1:3, function(i) i * 2L, BPPARAM = fresh),
                     list(2L, 4L, 6L))
    expect_false(bpisup(fresh))
    expect_false(bpisup(started))
    expect_identical(bplapply(1:2, function(i) -i, BPPARAM = started),
                     list(-1L, -2L))
    expect_identical(bpstop(started), started)
    expect_identical(length(bpbackend(started)), 2L)
})

test_that("stopping a param that never started does nothing", {
    param <- KubeParam(client = kubeClient("http://127.0.0.1:1"))
    expect_identical(bpstop(param), param)
    expect_false(bpisup(param))
})

test_that("versions are compared up to their second part", {
    expect_true(.sameSeries("4.5.1", "4.5.0"))
    expect_true(.sameSeries("1.47.0", "1.47.3"))
    expect_false(.sameSeries("4.5.1", "4.4.3"))
    expect_false(.sameSeries("1.46.0", "1.47.0"))
    expect_false(.sameSeries(NULL, "4.5.1"))
    expect_false(.sameSeries(NA_character_, "4.5.1"))
    expect_identical(.series("4.5"), "4.5")
    expect_identical(.series("4"), "4")
})
