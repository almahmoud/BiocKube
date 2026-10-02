## Tests against a live cluster run only when KUBE_TEST_KUBECONFIG names a
## kubeconfig file. They create their own namespaces and delete only those.

clusterClient <- function() {
    skip_on_cran()
    skip_on_bioc()
    path <- Sys.getenv("KUBE_TEST_KUBECONFIG")
    skip_if(!nzchar(path), "KUBE_TEST_KUBECONFIG is not set")
    kube::kube_client(kubeconfig = path)
}

testNamespaceLabel <- c(`app.kubernetes.io/managed-by` = "testthat")

localTestNamespace <- function(client, .local_envir = parent.frame()) {
    name <- basename(tempfile("biockube-test-"))
    body <- list(
        apiVersion = "v1",
        kind = "Namespace",
        metadata = list(name = name, labels = as.list(testNamespaceLabel))
    )
    kube::kube_request(client, "POST", "/api/v1/namespaces", body = body)
    withr::defer(deleteTestNamespace(client, name), envir = .local_envir)
    name
}

deleteTestNamespace <- function(client, name) {
    stopifnot(startsWith(name, "biockube-test-"))
    path <- paste0("/api/v1/namespaces/", name)
    ns <- kube::kube_request(client, "GET", path)
    labels <- ns$metadata$labels
    stopifnot(identical(labels[[names(testNamespaceLabel)]],
                        testNamespaceLabel[[1L]]))
    kube::kube_request(client, "DELETE", path)
    invisible(name)
}
