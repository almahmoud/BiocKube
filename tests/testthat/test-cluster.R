## End-to-end tests against the cluster named by KUBE_TEST_KUBECONFIG.

test_that("bplapply() runs on worker pods and cleans up after itself", {
    client <- clusterClient()
    namespace <- localTestNamespace(client)
    param <- KubeParam(workers = 2L, namespace = namespace, client = client,
                       cpu = "250m", memory = "1Gi", startTimeout = 1200L)

    bpstart(param)
    expect_true(bpisup(param))
    pods <- kube::kube_get("pods", namespace = namespace, output = "object",
                           client = client)
    expect_length(pods, 2L)

    result <- bplapply(1:6, function(i) {
        c(value = i^2, host = Sys.info()[["nodename"]])
    }, BPPARAM = param)
    expect_identical(vapply(result, `[[`, "", "value"),
                     as.character((1:6)^2))
    hosts <- unique(vapply(result, `[[`, "", "host"))
    expect_true(all(hosts %in% vapply(pods, function(p) p$metadata$name, "")))

    failed <- bptry(bplapply(1:3, function(i) {
        if (i == 2L) stop("no even numbers")
        i
    }, BPPARAM = param))
    expect_identical(bpok(failed), c(TRUE, FALSE, TRUE))
    expect_match(conditionMessage(failed[[2L]]), "no even numbers")

    bpstop(param)
    expect_false(bpisup(param))
    kube::kube_wait("pods", selector = c(`app.kubernetes.io/managed-by` =
                                         "BiocKube"),
                    deleted = TRUE, namespace = namespace, timeout = 120,
                    client = client)
    left <- kube::kube_get("pods", namespace = namespace, output = "object",
                           client = client)
    expect_length(left, 0L)
})
