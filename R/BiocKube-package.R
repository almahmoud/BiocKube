#' @description
#' Runs work from an R session on a Kubernetes cluster.
#' [KubeParam()] is a BiocParallel backend whose workers
#' are pods, and [submitJob()] runs a job declared with
#' BiocJobs as a Kubernetes Job.
#'
#' @keywords internal
#' @import BiocParallel
#' @import methods
#' @importFrom utils packageVersion
"_PACKAGE"
