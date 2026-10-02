# BiocKube 0.99.0

* `KubeParam()`: a BiocParallel backend whose workers are Kubernetes pods.
* `submitJob()` runs a BiocJobs job as a Kubernetes Job. `jobStatus()`,
  `waitJob()`, `jobLogs()`, `jobOutputs()`, `KubeJob()`, `listJobs()` and
  `deleteJob()` follow, collect and remove jobs.
* Example scripts in `system.file("examples", package = "BiocKube")`.
