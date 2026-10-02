# BiocKube

BiocKube runs work from an R session on a Kubernetes cluster.
`KubeParam()` is a BiocParallel backend whose workers are pods, and
`submitJob()` runs a job declared with BiocJobs as a Kubernetes Job.

## Installation

BiocKube needs the kube package and, for `submitJob()`, a BiocJobs that
provides `kubernetesJob()`:

```r
install.packages(c("BiocManager", "remotes"))
BiocManager::install(c("BiocParallel", "BiocStyle"))
remotes::install_github("almahmoud/kube")
remotes::install_github("almahmoud/BiocKube", build_vignettes = TRUE)
remotes::install_github("almahmoud/BiocJobs", subdir = "BiocJobs")   # for submitJob()
```

## Usage

Pods and jobs run in the namespace of the current kubeconfig context
unless `namespace` or `client` says otherwise.

```r
library(BiocKube)

param <- KubeParam(workers = 4, cpu = 1, memory = 4)  # per pod: 1 core, 4 GiB
bpstart(param)
res <- bplapply(1:8, function(i) Sys.info()[["nodename"]], BPPARAM = param)
unique(unlist(res))   # the names of the 4 pods
bpstop(param)
```

A job's local input files are uploaded to its pod; `http`, `https` and
`ftp` URLs are downloaded by the pod. `values` names the job's inputs and
options. `deseq2-job.R` (below) downloads this job and its test data.

```r
spec <- BiocJobs::readJob("deseq2-differential-expression.yaml")
job <- submitJob(spec, values = list(
    counts = "counts.tsv", coldata = "coldata.tsv",
    contrast_factor = "condition",
    contrast_numerator = "treated", contrast_denominator = "control"
))
waitJob(job)
jobOutputs(job, dir = "results")
deleteJob(job)
```

Both scripts that come with the package run on the cluster of the current
kubeconfig context: `bplapply.R` (worker pods, `bptry()`, `BPREDO`) and
`deseq2-job.R` (the BiocJobs DESeq2 job; downloads its test data from
GitHub).

```r
source(system.file("examples", "bplapply.R", package = "BiocKube"))
source(system.file("examples", "deseq2-job.R", package = "BiocKube"))
```

## What the cluster needs

- Images with R. `KubeParam()` defaults to the Bioconductor image of the
  session's Bioconductor version; pods install BiocParallel and `packages`
  at startup when the image lacks them. Job images need BiocJobs, the
  package declaring the job, and `curl` for URL inputs.
- A namespace that does not enforce the `restricted` Pod Security
  Standard: pods run as the image's user (root in the Bioconductor images)
  without `runAsNonRoot`.

Permissions and security notes: vignette sections "What runs in the
cluster" and "Security".

## Documentation

`vignette("BiocKube")`, `?KubeParam`, `?submitJob` and `?jobStatus`.

## License

Artistic-2.0
