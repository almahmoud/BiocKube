## Runs the DESeq2 example job of BiocJobs, with its test data, in the
## namespace of the current kubeconfig context.
library(BiocKube)

base <- paste0("https://raw.githubusercontent.com/almahmoud/BiocJobs/",
               "main/examples/DESeq2")
files <- c("inst/biocjobs/deseq2-differential-expression.yaml",
           "inst/biocjobs/scripts/deseq2-differential-expression.R",
           "test-data/counts.tsv", "test-data/coldata.tsv")
dir <- tempfile("deseq2-")
for (file in files) {
    dir.create(dirname(file.path(dir, file)), recursive = TRUE,
               showWarnings = FALSE)
    download.file(paste(base, file, sep = "/"), file.path(dir, file),
                  mode = "wb", quiet = TRUE)
}

spec <- BiocJobs::readJob(file.path(dir, files[1]))

## Uploads the two tables and starts the job.
job <- submitJob(spec, values = list(
    counts = file.path(dir, "test-data/counts.tsv"),
    coldata = file.path(dir, "test-data/coldata.tsv"),
    contrast_factor = "condition",
    contrast_numerator = "treated",
    contrast_denominator = "control"
))
print(job)

## The Job is deleted even when a step fails.
tryCatch({
    waitJob(job, timeout = 1800)
    outputs <- jobOutputs(job, dir = file.path(dir, "outputs"))
    print(outputs)
    print(head(read.delim(outputs[["results"]])))
}, finally = deleteJob(job))
