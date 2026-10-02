## Runs BiocParallel work on two pods in the namespace of the current
## kubeconfig context.
library(BiocKube)

param <- KubeParam(workers = 2, cpu = 0.5, memory = 1)
print(param)

## Creates the pods and waits until they are ready.
bpstart(param)

hosts <- bplapply(1:4, function(i) Sys.info()[["nodename"]], BPPARAM = param)
print(table(unlist(hosts)))

## bptry() returns the results together with the errors.
res <- bptry(bplapply(list(1, "a", 3), log, BPPARAM = param))
print(bpok(res))
print(conditionMessage(res[[2]]))

## BPREDO evaluates only the elements that failed.
res <- bplapply(list(1, 2, 3), log, BPREDO = res, BPPARAM = param)
print(unlist(res))

bpstop(param)
