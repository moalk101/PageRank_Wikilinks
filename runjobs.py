from pagerank import PageRankjob
from pagerankjob_init import PagrankJob_Init
from pageranksumjob import PageRankSumJob
from sinknodeshandling import SinkNodesHandling
from sinknodesprsum import SinkNodesprsum
from nodescount import NodesCount
import sys

INPUT_FILE_INIT="preprocessed.csv"
nodecount=""

nodecount_job=NodesCount(args=["preprocessed.csv"])

with nodecount_job.make_runner() as runner:
    runner.run()
    for key , value in nodecount_job.parse_output(runner.cat_output()):
        nodecount=str(value)
        break

pagerankjob_init = PagrankJob_Init(args=[INPUT_FILE_INIT,"--nodecount",nodecount])
runner_pinit=pagerankjob_init.make_runner()
runner_pinit.run()
input_sinkAndPagerankjob=runner_pinit._output_dir

pagerankjob=PageRankjob(args=[input_sinkAndPagerankjob,"--nodecount",nodecount])

for i in range(0):

    sinknodes_prsum=SinkNodesprsum(args=[input_sinkAndPagerankjob,"--nodecount",nodecount])
    runner_prsum=sinknodes_prsum.make_runner()
    runner_prsum.run()
    sprsum=0
    for k, v in sinknodes_prsum.parse_output(runner_prsum.cat_output()):
        sprsum=str(v)

    sinknodes_handling=SinkNodesHandling(args=[input_sinkAndPagerankjob,"--sprsum",sprsum])
    runner_handling=sinknodes_handling.make_runner()
    runner_handling.run()
    input_sinkAndPagerankjob=runner_handling._output_dir

    pagerankjob=PageRankjob(args=[input_sinkAndPagerankjob,"--nodecount",nodecount])
    pagerankjob_runner=pagerankjob.make_runner()
    pagerankjob_runner.run()
    input_sinkAndPagerankjob = pagerankjob_runner._output_dir



sumprsjob = PageRankSumJob(args=[input_sinkAndPagerankjob])
sum=0
with sumprsjob.make_runner() as runner:
    runner.run()
    for key , value in sumprsjob.parse_output(runner.cat_output()):
        sum=str(value)
        break
sys.stdout.write("sum pageranks= "+str(sum)+"\n")

with pagerankjob.make_runner() as runner:
    runner.run()
    for key , value in pagerankjob.parse_output(runner.cat_output()):
        sys.stdout.write(str(key)+"\n")
    