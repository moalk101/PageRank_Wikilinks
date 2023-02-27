from mrjob.job import MRJob
import re

class PageRankSumJob(MRJob):

    def configure_args(self):
        super(PageRankSumJob, self).configure_args()
        self.add_passthru_arg(
            '--nodecount')


    def mapper(self, _, line):
        idpr, outlinks=line.split("\t")
        idpr=re.sub("\[|'|\]","",idpr)
        id, pr=idpr.split(",")
        
        yield "sum", float(pr)

    def reducer(self, key, values):
        yield key,sum(values)

if __name__ == '__main__':
    PageRankSumJob.run()