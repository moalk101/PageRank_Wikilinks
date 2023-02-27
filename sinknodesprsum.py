from mrjob.job import MRJob
import re

class SinkNodesprsum(MRJob):

    def configure_args(self):
        super(SinkNodesprsum, self).configure_args()
        self.add_passthru_arg(
            '--nodecount')


    def mapper(self, _, line):
        idpr, outlinks=line.split("\t")
        idpr=re.sub("\[|'|\]","",idpr)
        id, pr=idpr.split(",")
        outlinks=re.sub("\[|\"|\]","",outlinks)
        outlinks=outlinks.split(",")
        for i in outlinks:
            if int(i)==-1 and len(outlinks)==1:
                yield "sink",float(pr)/int(self.options.nodecount)
                
    def reducer(self, key, values):
        beta=0.8
        yield key,beta*(sum(values))

if __name__ == '__main__':
    SinkNodesprsum.run()