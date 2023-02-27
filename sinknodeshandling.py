from mrjob.job import MRJob
import re

class SinkNodesHandling(MRJob):

    def configure_args(self):
        super(SinkNodesHandling, self).configure_args()
        self.add_passthru_arg(
            '--sprsum')


    def mapper(self, _, line):
        idpr, outlinks=line.split("\t")
        idpr=re.sub("\[|'|\]","",idpr)
        id, pr=idpr.split(",")
        outlinks=re.sub("\[|\"|\]","",outlinks)
        outlinks=outlinks.split(",")
        
        yield (int(id), float(pr)), outlinks
                
    def reducer(self, key, values):
        id,pr = key
        pr +=float(self.options.sprsum)
        outlinks=[]
        for i in values:
            outlinks=i
        yield (id, pr), outlinks

if __name__ == '__main__':
    SinkNodesHandling.run()