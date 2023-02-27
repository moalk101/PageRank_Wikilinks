from mrjob.job import MRJob
import re

class PageRankjob(MRJob):

    def configure_args(self):
        super(PageRankjob, self).configure_args()
        self.add_passthru_arg(
            '--nodecount')


    def mapper(self, _, line):
        idpr, outlinks=line.split("\t")
        idpr=re.sub("\[|'|\]","",idpr)
        id, pr=idpr.split(",")
        outlinks=re.sub("\[|\"|\]","",outlinks)
        outlinks=outlinks.split(",")
        for i in outlinks:
            if int(i)!=-1:
                yield int(i), float(pr)/len(outlinks)
        yield int(id), outlinks
           

    def reducer(self, key, values):
        beta=0.8
        pr=(1-beta)/int(self.options.nodecount)
        outlinks=[]
    
        for i in values:
            if type(i) is list:
                outlinks=i
            else:
                pr +=beta*i
        
        yield (key,pr) , outlinks

if __name__ == '__main__':
    PageRankjob.run()