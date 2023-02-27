from mrjob.job import MRJob
import re

class PagrankJob_Init(MRJob):

    def configure_args(self):
        super(PagrankJob_Init, self).configure_args()
        self.add_passthru_arg(
            '--nodecount')


    def mapper(self, _, line):
        y, outlinks = line.split("\t")
        outlinks=re.sub("\[|'|\]","",outlinks)
        outlinks=outlinks.split(",")
        for i in outlinks:
            if int(i)!=-1:
                pr=1/int(self.options.nodecount)
                yield int(i), pr/len(outlinks)
        yield int(y), outlinks
           

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
    PagrankJob_Init.run()