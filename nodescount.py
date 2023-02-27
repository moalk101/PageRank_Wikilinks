from mrjob.job import MRJob

class NodesCount(MRJob):

    def mapper(self, _, line):
        c1,c2=line.split("\t")
        yield "nodes" , c1

    def reducer(self, key, values):
        yield key, len(set(values))

if __name__ == '__main__':
    nodescount.run()