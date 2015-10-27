import re
import digdag

class AlgorithmZlib(digdag.BaseTask):
    def run(self):
        with open("output.Z", "w") as f:
            f.write("zzzzzzzzzz")
        self.carry_params["path_zlib"] = "output.Z"
        self.carry_params["size_zlib"] = 10

class AlgorithmDeflate(digdag.BaseTask):
    def run(self):
        with open("output.gz", "w") as f:
            f.write("defldefl")
        self.carry_params["path_deflate"] = "output.gz"
        self.carry_params["size_deflate"] = 8

class AlgorithmBzip2(digdag.BaseTask):
    def run(self):
        with open("output.bzip2", "w") as f:
            f.write("bz2bz2")
        self.carry_params["path_deflate"] = "output.bz2"
        self.carry_params["size_deflate"] = 6

class TakeAlgorithm(digdag.BaseTask):
    def run(self):
        smallest_size = None
        smallest_path = None
        for key in self.params:
            m = re.match("size_(.*)", key)
            if m:
                if smallest_size is None or smallest_size > self.params[key]:
                    smallest_size = self.params[key]
                    smallest_path = self.params["path_"+m.group(1)]
        self.carry_params["smallest_path"] = smallest_path

class ShowAlgorithm(digdag.BaseTask):
    def run(self):
        print "using file: "+self.params["smallest_path"]

