from __future__ import print_function
import re
import digdag

class Algorithm(digdag.BaseTask):
    def zlib(self):
        with open("output.Z", "w") as f:
            f.write("zzzzzzzzzz")
        self.carry_params["path_zlib"] = "output.Z"
        self.carry_params["size_zlib"] = 10

    def deflate(self):
        with open("output.gz", "w") as f:
            f.write("defldefl")
        self.carry_params["path_deflate"] = "output.gz"
        self.carry_params["size_deflate"] = 8

    def bzip2(self):
        with open("output.bzip2", "w") as f:
            f.write("bz2bz2")
        self.carry_params["path_deflate"] = "output.bz2"
        self.carry_params["size_deflate"] = 6

    def decide_algorithm(self, **params):
        best_size = None
        best_path = None
        for key in params:
            m = re.match("size_(.*)", key)
            if m:
                if best_size is None or best_size > params[key]:
                    best_size = params[key]
                    best_path = params["path_"+m.group(1)]
        self.carry_params["best_path"] = best_path

def show_algorithm(best_path):
    print("using file: "+best_path)

