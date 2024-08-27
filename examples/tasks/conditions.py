import re
import digdag

class Algorithm(object):
    def zlib(self):
        with open("output.Z", "w") as f:
            f.write("zzzzzzzzzz")
        digdag.env.store({"path_zlib": "output.Z"})
        digdag.env.store({"size_zlib": 10})

    def deflate(self):
        with open("output.gz", "w") as f:
            f.write("defldefl")
        digdag.env.store({"path_deflate": "output.gz"})
        digdag.env.store({"size_deflate": 8})

    def bzip2(self):
        with open("output.bzip2", "w") as f:
            f.write("bz2bz2")
        digdag.env.store({"path_deflate": "output.bz2"})
        digdag.env.store({"size_deflate": 6})

    def decide_algorithm(self, **params):
        best_size = None
        best_path = None
        for key in params:
            m = re.match("size_(.*)", key)
            if m:
                if best_size is None or best_size > params[key]:
                    best_size = params[key]
                    best_path = params["path_"+m.group(1)]
        digdag.env.store({"best_path": best_path})

def show_algorithm(best_path):
    print("using file: "+best_path)

