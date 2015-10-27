import digdag

class GenerateData(digdag.BaseTask):
    def run(self):
        print "generating 'generated.csv'..."
        with open("generated.csv", "w") as f:
            f.write("ok")

class CheckGenerated(digdag.BaseTask):
    def run(self):
        print "checking 'generated.csv'..."
        with open("generated.csv", "r") as f:
            data = f.read()
        if len(data) < 2:
            raise Exception("Output data is too small")


class ComplexPlan(digdag.BaseTask):
    def run(self):
        self.carry_params["path"] = "complex.csv"

class ComplexGenerate(digdag.BaseTask):
    def run(self):
        print "generating "+self.params["path"]
        with open(self.params["path"], "w") as f:
            f.write("ok")

class CheckComplexGenerated(digdag.BaseTask):
    def run(self):
        print "checking "+self.params["path"]
        with open(self.params["path"], "r") as f:
            data = f.read()
        if len(data) < 2:
            raise Exception("Output data is too small")

