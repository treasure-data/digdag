from tasks import BaseTask

class GenerateData(BaseTask):
    def run(self):
        print "generating 'generated.csv'..."
        with open("generated.csv", "w") as f:
            f.write("ok")

class CheckGenerated(BaseTask):
    def run(self):
        print "checking 'generated.csv'..."
        with open("generated.csv", "r") as f:
            data = f.read()
        if len(data) < 2:
            raise Exception("Output data is too small")


class ComplexPlan(BaseTask):
    def run(self):
        self.carry_params["path"] = "complex.csv"

class ComplexGenerate(BaseTask):
    def run(self):
        print "generating "+self.params["path"]
        with open(self.params["path"], "w") as f:
            f.write("ok")

class CheckComplexGenerated(BaseTask):
    def run(self):
        print "checking "+self.params["path"]
        with open(self.params["path"], "r") as f:
            data = f.read()
        if len(data) < 2:
            raise Exception("Output data is too small")

