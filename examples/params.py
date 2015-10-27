import digdag

class SetMyParam(digdag.BaseTask):
    def run(self):
        self.carry_params["my_param"] = {"key": "value"}

class ShowMyParam(digdag.BaseTask):
    def run(self):
        print "my_param = " + str(self.params["my_param"])

