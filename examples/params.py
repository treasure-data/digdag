from tasks import BaseTask

class SetMyParam(BaseTask):
    def run(self):
        self.carry_params["my_param"] = {"key": "value"}

class ShowMyParam(BaseTask):
    def run(self):
        print "my_param = " + str(self.params["my_param"])

