from tasks import BaseTask

class RequiredArgument(BaseTask):
    def init(self, required1, required2):
        self.required1 = required1
        self.required2 = required2

    def run(self):
        print "required1 = " + str(self.required1)
        print "required2 = " + str(self.required2)


class OptionalArgument(BaseTask):
    def init(self, optional1=None, optional2="default"):
        self.optional1 = optional1
        self.optional2 = optional2

    def run(self):
        print "optional1 = " + str(self.optional1)
        print "optional2 = " + str(self.optional2)


class MixedArgument(BaseTask):
    def init(self, arg1, arg2=None, arg3=None):
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3

    def run(self):
        print "arg1 = " + str(self.arg1)
        print "arg2 = " + str(self.arg2)
        print "arg3 = " + str(self.arg3)

