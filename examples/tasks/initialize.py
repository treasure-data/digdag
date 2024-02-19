import digdag

class InstanceVariable:
    def __init__(self, arg1):
        self.arg1 = arg1

    def step1(self):
        print(self.arg1)
        print(digdag.env.params['arg1'])
        print(digdag.env.params['arg2'])
        print('self.arg1 is modified!!!')
        self.arg1 = 'var3'
        print(self.arg1)

    def step2(self):
        print('Instance variables is initialized by each tasks.')
        print(self.arg1)
