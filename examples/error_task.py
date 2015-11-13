import digdag

class Fails(digdag.BaseTask):
    def run(self):
        raise Exception("This task fails")

class ShowError(digdag.BaseTask):
    def run(self):
        print "Task failed:"
        print "  * error_message: " + str(self.params["error_message"])
        print "  * error_task_name: " + str(self.params["error_task_name"])

