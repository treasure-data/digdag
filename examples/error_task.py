from tasks import BaseTask

class Fails(BaseTask):
    def run(self):
        raise Exception("This task fails")

class ShowError(BaseTask):
    def run(self):
        print "Task failed:"
        print "  * error_message: " + str(self.params["error_message"])
        print "  * error_task_name: " + str(self.params["error_task_name"])
        print "  * error_retry_at: " + str(self.params["error_retry_at"])
        print "  * is_subtask_propagation: " + str(self.params["is_subtask_propagation"])

