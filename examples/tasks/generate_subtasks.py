import digdag

class ParallelProcess(object):
    def split(self):
        digdag.env.store({"task_count": 3})

    def run(self, task_count):
        for i in range(task_count):
            digdag.env.add_subtask(ParallelProcess.subtask, index=i)
        digdag.env.subtask_config["_parallel"] = True

    def subtask(self, index):
        print("Processing " + str(index))

