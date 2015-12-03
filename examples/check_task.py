from __future__ import print_function
import digdag


def generate():
    with open("result.csv", "w") as f:
        f.write("ok")

def check_generated():
    with open("result.csv", "r") as f:
        data = f.read()

    if len(data) < 2:
        raise Exception("Output data is too small")
    print("ok.")


class Generator(digdag.BaseTask):
    def run(self):
        with open("result.csv", "w") as f:
            f.write("ok")
        digdag.task.carry_params["path"] = "result.csv"

    def check(self, path):
        print("checking "+path)

        with open(path, "r") as f:
            data = f.read()

        if len(data) < 2:
            raise Exception("Output data is too small")
        print("ok.")

