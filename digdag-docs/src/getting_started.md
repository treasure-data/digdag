# Getting started

## 1. Downloading the latest version

Digdag is a simple executable file. You can download the file to ``/usr/local/bin`` using `curl` command as following:

    $ curl -o /usr/local/bin/digdag --create-dirs -L "https://dl.digdag.io/digdag-latest"
    $ chmod +x /usr/local/bin/digdag

If `digdag --help` command works, Digdag is installed successfully.

### On Windows, or if curl command doesn't work

On Windows, you can download a file named `digdag-<VERSION>.jar` from [https://dl.digdag.io/digdag-latest](https://dl.digdag.io/digdag-latest). This downloaded file is an executable bat file (as well as a jar file and a UNIX shell script).

Once download completed, please rename the file to `digdag.bat` (Windows) or `/usr/local/bin/digdag` (Linux/UNIX).

### Got error?

If you got an error such as **'Unsupported major.minor version 52.0'**, please download and install the latest [Java SE Development Kit 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (must be newer than **8u72**).

## 2. Running sample workflow

`digdag init <dir>` command generates sample workflow for you:

    $ digdag init mydag
    $ cd mydag
    $ digdag run mydag.dig

Did it work? Next step is adding tasks to `digdag.dig` file to automate your jobs.

Next steps
----------------------------------

* [Architecture](architecture.html)
* [Scheduling workflow](scheduling_workflow.html)
* [Workflow definition](workflow_definition.html)
* [More choices of operators](operators.html)

