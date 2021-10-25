# Getting started

## 1. Requirements.

Digdag runs on Java. Make sure Java Runtime is installed.

| Digdag version | Java Runtime Version |
|----------------|----------------------|
| >= 0.10.0      | Java 8 or 11         |
| <  0.10.0      | Java 8               |

## 2. Downloading the latest version

Digdag is a single executable file. You can install the file to `~/bin` using `curl` command as following:

    $ curl -o ~/bin/digdag --create-dirs -L "https://dl.digdag.io/digdag-latest"
    $ chmod +x ~/bin/digdag
    $ echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc

Please reopen your terminal window or type following command so that the change of PATH takes effect.

    $ source ~/.bashrc

If `digdag --help` command works, Digdag is installed successfully.

(Note: if you're using zsh, modify your`~/.zshrc` file instead of `~/.bashrc`).

### On Windows?

On Windows, please open cmd.exe or PowerShell.exe and type following command exactly:

```
PowerShell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::TLS12; mkdir -Force $env:USERPROFILE\bin; Invoke-WebRequest http://dl.digdag.io/digdag-latest.jar -OutFile $env:USERPROFILE\bin\digdag.bat}"
```

Above command downloads a file named `digdag.bat` to a folder named `bin` at your home folder (`C:\Users\YOUR_NAME\bin`).

Then, type following command so that cmd.exe or PowerShell.exe search `digdag` command from the `bin` folder:

```
setx PATH "%PATH%;%USERPROFILE%\bin"
```

Please reopen your command window. If `digdag --help` command shows usage message, Digdag is installed successfully.

### Using packages?

Digdag has already been packaged for multiple platforms.

[![Packaging status](https://repology.org/badge/vertical-allrepos/digdag.svg)](https://repology.org/project/digdag/versions)

### curl did not work?

Some environments (ex: Ubuntu 16.04) may produce the following error:

```shell
curl: (77) error setting certificate verify locations:
  CAfile: /etc/pki/tls/certs/ca-bundle.crt
  CApath: none
```

Most likely, the SSL certificate file is in `/etc/ssl/certs/ca-certificates.crt` while `curl` expects it in `/etc/pki/tls/certs/ca-bundle.crt`. To fix this, run the folllowing:

```shell
$ sudo mkdir -p /etc/pki/tls/certs
$ sudo cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
```

Then, run Step 1 again.

### Got error?

If you got an error such as **'Unsupported major.minor version 52.0'**, please download and install the latest [Java SE Development Kit 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (must be newer than **8u72**).

## 3. Running sample workflow

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

