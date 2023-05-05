# sh>: Shell scripts

**sh>** operator runs a shell script.

Running a shell command (Note: you can use [echo> operator](echo.html) to show a message):

    +step1:
      sh>: echo "hello world"

Running a shell script:

    +step1:
      sh>: tasks/step1.sh
    +step2:
      sh>: tasks/step2.sh

## Options

* **sh>**: COMMAND [ARGS...]

  Name of the command to run.

  Examples:

      sh>: tasks/workflow.sh --task1

The shell defaults to `/bin/sh`. If an alternate shell such as `zsh` is desired, use the `shell` option in the `_export` section.

    _export:
      sh:
        shell: ["/usr/bin/zsh"]

    +step1:
      sh>: tasks/step2.sh

On Windows, you can set PowerShell.exe to the `shell` option.
Since ver.0.10.6, it is correct to specify ["powershell.exe"], 
but digdag works with ["powershell.exe", "-"] in ver.0.9.46
or earlier.

>= 0.10.6
    _export:
      sh:
        shell: ["powershell.exe"]

    +step1:
      sh>: step1.exe

    +step2:
      sh>: step2.ps1

<= 0.9.46

    _export:
      sh:
        shell: ["powershell.exe", "-"]

    +step1:
      sh>: step1.exe

    +step2:
      sh>: step2.ps1

