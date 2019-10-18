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

On Windows, you can set PowerShell.exe to the `shell` option:

    _export:
      sh:
        shell: ["powershell.exe", "-"]

    +step1:
      sh>: step1.exe

    +step2:
      sh>: step2.ps1

