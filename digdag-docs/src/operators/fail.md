# fail>: Makes the workflow failed

**fail>** always fails and makes the workflow failed.

This operator is useful used with [if> operator](if.html) to validate results of a previous task with `_check` directive so that a workflow fails when the validation doesn't pass.

    +fail_if_too_few:
      if>: ${count < 10}
      _do:
        fail>: count is less than 10!

## Options

* **fail>**: STRING

  Message so that `_error` task can refer the message using `${error.message}` syntax.
