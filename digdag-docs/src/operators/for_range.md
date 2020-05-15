# for_range>: Repeat tasks for a range

**for_range>** operator runs subtasks multiple times using sets of variables.

This operator exports `${range.from}`, `${range.to}`, and `${range.index}` variables for the subtasks. Index begins from 0.

    +repeat:
      for_range>:
        from: 10
        to: 50
        step: 10
      _do:
        echo>: processing from ${range.from} to ${range.to}.
        # this will generate 4 tasks:
        #  +range-from=10&to=20:
        #    echo>: processing from 10 to 20.
        #  +range-from=20&to=30:
        #    echo>: processing from 20 to 30.
        #  +range-from=30&to=40:
        #    echo>: processing from 30 to 40.
        #  +range-from=40&to=50:
        #    echo>: processing from 40 to 50.

* **for_range>**: object of from, to, and slices or step

  This nested object is used to declare a range from **from** to **to**.

  Then you divide the range into fixed number of slices using **slices** option, or divide the range by width by **step** option. Setting both slices and step is error.


  Examples:

  ```
  for_range>:
    from: 0
    to: 10
    step: 3
    # this repeats tasks for 4 times (number of slices is computed automatically):
    #  * {range.from: 0, range.to: 3, range.index: 0}
    #  * {range.from: 3, range.to: 6, range.index: 1}
    #  * {range.from: 6, range.to: 9, range.index: 2}
    #  * {range.from: 9, range.to: 10, range.index: 3}
  _do:
    echo>: from ${range.from} to ${range.to}
  ```

  ```
  for_range>:
    from: 0
    to: 10
    slices: 3
    # this repeats tasks for 3 times (size of a slice is computed automatically):
    #  * {range.from: 0, range.to: 4, range.index: 0}
    #  * {range.from: 4, range.to: 8, range.index: 1}
    #  * {range.from: 8, range.to: 10, range.index: 2}
  _do:
    echo>: from ${range.from} to ${range.to}
  ```

* **\_parallel**: BOOLEAN | OBJECT

  Runs the repeating tasks in parallel.
  If ``_parallel: {limit: N}`` (N is an integer: 1, 2, 3, …) parameter is set,
  the number of tasks running in parallel is limited to N.
  Note that the tasks in the loop will be running in serial.

  Examples:

  ```
  _parallel: true
  ```

  Examples:

  ```
  _parallel:
    limit: 2
  ```

* **\_do**: TASKS

  Tasks to run.

