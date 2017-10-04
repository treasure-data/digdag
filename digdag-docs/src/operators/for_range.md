# for_range>: Repeat tasks for a range

**for_range>** operator runs subtasks multiple times using sets of variables.

(This operator is EXPERIMENTAL. Parameters may change in a future release)

    +repeat:
      for_range>:
        from: 10
        to: 50
        step: 10
      _do:
        echo>: processing from ${range.from} to ${range.to}.
        # this will generate 4 tasks:
        #  +range-from=apple&verb=eat:
        #    echo>: processing from 10 to 20.
        #  +range-from=apple&verb=throw:
        #    echo>: processing from 20 to 30.
        #  +range-from=orange&verb=eat:
        #    echo>: processing from 30 to 40.
        #  +range-from=orange&verb=throw:
        #    echo>: processing from 40 to 50.

* **for_each>**: object of from, to, and slices or step

  This nested object is used to declare a range from **from** to **to**.

  Then you divide the range into fixed number of slices using **slices** option, or divide the range by width by **step** option. Setting both slices and step is error.


  Examples:

  ```
  for_range>:
    from: 0
    to: 10
    step: 3
    # this repeats tasks for 4 times (number of slices is computed automatically):
    #  * {range.from: 0, range.to: 3}
    #  * {range.from: 3, range.to: 6}
    #  * {range.from: 6, range.to: 9}
    #  * {range.from: 9, range.to: 10}
  _do:
    echo>: from ${range.from} to ${range.to}
  ```

  ```
  for_range>:
    from: 0
    to: 10
    slices: 3
    # this repeats tasks for 3 times (size of a slice is computed automatically):
    #  * {range.from: 0, range.to: 4}
    #  * {range.from: 4, range.to: 8}
    #  * {range.from: 8, range.to: 10}
  _do:
    echo>: from ${range.from} to ${range.to}
  ```

* **\_parallel**: BOOLEAN

  Runs the repeating tasks in parallel.

  Examples:

  ```
  _parallel: true
  ```

* **\_do**: TASKS

  Tasks to run.

