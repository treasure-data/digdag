# embulk>: Embulk data transfer

**This operator is obsoleted**. `embulk>` operator is going to be removed, or rewritten with backward incompatibility.

Please use `sh>: embulk <options>` instead.

**embulk>** operator was used to run a [Embulk](http://www.embulk.org) script to transfer data across storages.

    +load:
      sh>: embulk <option> data/load.yml

