# embulk>: Embulk data transfer

**embulk>** operator runs a [Embulk](<http://www.embulk.org) script to transfer data across storages.

    +load:
      embulk>: data/load.yml

## Options

* `embulk>: FILE.yml`
  Path to a configuration template file.

  * Example: `embulk>: embulk/mysql_to_csv.yml`


