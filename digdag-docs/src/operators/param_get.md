# param_get>: Get a persistent data from a DataStore and set it into ParamStore

**param_get>** operator gets a value from a DataStore and sets into ParamStore

    +get_single_value:
      param_get>:
      key1: key1_for_params
    
    +get_multiple_values:
      param_get>:
      key2: key2_for_params
      key3: key3_for_params
      
    +show_gotten_data:
      sh>: echo '${key1_for_params} ${key2_for_params} ${key3_for_params}'

# System configurations

## Common

* **param_server.database.type (required)**:

  String of the database type of DataStore ( `postgresql` or `redis` )

## When using PostgreSQL

* **param_server.database.host (required)**:

  String of the host of the DataStore

* **param_server.database.database (required)**:

  String of the database name of PostgreSQL

* **param_server.database.user (required)**:

  String of the user of the DataStore

* **param_server.database.password (optional)**:

  String of the password of the DataStore


## When using Redis

* **param_server.database.host (required)**:

  String of the host of the DataStore

* **param_server.database.password (optional)**:

  String of the password of the DataStore

* **param_server.database.ssl (optional)**:

  The boolean flag of whether connection with SSL or not SSL.

  The Default value is `false`

# Options

* **{key}: {destKey}**

  {key}: The name of the key which you want to get from DataStore

  {destKey}: The name of the key which you want to set the value gotten from DataStore to ParamStore
