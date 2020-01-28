# param_get>: Get persistent data from ParamServer and set it as a value of store parameters

**param_get>** operator gets a value from a ParamServer and sets it as a value of [store parameters](../concepts.html#export-and-store-parameters)

    +get_single_value:
      param_get>:
      key1: key_of_store_parameter1
    
    +get_multiple_values:
      param_get>:
      key2: key_of_store_parameter2
      key3: key_of_store_parameter3
      
    +show_gotten_data:
      sh>: echo '${key_of_store_parameter1} ${key_of_store_parameter2} ${key_of_store_parameter3}'

## System configurations

### Common

* **param_server.type (required)**:

  String of the database type of ParamServer ( `postgresql` or `redis` )

### When using PostgreSQL

* **param_server.host (required)**:

  String of the host of the ParamServer

* **param_server.database (required)**:

  String of the database name of PostgreSQL

* **param_server.user (required)**:

  String of the user of the ParamServer

* **param_server.password (optional)**:

  String of the password of the ParamServer

* **param_server.loginTimeout (optional)**:

  Seconds in integer. default: 30

* **param_server.socketTimeout (optional)**:

  Seconds in integer. default: 1800

* **param_server.ssl (optional)**:

  Boolean. default: false

* **param_server.connectionTimeout (optional)**:

  Seconds in integer. default: 30

* **param_server.idleTimeout (optional)**:

  Seconds in integer. default: 600

* **param_server.validationTimeout (optional)**:

  Seconds in integer. default: 5

* **param_server.maximumPoolSize (optional)**:

  Integer. default: available CPU cores * 32

* **param_server.minimumPoolSize (optional)**:

  Integer. default: same as param_server.maximumPoolSize


### When using Redis

* **param_server.host (required)**:

  String of the host of the ParamServer

* **param_server.password (optional)**:

  String of the password of the ParamServer

* **param_server.ssl (optional)**:

  The boolean flag of whether connection with SSL or not SSL.

  The Default value is `false`

## Options

* **{key}: {destKey}**

  {key}: The name of the key which you want to get from ParamServer

  {destKey}: The name of the key which you want to set the value gotten from ParamServer to ParamStore
