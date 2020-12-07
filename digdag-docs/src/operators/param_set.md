# param_set>: Set a value into a ParamServer as persistent data

**param_set>** operator saves a specified value into a ParamServer

    +set_single_value:
      param_set>:
      key1: value1
    
    +set_multiple_values:
      param_set>:
      key2: value2
      key3: value3


(Note: Each parameter has expired time (TTL).
The value is 7,776,000 seconds (90days: 60sec * 60min * 24hours * 90days)).

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


#### Sample

    param_server.type=postgresql
    param_server.host=my_params.example.com
    param_server.user=serizawa
    param_server.password=QD8-_7nE4eMoaZ4FbKE2pA
    param_server.database=digdag_param_server

### When using Redis

* **param_server.host (required)**:

  String of the host of the ParamServer

* **param_server.password (optional)**:

  String of the password of the ParamServer

* **param_server.ssl (optional)**:

  The boolean flag of whether connection with SSL or not SSL.

  The Default value is `false`


#### Sample

    param_server.type=redis
    param_server.host=my_params.example.com
    param_server.password=AQ5e5EvdiVzlLNsDI0Pm4A
    param_server.ssl=true

## Options

* **{key}: {value}**

  The name of the key/value of the record which you want to save to ParamServer

  * About data type of {value}

    {value} is saved as a String data, so {value} must be a String like value

    OK: `1234`, `abcd`

    NG: `{a: 1}`, `[1,2,3,4,5]`
