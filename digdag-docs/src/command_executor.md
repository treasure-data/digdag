# Command Executor
`Command Executor` provides a functionality to run operators in various environment like Docker.

`sh>`, `py>`, and `rb>` support Command Executor.

Supported environments are `AWS ECS (Elastic Container Service)`, `Docker`, and local.
`Kubernetes` is under development.

For example, if you define a task with `sh>`, the task runs in local. If you add configuration for Docker, the task is executed in a docker container.
You can switch environment to run a task without changing task definition.

Currently, ECS is default Command Executor.
If there is no valid configuration for ECS, fallback to Docker. 
If there is no valid configuration for Docker, fallback to local.

## ECS
### Setup
The following is an example configuration for ECS Command Executor.

```
agent.command_executor.ecs.name = digdag-test

agent.command_executor.ecs.digdag-test.access_key_id = <ACCESS KEY>
agent.command_executor.ecs.digdag-test.secret_access_key = <SECRET KEY>
agent.command_executor.ecs.digdag-test.launch_type = FARGATE
agent.command_executor.ecs.digdag-test.region = us-east-1
agent.command_executor.ecs.digdag-test.subnets = subnet-NNNNN
agent.command_executor.ecs.digdag-test.max_retries = 3

agent.command_executor.ecs.temporal_storage.type = s3
agent.command_executor.ecs.temporal_storage.s3.bucket = <Bucket>
agent.command_executor.ecs.temporal_storage.s3.endpoint = s3.amazonaws.com
agent.command_executor.ecs.temporal_storage.s3.credentials.access-key-id = <ACCESS KEY>
agent.command_executor.ecs.temporal_storage.s3.credentials.secret-access-key = <SECRET KEY>
```

Each sub keys of `agent.command_executor` are as follows:

| key                                |  description                                     |
| :--------------------------------- | :----------------------------------------------- |
| ecs.name                           | ECS Cluster name. The value &lt;name&gt; is used as the key of following configuration |
| ecs.&lt;name&gt;.access_key_id     | AWS access key for ECS. The key needs permissions for ECS and CloudWatch  |
| ecs.&lt;name&gt;.secret_access_key | AWS secret key                                   |
| ecs.&lt;name&gt;.launch_type       | The launch type of container. `FARGATE` or `EC2` |
| ecs.&lt;name&gt;.region            | AWS region                                       |
| ecs.&lt;name&gt;.subnets           | AWS subnet                                       |
| ecs.&lt;name&gt;.max_retries       | Number of retry for AWS client                   |

Following keys are for configuration of temporal storage with AWS S3.

| key                                                   | description                      |
| :---------------------------------------------------- | :------------------------------- |
| ecs.temporal_storage.type                             | The bucket type. `s3` for AWS S3 |
| ecs.temporal_storage.s3.bucket                        | Bucket name                      |
| ecs.temporal_storage.s3.endpoint                      | The end point URL for S3         |
| ecs.temporal_storage.s3.credentials.access-key-id     | AWS access key for the bucket    |
| ecs.temporal_storage.s3.credentials.secret-access-key | AWS secret key                   |

### How to use from workflow

In workflow definition, there are two ways to set a task on ECS.

#### Set `ecs.task_definition_arn`
```
_export:
  ecs:
    task_definition_arn: "arn:aws:ecs:us-east-1:..."

+task1:
  py>: ...
```

#### Set `docker.image`
```
_export:
  docker:
    image: "digdag/digdag-python:3.7"

+task1:
  py>: ...
```

You need to set a tag `digdag.docker.image` in the task definition.
ECS Command Executor try to search the tagged task definition.

(This way lists and check all task definitions until found and take long time to run the task. See issue [#1488](https://github.com/treasure-data/digdag/issues/1488))

## Docker
### Setup
No configuration is required to use Docker Command Executor.

### How to use from workflow
The following is an example workflow definition for Docker Command Executor.

```
_export:
  docker:
    image: "python:3.7"
    docker: "/usr/local/bin/docker"
    run_options: [ "-m", "1G" ]
    pull_always: true

+task1:
  py>: ...

```
The sub keys in docker are as follows.

| key         | description                             |
| :---------- | :-------------------------------------- |
| image       | Docker image                            |
| docker      | Docker command. default is `docker`     |
| run_options | Arguments to be passed to `docker run`1 |
| pull_always | Digdag caches the docker image. If you want to pull the image always, set to `true`. Default is `false` |

You can build a docker image to be used with `build` parameter.

```
_export:
  docker:
    image: "azul/zulu-openjdk:8"
    docker: "/usr/local/bin/docker"
    run_options: [ "-m", "1G" ]
    build:
      - apt-get -y update
      - apt-get -y install software-properties-common
    build_options:
      - --build-arg var1=test1

+task1:
  py>: ...

```

Docker Command Executor generates a Dockerfile and build an image then run a container with the image.

| key           | description                            |
| :------------ | :------------------------------------- |
| image         | Base image in the generated Dockerfile |
| build         | Command list which are described in the generated Dockerfile with `RUN` |
| build_options | Option list for `docker build` command |
