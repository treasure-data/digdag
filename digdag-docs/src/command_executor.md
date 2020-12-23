# Command Executor
`Command Executor` provides a functionality to run operators in various environment like Docker.

`sh>`, `py>` `rb>` support Command Executor.

Supported environments are `AWS ECS(Elastic Container Service`, `Docker`, and local.
`Kubernetes` is under development.

For example, if you define a task with `sh>`, the task runs in local. But if you add configuration for Docker, the task is executed in a docker container.
You can switch environment to run a task with simple modification.

Currently, ECS is default Command Executor.
If there is no valid configuration for ECS, fallback to Docker. 
If there is no valid configuration for Docker, fallback to local.

## ECS
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
* agent.command_executor.ecs.name = &lt;name&gt;  ECS Cluster name. &lt;name&gt; is used as the key of following configuration

* agent.command_executor.ecs.&lt;name&gt;.access_key_id AWS access key for ECS. The key needs permissions for ECS and CloudWatch
* agent.command_executor.ecs.&lt;name&gt;.secret_access_key AWS secret key
* agent.command_executor.ecs.&lt;name&gt;.launch_type The launch type of container. `FARGATE` or `EC2`
* agent.command_executor.ecs.&lt;name&gt;.region AWS Region
* agent.command_executor.ecs.&lt;name&gt;.subnets AWS Subnet
* agent.command_executor.ecs.&lt;name&gt;.max_retries retry number for AWS client

Following is configuration for S3 as temporal storage of ECS Command Executor.

* agent.command_executor.ecs.temporal_storage ECS Command Executor requires a storage for running.
* agent.command_executor.ecs.temporal_storage.type Storage type. `s3` or `gcs`
* agent.command_executor.ecs.temporal_storage.s3.bucket The bucket name.
* agent.command_executor.ecs.temporal_storage.s3.endpoint The end point URL for S3
* agent.command_executor.ecs.temporal_storage.s3.credentials.access-key-id AWS access key for the bucket 
* agent.command_executor.ecs.temporal_storage.s3.credentials.secret-access-key AWS secret key

In workflow definition, there are two ways to set a task on ECS.

* Set `ecs.task_definition_arn`
```
_export:
  ecs:
    task_definition_arn: "arn:aws:ecs:us-east-1:..."

+task1:
  py>:
```

* Set `docker.image`
```
_export:
  docker:
    image: "digdag/digdag-python:3.7"

+task1:
  py>: ...
```

In this way, you need to set a tag `digdag.docker.image` as the image name.
ECS Command Executor try to search the tagged Task Definition.
(This way list and check all task definition until found and cause of stress. See issue #1488)

## Docker
The following is an example configuration for Docker Command Executor.

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