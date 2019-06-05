# emr>: Amazon Elastic Map Reduce

**emr>** operator can be used to run EMR jobs, create clusters and submit steps to existing clusters.

For detailed information about EMR, see the [Amazon Elastic MapReduce Documentation](https://aws.amazon.com/documentation/elastic-mapreduce/).

    +emr_job:
      emr>:
      cluster:
        name: my-cluster
        ec2:
          key: my-ec2-key
          master:
            type: m3.2xlarge
          core:
            type: m3.xlarge
            count: 10
        logs: s3://my-bucket/logs/
      staging: s3://my-bucket/staging/
      steps:
        - type: spark
          application: pi.py
        - type: spark-sql
          query: queries/query.sql
          result: s3://my-bucket/results/${session_uuid}/
        - type: script
          script: scripts/hello.sh
          args: [hello, world]

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

* **aws.emr.access_key_id, aws.access_key_id**

  The AWS Access Key ID to use when submitting EMR jobs.

* **aws.emr.secret_access_key, aws.secret_access_key**

  The AWS Secret Access Key to use when submitting EMR jobs.

* **aws.emr.role_arn, aws.role_arn**

  The AWS Role to assume when submitting EMR jobs.

* **aws.emr.region, aws.region**

  The AWS region to use for EMR service.

* **aws.emr.endpoint**

  The AWS EMR [endpoint address](http://docs.aws.amazon.com/general/latest/gr/rande.html) to use.

* **aws.s3.region, aws.region**

  The AWS region to use for S3 service to store staging files.

* **aws.s3.endpoint**

  The AWS S3 [endpoint address](http://docs.aws.amazon.com/general/latest/gr/rande.html) to use for staging files.

* **aws.kms.region, aws.region**

  The AWS region to use for KMS service to encrypt variables passed to EMR jobs.

* **aws.kms.endpoint**

  The AWS KMS [endpoint address](http://docs.aws.amazon.com/general/latest/gr/rande.html) to use for EMR variable encryption.

## Options

* **cluster**: STRING | OBJECT

  Specifies either the ID of an existing cluster to submit steps to or the configuration of a new cluster to create.

  **Using an existing cluster:**

      cluster: j-7KHU3VCWGNAFL

  **Creating a new minimal ephemeral cluster with just one node:**

      cluster:
        ec2:
          key: my-ec2-key
        logs: s3://my-bucket/logs/

  **Creating a customized cluster with several hosts:**

      cluster:
        name: my-cluster
        auto_terminate: false
        release: emr-5.2.0
        applications:
          - hadoop
          - spark
          - hue
          - zookeeper
        ec2:
          key: my-ec2-key
          subnet_id: subnet-83047402b
          master:
            type: m4.2xlarge
          core:
            type: m4.xlarge
            count: 10
            ebs:
              optimized: true
              devices:
                volume_specifiation:
                  iops: 10000
                  size_in_gb: 1000
                  type: gp2
                volumes_per_instance: 6
          task:
            - type: c4.4xlarge
              count: 20
            - type: g2.2xlarge
              count: 6
        logs: s3://my-bucket/logs/
        bootstrap:
          - install_foo.sh
          - name: Install Bar
            path: install_bar.sh
            args: [baz, quux]

* **staging**: S3_URI

  A S3 folder to use for staging local files for execution on the EMR cluster. *Note:* the configured AWS credentials must have permission to put and get objects in this folder.

  Examples:

  ```
  staging: s3://my-bucket/staging/
  ```

* **emr.region**

  The AWS region to use for EMR service.

* **emr.endpoint**

  The AWS EMR [endpoint address](http://docs.aws.amazon.com/general/latest/gr/rande.html) to use.

* **s3.region**

  The AWS region to use for S3 service to store staging files.

* **s3.endpoint**

  The AWS S3 [endpoint address](http://docs.aws.amazon.com/general/latest/gr/rande.html) to use for staging files.

* **kms.region**

  The AWS region to use for KMS service to encrypt variables passed to EMR jobs.

* **kms.endpoint**

  The AWS KMS [endpoint address](http://docs.aws.amazon.com/general/latest/gr/rande.html) to use for EMR variable encryption.

* **steps**: LIST

  A list of steps to submit to the EMR cluster.

    steps:
      - type: flink
        application: flink/WordCount.jar

      - type: hive
        script: queries/hive-query.q
        vars:
          INPUT: s3://my-bucket/data/
          OUTPUT: s3://my-bucket/output/
        hiveconf:
          hive.support.sql11.reserved.keywords: false

      - type: spark
        application: spark/pi.scala

      - type: spark
        application: s3://my-bucket/spark/hello.py
        args: [foo, bar]

      - type: spark
        application: spark/hello.jar
        class: com.example.Hello
        jars:
          - libhello.jar
          - s3://td-spark/td-spark-assembly-0.1.jar
        conf:
          spark.locality.wait: 5s
          spark.memory.fraction: 0.5
        args: [foo, bar]

      - type: spark-sql
        query: spark/query.sql
        result: s3://my-bucket/results/${session_uuid}/

      - type: script
        script: s3://my-bucket/scripts/hello.sh
        args: [hello, world]

      - type: script
        script: scripts/hello.sh
        args: [world]

      - type: command
        command: echo
        args: [hello, world]

* **action_on_failure**: TERMINATE_JOB_FLOW | TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE

  The action EMR should take in response to a job step failing.

## Output parameters

* **emr.last_cluster_id**

  The ID of the cluster created. If a pre-existing cluster was used, this parameter will not be set.
