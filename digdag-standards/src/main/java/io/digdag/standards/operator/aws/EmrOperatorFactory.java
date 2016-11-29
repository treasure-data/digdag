package io.digdag.standards.operator.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.api.client.repackaged.com.google.common.base.Splitter;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.ImmutableTaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.digdag.standards.operator.aws.EmrOperatorFactory.FileReference.Type.DIRECT;
import static io.digdag.standards.operator.aws.EmrOperatorFactory.FileReference.Type.LOCAL;
import static io.digdag.standards.operator.aws.EmrOperatorFactory.FileReference.Type.S3;
import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public class EmrOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(EmrOperatorFactory.class);

    public String getType()
    {
        return "emr";
    }

    private final TemplateEngine templateEngine;
    private final ObjectMapper objectMapper;
    private final ConfigFactory cf;

    @Inject
    public EmrOperatorFactory(TemplateEngine templateEngine, ObjectMapper objectMapper, ConfigFactory cf)
    {
        this.templateEngine = templateEngine;
        this.objectMapper = objectMapper;
        this.cf = cf;
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new EmrOperator(projectPath, request);
    }

    private class EmrOperator
            extends BaseOperator
    {
        private final TaskState state;
        private final Config params;
        private final String defaultActionOnFailure;

        EmrOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.state = TaskState.of(request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws").getNestedOrGetEmpty("emr"))
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws"));
            this.defaultActionOnFailure = params.get("action_on_failure", String.class, "CANCEL_AND_WAIT");
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.<String>builder()
                    .add("aws.*")
                    .addAll(secretReferences())
                    .build();
        }

        private List<String> secretReferences()
        {
            List<Config> steps = params.getListOrEmpty("steps", Config.class);
            return steps.stream()
                    .filter(c -> c.get("type", String.class, "").equals("spark"))
                    .map(c -> c.getNestedOrderedOrGetEmpty("conf"))
                    .flatMap(conf -> conf.getKeys().stream().flatMap(key -> {
                        JsonNode n = conf.get(key, JsonNode.class);
                        if (n.isObject()) {
                            return Stream.of(n.get("secret").asText());
                        }
                        else {
                            return Stream.of();
                        }
                    })).collect(Collectors.toList());
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            String tag = state.constant("tag", String.class, this::randomTag);

            AWSCredentials credentials = credentials(tag, ctx);

            AWSKMSClient kms = new AWSKMSClient(credentials);
            AmazonElasticMapReduce emr = new AmazonElasticMapReduceClient(credentials);
            AmazonS3Client s3 = new AmazonS3Client(credentials);

            try {
                return run(tag, emr, s3, kms, ctx);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                s3.shutdown();
                emr.shutdown();
            }
        }

        private AWSCredentials credentials(String tag, TaskExecutionContext ctx)
        {
            SecretProvider awsSecrets = ctx.secrets().getSecrets("aws");
            SecretProvider emrSecrets = awsSecrets.getSecrets("emr");

            String accessKeyId = emrSecrets.getSecretOptional("access-key-id")
                    .or(() -> awsSecrets.getSecret("access-key-id"));

            String secretAccessKey = emrSecrets.getSecretOptional("secret-access-key")
                    .or(() -> awsSecrets.getSecret("secret-access-key"));

            AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

            Optional<String> roleArn = emrSecrets.getSecretOptional("role-arn")
                    .or(awsSecrets.getSecretOptional("role-arn"));

            if (!roleArn.isPresent()) {
                return credentials;
            }

            // use STS to assume role
            String roleSessionName = emrSecrets.getSecretOptional("role-session-name")
                    .or(awsSecrets.getSecretOptional("role-session-name"))
                    .or("digdag-emr-" + tag);

            AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(credentials);
            AssumeRoleResult assumeResult = stsClient.assumeRole(new AssumeRoleRequest()
                    .withRoleArn(roleArn.get())
                    .withDurationSeconds(3600)
                    .withRoleSessionName(roleSessionName));

            return new BasicSessionCredentials(
                    assumeResult.getCredentials().getAccessKeyId(),
                    assumeResult.getCredentials().getSecretAccessKey(),
                    assumeResult.getCredentials().getSessionToken());
        }

        private TaskResult run(String tag, AmazonElasticMapReduce emr, AmazonS3Client s3, AWSKMSClient kms, TaskExecutionContext ctx)
                throws IOException
        {
            List<Config> steps = params.getListOrEmpty("steps", Config.class);

            Optional<AmazonS3URI> staging = params.getOptional("staging", String.class).transform(s -> {
                AmazonS3URI uri = new AmazonS3URI(s);
                if (uri.getKey() != null && !uri.getKey().endsWith("/")) {
                    throw new ConfigException("Invalid staging uri: '" + s + "'");
                }
                return uri;
            });

            // Construct job steps
            List<StagingFile> stagingFiles = new ArrayList<>();
            List<StepConfig> stepConfigs = stepConfigs(tag, steps, staging, stagingFiles, kms, ctx);

            // Set up job submitter
            Submitter submitter;
            Config cluster = null;
            try {
                cluster = params.parseNestedOrGetEmpty("cluster");
            }
            catch (ConfigException ignore) {
            }
            if (cluster != null) {
                // Create a new cluster
                submitter = newClusterSubmitter(emr, tag, stepConfigs, cluster, staging, stagingFiles);
            }
            else {
                // Cluster ID? Use existing cluster.
                String clusterId = params.get("cluster", String.class);
                submitter = existingClusterSubmitter(emr, stepConfigs, clusterId);
            }

            // Upload files to staging area
            if (!stagingFiles.isEmpty()) {
                stageFiles(s3, stagingFiles);
            }

            // Submit EMR job
            SubmissionResult submission = submitter.submit();

            // Wait for the steps to finish running
            if (!steps.isEmpty()) {
                waitForSteps(emr, submission);
            }

            return result(submission);
        }

        private String randomTag()
        {
            byte[] bytes = new byte[8];
            ThreadLocalRandom.current().nextBytes(bytes);
            return BaseEncoding.base32().omitPadding().encode(bytes);
        }

        private void waitForSteps(AmazonElasticMapReduce emr, SubmissionResult submission)
        {
            // Note: This currently only looks at the status of the "last" submitted step.
            // TODO: check and log status of intermediate steps
            String lastStepId = Iterables.getLast(submission.stepIds());
            pollingWaiter(state, "result")
                    .withWaitMessage("EMR steps still running")
                    .withPollInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                    .awaitOnce(Step.class, pollState -> {
                        Step lastStep = pollingRetryExecutor(pollState, "poll")
                                .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                                .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                                .run(s -> emr.describeStep(new DescribeStepRequest()
                                        .withClusterId(submission.clusterId())
                                        .withStepId(lastStepId))
                                        .getStep());

                        String stepState = lastStep.getStatus().getState();

                        switch (stepState) {
                            case "PENDING":
                            case "RUNNING":
                                return Optional.absent();

                            case "CANCEL_PENDING":
                            case "CANCELLED":
                            case "FAILED":
                            case "INTERRUPTED":
                                // TODO: consider task done if action_on_failure == CONTINUE?
                                // TODO: include & log failure information
                                throw new TaskExecutionException("EMR failed", ConfigElement.empty());

                            case "COMPLETED":
                                logger.info("EMR steps done");
                                return Optional.of(lastStep);

                            default:
                                throw new RuntimeException("Unknown step status: " + lastStep);
                        }
                    });
        }

        private void stageFiles(AmazonS3Client s3, List<StagingFile> stagingFiles)
        {
            // TODO: break the list of files up into smaller and throw a polling exception in between to avoid blocking this this tread a long time and to allow for agent restarts during staging.
            pollingRetryExecutor(state, "staging")
                    .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                    .runOnce(s -> {
                        TransferManager transferManager = new TransferManager(s3);
                        try {
                            List<PutObjectRequest> requests = new ArrayList<>();
                            for (StagingFile f : stagingFiles) {
                                logger.info("Staging {} -> {}", f.file().reference().filename(), f.file().s3Uri());
                                requests.add(stagingFilePutRequest(f));
                            }

                            List<Upload> uploads = requests.stream()
                                    .map(transferManager::upload)
                                    .collect(Collectors.toList());

                            for (Upload upload : uploads) {
                                try {
                                    upload.waitForCompletion();
                                }
                                catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
                                }
                            }
                        }
                        finally {
                            transferManager.shutdownNow(false);
                        }
                    });
        }

        private PutObjectRequest stagingFilePutRequest(StagingFile stagingFile)
        {
            AmazonS3URI uri = stagingFile.file().s3Uri();
            FileReference reference = stagingFile.file().reference();
            switch (reference.type()) {
                case LOCAL: {
                    if (stagingFile.template()) {
                        try {
                            String content = workspace.templateFile(templateEngine, reference.filename(), UTF_8, params);
                            byte[] bytes = content.getBytes(UTF_8);
                            ObjectMetadata metadata = new ObjectMetadata();
                            metadata.setContentLength(bytes.length);
                            return new PutObjectRequest(uri.getBucket(), uri.getKey(), new ByteArrayInputStream(bytes), metadata);
                        }
                        catch (IOException | TemplateException e) {
                            throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
                        }
                    }
                    else {
                        return new PutObjectRequest(uri.getBucket(), uri.getKey(), workspace.getFile(reference.filename()));
                    }
                }
                case RESOURCE: {
                    byte[] bytes;
                    try {
                        bytes = Resources.toByteArray(new URL(reference.reference().get()));
                    }
                    catch (IOException e) {
                        throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
                    }
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(bytes.length);
                    return new PutObjectRequest(uri.getBucket(), uri.getKey(), new ByteArrayInputStream(bytes), metadata);
                }
                case DIRECT:
                    byte[] bytes = reference.contents().get();
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(bytes.length);
                    return new PutObjectRequest(uri.getBucket(), uri.getKey(), new ByteArrayInputStream(bytes), metadata);
                case S3:
                default:
                    throw new AssertionError();
            }
        }

        private TaskResult result(SubmissionResult submission)
        {
            ImmutableTaskResult.Builder result = TaskResult.defaultBuilder(request);
            if (submission.newCluster()) {
                Config storeParams = request.getConfig().getFactory().create();
                storeParams.getNestedOrSetEmpty("emr").set("last_cluster_id", submission.clusterId());
                result.storeParams(storeParams);
                result.addResetStoreParams(ConfigKey.of("emr", "last_cluster_id"));
            }
            return result.build();
        }

        private Submitter existingClusterSubmitter(AmazonElasticMapReduce emr, List<StepConfig> stepConfigs, String clusterId)
        {
            return () -> {
                AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
                        .withJobFlowId(clusterId)
                        .withSteps(stepConfigs);

                List<String> stepIds = pollingRetryExecutor(state, "submission")
                        .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        .runOnce(new TypeReference<List<String>>() {}, s -> {
                            logger.info("Submitting {} EMR step(s): ", request.getSteps().size(), clusterId);
                            AddJobFlowStepsResult result = emr.addJobFlowSteps(request);
                            logger.info("Submitted {} EMR step(s): {}: {}", request.getSteps().size(), clusterId, result.getStepIds());
                            return ImmutableList.copyOf(result.getStepIds());
                        });

                return SubmissionResult.ofExistingCluster(clusterId, stepIds);
            };
        }

        private Submitter newClusterSubmitter(AmazonElasticMapReduce emr, String tag, List<StepConfig> stepConfigs, Config cluster, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles)
        {
            Config ec2 = cluster.getNested("ec2");
            Config master = ec2.getNestedOrGetEmpty("master");
            List<Config> core = ec2.getOptional("core", Config.class).transform(ImmutableList::of).or(ImmutableList.of());
            List<Config> task = ec2.getListOrEmpty("task", Config.class);

            List<String> applications = cluster.getListOrEmpty("applications", String.class);
            if (applications.isEmpty()) {
                applications = ImmutableList.of("Hadoop", "Hive", "Spark", "Flink");
            }

            // TODO: allow configuring additional application parameters
            List<Application> applicationConfigs = applications.stream()
                    .map(s -> new Application().withName(s))
                    .collect(Collectors.toList());

            // TODO: merge configurations with the same classification?
            List<Configuration> configurations = cluster.getListOrEmpty("configurations", JsonNode.class).stream()
                    .map(this::clusterConfigurations)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            List<BootstrapActionConfig> bootstrapActions = cluster.getListOrEmpty("bootstrap", JsonNode.class).stream()
                    .map(node -> bootstrapAction(node, tag, staging, stagingFiles))
                    .collect(Collectors.toList());

            Optional<String> subnetId = ec2.getOptional("subnet_id", String.class);

            String defaultMasterInstanceType;
            String defaultCoreInstanceType;
            String defaultTaskInstanceType;

            if (subnetId.isPresent()) {
                // m4 requires VPC (subnet id)
                defaultMasterInstanceType = "m4.2xlarge";
                defaultCoreInstanceType = "m4.xlarge";
                defaultTaskInstanceType = "m4.xlarge";
            }
            else {
                defaultMasterInstanceType = "m3.2xlarge";
                defaultCoreInstanceType = "m3.xlarge";
                defaultTaskInstanceType = "m3.xlarge";
            }

            RunJobFlowRequest request = new RunJobFlowRequest()
                    .withName(cluster.get("name", String.class, "Digdag") + " (" + tag + ")")
                    .withReleaseLabel(cluster.get("release", String.class, "emr-5.2.0"))
                    .withSteps(stepConfigs)
                    .withBootstrapActions(bootstrapActions)
                    .withApplications(applicationConfigs)
                    .withLogUri(cluster.get("logs", String.class, null))
                    .withJobFlowRole(cluster.get("cluster_role", String.class, "EMR_EC2_DefaultRole"))
                    .withServiceRole(cluster.get("service_role", String.class, "EMR_DefaultRole"))
                    .withTags(new Tag().withKey("DIGDAG_CLUSTER_ID").withValue(tag))
                    .withVisibleToAllUsers(cluster.get("visible", boolean.class, true))
                    .withConfigurations(configurations)
                    .withInstances(new JobFlowInstancesConfig()
                            .withInstanceGroups(ImmutableList.<InstanceGroupConfig>builder()
                                    // Master Node
                                    .add(instanceGroupConfig("Master", master, "MASTER", defaultMasterInstanceType, 1))
                                    // Core Group
                                    .addAll(instanceGroupConfigs("Core", core, "CORE", defaultCoreInstanceType))
                                    // Task Groups
                                    .addAll(instanceGroupConfigs("Task %d", task, "TASK", defaultTaskInstanceType))
                                    .build()
                            )
                            .withAdditionalMasterSecurityGroups(ec2.getListOrEmpty("additional_master_security_groups", String.class))
                            .withAdditionalSlaveSecurityGroups(ec2.getListOrEmpty("additional_slave_security_groups", String.class))
                            .withEmrManagedMasterSecurityGroup(ec2.get("emr_managed_master_security_group", String.class, null))
                            .withEmrManagedSlaveSecurityGroup(ec2.get("emr_managed_slave_security_group", String.class, null))
                            .withServiceAccessSecurityGroup(ec2.get("service_access_security_group", String.class, null))
                            .withTerminationProtected(cluster.get("termination_protected", boolean.class, false))
                            .withPlacement(cluster.getOptional("availability_zone", String.class).transform(s -> new PlacementType().withAvailabilityZone(s)).orNull())
                            .withEc2SubnetId(subnetId.orNull())
                            .withEc2KeyName(ec2.get("key", String.class))
                            .withKeepJobFlowAliveWhenNoSteps(!cluster.get("auto_terminate", boolean.class, true)));

            // Only creating a cluster, with no steps?
            boolean createOnly = stepConfigs.isEmpty();

            return () -> {
                // Start cluster
                String clusterId = pollingRetryExecutor(state, "submission")
                        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        // TODO: EMR requests are not idempotent, thus retrying might produce duplicate cluster submissions.
                        .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                        .runOnce(String.class, s -> {
                            logger.info("Submitting EMR job with {} steps(s)", request.getSteps().size());
                            RunJobFlowResult result = emr.runJobFlow(request);
                            logger.info("Submitted EMR job with {} step(s): {}", request.getSteps().size(), result.getJobFlowId(), result);
                            return result.getJobFlowId();
                        });

                // Get submitted step IDs
                List<String> stepIds = pollingRetryExecutor(this.state, "steps")
                        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                        .runOnce(new TypeReference<List<String>>() {}, s -> {
                            List<String> ids = listSubmittedStepIds(emr, tag, clusterId, stepConfigs.size());
                            logger.info("EMR step ID's: {}: {}", clusterId, ids);
                            return ids;
                        });

                // Log cluster status while waiting for it to come up
                pollingWaiter(state, "bootstrap")
                        .withWaitMessage("EMR cluster still booting")
                        .withPollInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        .awaitOnce(String.class, s -> {
                            DescribeClusterResult describeClusterResult = pollingRetryExecutor(s, "describe-cluster")
                                    .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                                    .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                                    .run(ds -> emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId)));

                            ClusterStatus clusterStatus = describeClusterResult.getCluster().getStatus();
                            String clusterState = clusterStatus.getState();

                            switch (clusterState) {
                                case "STARTING":
                                    logger.info("EMR cluster starting: {}", clusterId);
                                    return Optional.absent();
                                case "BOOTSTRAPPING":
                                    logger.info("EMR cluster bootstrapping: {}", clusterId);
                                    return Optional.absent();

                                case "RUNNING":
                                case "WAITING":
                                    logger.info("EMR cluster up: {}", clusterId);
                                    return Optional.of(clusterState);

                                case "TERMINATED_WITH_ERRORS":
                                    if (createOnly) {
                                        // TODO: log more information about the errors
                                        // TODO: inspect state change reason to figure out whether it was the boot that failed or e.g. steps submitted by another agent
                                        throw new TaskExecutionException("EMR boot failed: " + clusterId, ConfigElement.empty());
                                    }
                                    return Optional.of(clusterState);

                                case "TERMINATING":
                                    if (createOnly) {
                                        // Keep waiting for the final state
                                        // TODO: inspect state change reason and bail early here
                                        return Optional.absent();
                                    }
                                    return Optional.of(clusterState);

                                case "TERMINATED":
                                    return Optional.of(clusterState);

                                default:
                                    throw new RuntimeException("Unknown EMR cluster state: " + clusterState);
                            }
                        });

                return SubmissionResult.ofNewCluster(clusterId, stepIds);
            };
        }

        private List<InstanceGroupConfig> instanceGroupConfigs(String defaultName, List<Config> configs, String role, String defaultInstanceType)
        {
            List<InstanceGroupConfig> instanceGroupConfigs = new ArrayList<>();
            for (int i = 0; i < configs.size(); i++) {
                Config config = configs.get(i);
                instanceGroupConfigs.add(instanceGroupConfig(String.format(defaultName, i + 1), config, role, defaultInstanceType));
            }
            return instanceGroupConfigs;
        }

        private InstanceGroupConfig instanceGroupConfig(String defaultName, Config config, String role, String defaultInstanceType)
        {
            int instanceCount = config.get("count", int.class, 0);
            return instanceGroupConfig(defaultName, config, role, defaultInstanceType, instanceCount);
        }

        private InstanceGroupConfig instanceGroupConfig(String defaultName, Config config, String role, String defaultInstanceType, int instanceCount)
        {
            return new InstanceGroupConfig()
                    .withName(config.get("name", String.class, defaultName))
                    .withInstanceRole(role)
                    .withInstanceCount(instanceCount)
                    .withInstanceType(config.get("type", String.class, defaultInstanceType))
                    .withMarket(config.get("market", String.class, null))
                    .withBidPrice(config.get("bid_price", String.class, null))
                    .withEbsConfiguration(config.getOptional("ebs", Config.class).transform(this::ebsConfiguration).orNull())
                    .withConfigurations(config.getListOrEmpty("configurations", JsonNode.class).stream()
                            .map(this::clusterConfigurations)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList()));
        }

        private EbsConfiguration ebsConfiguration(Config config)
        {
            return new EbsConfiguration()
                    .withEbsOptimized(config.get("optimized", Boolean.class, null))
                    .withEbsBlockDeviceConfigs(config.getListOrEmpty("devices", Config.class).stream()
                            .map(this::ebsBlockDeviceConfig)
                            .collect(Collectors.toList()));
        }

        private EbsBlockDeviceConfig ebsBlockDeviceConfig(Config config)
        {
            return new EbsBlockDeviceConfig()
                    .withVolumeSpecification(volumeSpecification(config.getNested("volume_specification")))
                    .withVolumesPerInstance(config.get("volumes_per_instance", Integer.class, null));
        }

        private VolumeSpecification volumeSpecification(Config config)
        {
            return new VolumeSpecification()
                    .withIops(config.get("iops", Integer.class, null))
                    .withSizeInGB(config.get("size_in_gb", Integer.class))
                    .withVolumeType(config.get("type", String.class));
        }

        private List<String> listSubmittedStepIds(AmazonElasticMapReduce emr, String tag, String clusterId, int expectedSteps)
        {
            List<String> stepIds = new ArrayList<>();
            ListStepsRequest request = new ListStepsRequest().withClusterId(clusterId);
            while (stepIds.size() < expectedSteps) {
                ListStepsResult result = emr.listSteps(request);
                for (StepSummary step : result.getSteps()) {
                    if (step.getName().contains(tag)) {
                        stepIds.add(step.getId());
                    }
                }
                if (result.getMarker() == null) {
                    break;
                }
                request.setMarker(result.getMarker());
            }
            // The ListSteps api returns steps in reverse order. So reverse them to submission order.
            Collections.reverse(stepIds);
            return stepIds;
        }

        private BootstrapActionConfig bootstrapAction(JsonNode action, String tag, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles)
        {
            String script;
            String name;
            List<String> args;
            FileReference reference;

            if (action.isTextual()) {
                script = action.asText();
                reference = fileReference("bootstrap", script);
                name = reference.filename();
                args = ImmutableList.of();
            }
            else if (action.isObject()) {
                Config config = request.getConfig().getFactory().create(action);
                script = config.get("path", String.class);
                reference = fileReference("bootstrap", script);
                name = config.get("name", String.class, reference.filename());
                args = config.getListOrEmpty("args", String.class);
            }
            else {
                throw new ConfigException("Invalid bootstrap action: " + action);
            }

            RemoteFile file = prepareRemoteFile(tag, 0, reference, staging, stagingFiles, false);

            return new BootstrapActionConfig()
                    .withName(name)
                    .withScriptBootstrapAction(new ScriptBootstrapActionConfig()
                            .withPath(file.s3Uri().toString())
                            .withArgs(args));
        }

        private List<Configuration> clusterConfigurations(JsonNode node)
        {
            if (node.isTextual()) {
                // File
                String configurationJson;
                try {
                    configurationJson = workspace.templateFile(templateEngine, node.asText(), UTF_8, params);
                }
                catch (IOException | TemplateException e) {
                    throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
                }
                List<ConfigurationJson> values;
                try {
                    values = objectMapper.readValue(configurationJson, new TypeReference<List<ConfigurationJson>>() {});
                }
                catch (IOException e) {
                    throw new ConfigException("Invalid EMR configuration file: " + node.asText());
                }
                return values.stream()
                        .map(ConfigurationJson::toConfiguration)
                        .collect(Collectors.toList());
            }
            else if (node.isObject()) {
                // Embedded configuration
                Config config = cf.create(node);
                return ImmutableList.of(new Configuration()
                        .withConfigurations(config.getListOrEmpty("configurations", JsonNode.class).stream()
                                .map(this::clusterConfigurations)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()))
                        .withClassification(config.get("classification", String.class, null))
                        .withProperties(config.get("properties", new TypeReference<Map<String, String>>() {}, null)));
            }
            else {
                throw new ConfigException("Invalid EMR configuration: '" + node + "'");
            }
        }

        private List<StepConfig> stepConfigs(String tag, List<Config> steps, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, AWSKMSClient kms, TaskExecutionContext ctx)
                throws IOException
        {
            List<StepConfig> stepConfigs = new ArrayList<>();
            for (int i = 0; i < steps.size(); i++) {
                Config step = steps.get(i);
                int stepIndex = i + 1;
                String type = step.get("type", String.class);
                switch (type) {
                    case "flink":
                        flinkStep(tag, staging, stagingFiles, stepConfigs, stepIndex, step);
                        break;
                    case "hive":
                        hiveStep(tag, staging, stagingFiles, stepConfigs, stepIndex, step);
                        break;
                    case "spark":
                        sparkStep(tag, staging, stagingFiles, stepConfigs, stepIndex, step, kms, ctx);
                        break;
                    case "spark-sql":
                        sparkSqlStep(tag, staging, stagingFiles, stepConfigs, stepIndex, step);
                        break;
                    case "script":
                        scriptStep(tag, staging, stagingFiles, stepConfigs, stepIndex, step);
                        break;
                    case "command":
                        commandStep(tag, stepConfigs, step);
                        break;
                    default:
                        throw new ConfigException("Unsupported step type: " + type);
                }
            }
            return stepConfigs;
        }

        private void sparkStep(String tag, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, List<StepConfig> stepConfigs, int stepIndex, Config step, AWSKMSClient kms, TaskExecutionContext ctx)
                throws IOException
        {
            FileReference applicationReference = fileReference("application", step);
            boolean scala = applicationReference.filename().endsWith(".scala");
            boolean python = applicationReference.filename().endsWith(".py");
            boolean script = scala || python;
            RemoteFile applicationFile = prepareRemoteFile(tag, stepIndex, applicationReference, staging, stagingFiles, script);

            List<String> jars = step.getListOrEmpty("jars", String.class);
            List<RemoteFile> jarFiles = jars.stream()
                    .map(r -> fileReference("jar", r))
                    .map(r -> prepareRemoteFile(tag, stepIndex, r, staging, stagingFiles, false))
                    .collect(Collectors.toList());

            List<String> jarArgs = jarFiles.isEmpty()
                    ? ImmutableList.of()
                    : ImmutableList.of("--jars", jarFiles.stream().map(RemoteFile::localPath).collect(Collectors.joining(",")));

            Config conf = step.getNestedOrderedOrGetEmpty("conf");
            List<Parameter> confArgs = conf.getKeys().stream()
                    .flatMap(key -> {
                        JsonNode c = conf.get(key, JsonNode.class);
                        if (c.isTextual()) {
                            return Stream.of(Parameter.ofPlain("--conf"), Parameter.ofPlain(key + "=" + c.asText()));
                        }
                        else if (c.isObject()) {
                            String secretKey = c.get("secret").asText();
                            String secretValue = ctx.secrets().getSecret(secretKey);
                            String value = key + "=" + secretValue;
                            return Stream.of(Parameter.ofPlain("--conf"), Parameter.ofKmsEncrypted(kmsEncrypt(kms, ctx, value)));
                        }
                        else {
                            throw new ConfigException("Invalid conf: " + key + ": +'" + c + '"');
                        }
                    })
                    .collect(Collectors.toList());

            List<String> classArgs = step.getOptional("class", String.class)
                    .transform(s -> ImmutableList.of("--class", s))
                    .or(ImmutableList.of());

            String name;
            if (scala) {
                name = "Spark Shell Script";
            }
            else if (python) {
                name = "Spark Py Script";
            }
            else {
                name = "Spark Application";
            }

            ImmutableCommandRunnerConfiguration.Builder configuration = CommandRunnerConfiguration.builder();

            // Download
//            downloadStep(tag, stepConfigs, step, applicationFile, name);
//            jarFiles.forEach(f -> downloadStep(tag, stepConfigs, step, f, "Jar"));
            configuration.addDownload(DownloadConfig.of(applicationFile.s3Uri().toString(), applicationFile.localPath()));
            for (RemoteFile jarFile : jarFiles) {
                configuration.addDownload(DownloadConfig.of(jarFile.s3Uri().toString(), jarFile.localPath()));
            }

            // Run
            String command;
            List<String> applicationArgs;
            String deployMode;
            List<String> args = step.getListOrEmpty("args", String.class);
            if (scala) {
                // spark-shell needs the script to explicitly exit, otherwise it will wait forever for user input.
                // Fortunately spark-shell accepts multiple scripts on the command line, so we append a helper script to run last and exit the shell.
                // This could also have been accomplished by wrapping the spark-shell invocation in a bash session that concatenates the exit command onto the user script using
                // anonymous fifo's etc but that seems a bit more brittle. Also, this way the actual names of the scripts appear in logs instead of /dev/fd/47 etc.
                String exitHelperFilename = "__exit-helper.scala";
                URL exitHelperResource = Resources.getResource(EmrOperatorFactory.class, exitHelperFilename);
                FileReference exitHelperFileReference = ImmutableFileReference.builder()
                        .reference(exitHelperResource.toString())
                        .type(FileReference.Type.RESOURCE)
                        .filename(exitHelperFilename)
                        .build();
                RemoteFile exitHelperFile = prepareRemoteFile(tag, stepIndex, exitHelperFileReference, staging, stagingFiles, false);
//                downloadStep(tag, stepConfigs, step, exitHelperFile, "Spark Shell Script Exit Helper");
                configuration.addDownload(DownloadConfig.of(exitHelperFile.s3Uri().toString(), exitHelperFile.localPath()));

                command = "spark-shell";
                applicationArgs = ImmutableList.of("-i", applicationFile.localPath(), exitHelperFile.localPath());
                String requiredDeployMode = "client";
                deployMode = step.get("deploy_mode", String.class, requiredDeployMode);
                if (!deployMode.equals(requiredDeployMode)) {
                    throw new ConfigException("Only '" + requiredDeployMode + "' deploy_mode is supported for Spark shell scala scripts, got: '" + deployMode + "'");
                }
                if (!args.isEmpty()) {
                    throw new ConfigException("The 'args' parameter is not supported for Spark shell scala scripts, got: " + args);
                }
            }
            else {
                command = "spark-submit";
                applicationArgs = ImmutableList.<String>builder()
                        .add(applicationFile.localPath())
                        .addAll(args)
                        .build();
                deployMode = step.get("deploy_mode", String.class, "cluster");
            }

            configuration.addAllCommand(Parameter.ofPlain(command, "--deploy-mode", deployMode));
            configuration.addAllCommand(Parameter.ofPlain(step.getListOrEmpty("submit_options", String.class)));
            configuration.addAllCommand(Parameter.ofPlain(jarArgs));
            configuration.addAllCommand(confArgs);
            configuration.addAllCommand(Parameter.ofPlain(classArgs));
            configuration.addAllCommand(Parameter.ofPlain(applicationArgs));

            // TODO: stage and share a single command runner script for all steps
            URL commandRunnerResource = Resources.getResource(EmrOperatorFactory.class, "command-runner.py");
            FileReference commandRunnerFileReference = ImmutableFileReference.builder()
                    .reference(commandRunnerResource.toString())
                    .type(FileReference.Type.RESOURCE)
                    .filename("command-runner.py")
                    .build();
            RemoteFile remoteCommandRunnerFile = prepareRemoteFile(tag, stepIndex, commandRunnerFileReference, staging, stagingFiles, false);

            String configFilename = "config-" + UUID.randomUUID() + ".json";
            ImmutableCommandRunnerConfiguration c = configuration.build();
            FileReference configurationFileReference = ImmutableFileReference.builder()
                    .type(FileReference.Type.DIRECT)
                    .contents(objectMapper.writeValueAsBytes(configuration.build()))
                    .filename(configFilename)
                    .build();
            RemoteFile remoteConfigurationFile = prepareRemoteFile(tag, stepIndex, configurationFileReference, staging, stagingFiles, false);

            StepConfig runStep = stepConfig("Run", name, tag, step)
                    .withHadoopJarStep(stepFactory().newScriptRunnerStep(remoteCommandRunnerFile.s3Uri().toString(), remoteConfigurationFile.s3Uri().toString()));

            stepConfigs.add(runStep);
        }

        private String kmsEncrypt(AWSKMSClient kms, TaskExecutionContext ctx, String value)
        {
            String kmsKeyId = ctx.secrets().getSecret("aws.emr.kms_key_id");
            EncryptResult result = kms.encrypt(new EncryptRequest().withKeyId(kmsKeyId).withPlaintext(UTF_8.encode(value)));
            return base64(result.getCiphertextBlob());
        }

        private String base64(ByteBuffer bb)
        {
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }

        private void sparkSqlStep(String tag, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, List<StepConfig> stepConfigs, int stepIndex, Config step)
        {
            FileReference queryReference = fileReference("query", step);

            RemoteFile queryFile = prepareRemoteFile(tag, stepIndex, queryReference, staging, stagingFiles, true);

            String wrapperFilename = "spark-sql-wrapper.py";
            URL wrapperResource = Resources.getResource(EmrOperatorFactory.class, wrapperFilename);
            FileReference wrapperFileReference = ImmutableFileReference.builder()
                    .reference(wrapperResource.toString())
                    .type(FileReference.Type.RESOURCE)
                    .filename(wrapperFilename)
                    .build();

            RemoteFile wrapperFile = prepareRemoteFile(tag, stepIndex, wrapperFileReference, staging, stagingFiles, false);

            // Download
            downloadStep(tag, stepConfigs, step, wrapperFile, "Spark Sql Wrapper");
            downloadStep(tag, stepConfigs, step, queryFile, "Spark Sql Query");

            // Submit
            StepConfig stepConfig = stepConfig("Run", "Spark Sql", tag, step)
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar("command-runner.jar")
                            .withArgs(ImmutableList.<String>builder()
                                    .add("spark-submit")
                                    .add("--deploy-mode", step.get("deploy_mode", String.class, "cluster"))
                                    .add("--files", queryFile.localPath())
                                    .addAll(step.getListOrEmpty("submit_options", String.class))
                                    .add(wrapperFile.localPath())
                                    .add(queryReference.filename(), step.get("result", String.class))
                                    .build()));
            stepConfigs.add(stepConfig);
        }

        private void scriptStep(String tag, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, List<StepConfig> stepConfigs, int stepIndex, Config step)
        {
            FileReference fileReference = fileReference("script", step);
            RemoteFile remoteFile = prepareRemoteFile(tag, stepIndex, fileReference, staging, stagingFiles, false);
            String[] args = step.getListOrEmpty("args", String.class).stream().toArray(String[]::new);
            StepConfig stepConfig = stepConfig("Run", "Script", tag, step)
                    .withHadoopJarStep(stepFactory().newScriptRunnerStep(remoteFile.s3Uri().toString(), args));
            stepConfigs.add(stepConfig);
        }

        private void flinkStep(String tag, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, List<StepConfig> stepConfigs, int stepIndex, Config step)
        {
            String name = "Flink Application";

            FileReference fileReference = fileReference("application", step);
            RemoteFile remoteFile = prepareRemoteFile(tag, stepIndex, fileReference, staging, stagingFiles, false);

            // Download
            downloadStep(tag, stepConfigs, step, remoteFile, name);

            // Run
            StepConfig runStep = stepConfig("Run", name, tag, step)
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar("command-runner.jar")
                            .withArgs(ImmutableList.<String>builder()
                                    .add("flink", "run", "-m", "yarn-cluster")
                                    .add("-yn", Integer.toString(step.get("yarn_containers", int.class, 2)))
                                    .add(remoteFile.localPath())
                                    .addAll(step.getListOrEmpty("args", String.class))
                                    .build()));

            stepConfigs.add(runStep);
        }

        private void hiveStep(String tag, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, List<StepConfig> stepConfigs, int stepIndex, Config step)
        {
            FileReference scriptReference = fileReference("script", step);
            RemoteFile remoteScript = prepareRemoteFile(tag, stepIndex, scriptReference, staging, stagingFiles, false);

            Config hiveConf = step.getNestedOrGetEmpty("hiveconf");
            List<String> hiveconfArgs = hiveConf.getKeys().stream()
                    .flatMap(key -> Stream.of("-hiveconf", key + "=" + hiveConf.get(key, String.class)))
                    .collect(Collectors.toList());

            Config varsConf = step.getNestedOrGetEmpty("vars");
            List<String> varsArgs = varsConf.getKeys().stream()
                    .flatMap(key -> Stream.of("-d", key + "=" + varsConf.get(key, String.class)))
                    .collect(Collectors.toList());

            StepConfig stepConfig = stepConfig("Run", "Hive Script", tag, step)
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar("command-runner.jar")
                            .withArgs(ImmutableList.<String>builder()
                                    .add("hive-script", "--run-hive-script", "--args", "-f", remoteScript.s3Uri().toString())
                                    .addAll(varsArgs)
                                    .addAll(hiveconfArgs)
                                    .build()));

            stepConfigs.add(stepConfig);
        }

        private void downloadStep(String tag, List<StepConfig> stepConfigs, Config step, RemoteFile file, String name)
        {
            // TODO: download staging files in one step using aws s3 cp --recursive
            StepConfig stepConfig = stepConfig("Download", name, tag, step)
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar("command-runner.jar")
                            .withArgs("aws", "s3", "cp", file.s3Uri().toString(), file.localPath()));
            stepConfigs.add(stepConfig);
        }

        private StepFactory stepFactory()
        {
            // TODO: configure region
            return new StepFactory();
        }

        private StepConfig stepConfig(String prefix, String defaultName, String tag, Config step)
        {
            String name = step.get("name", String.class, defaultName);
            return new StepConfig()
                    .withName(prefix + " - " + name + " (" + tag + ")")
                    // TERMINATE_JOB_FLOW | TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE
                    .withActionOnFailure(step.get("action_on_failure", String.class, defaultActionOnFailure));
        }

        private RemoteFile prepareRemoteFile(String tag, int stepIndex, FileReference reference, Optional<AmazonS3URI> staging, List<StagingFile> stagingFiles, boolean template)
        {
            String relativePath = tag + "/" + stepIndex + "/" + reference.filename();
            String localPath = "/home/hadoop/digdag-staging/" + relativePath;

            ImmutableRemoteFile.Builder builder =
                    ImmutableRemoteFile.builder()
                            .reference(reference)
                            .relativePath(relativePath)
                            .localPath(localPath);

            if (reference.local()) {
                // Local file? Then we need to upload it to S3.
                if (!staging.isPresent()) {
                    throw new ConfigException("Please configure a S3 'staging' directory");
                }
                String baseKey = staging.get().getKey();
                String key = (baseKey != null ? baseKey : "") + relativePath;
                builder.s3Uri(new AmazonS3URI("s3://" + staging.get().getBucket() + "/" + key));
            }
            else {
                builder.s3Uri(new AmazonS3URI(reference.reference().get()));
            }

            RemoteFile remoteFile = builder.build();

            if (reference.local()) {
                stagingFiles.add(ImmutableStagingFile.builder()
                        .template(template)
                        .file(remoteFile)
                        .build());
            }

            return remoteFile;
        }

        private FileReference fileReference(String key, Config config)
        {
            String reference = config.get(key, String.class);
            return fileReference(key, reference);
        }

        private FileReference fileReference(String key, String reference)
        {
            if (reference.startsWith("s3:")) {
                // File on S3
                AmazonS3URI uri;
                String invalidMessage = "Invalid " + key + ": '" + reference + "'";
                try {
                    uri = new AmazonS3URI(reference);
                }
                catch (IllegalArgumentException e) {
                    throw new ConfigException(invalidMessage, e);
                }
                if (uri.getKey() == null || uri.getKey().endsWith("/")) {
                    throw new ConfigException(invalidMessage);
                }
                return ImmutableFileReference.builder()
                        .reference(reference)
                        .filename(Iterables.getLast(Splitter.on('/').split(reference), ""))
                        .type(S3)
                        .build();
            }
            else {
                // Local file
                return ImmutableFileReference.builder()
                        .reference(reference)
                        .filename(Paths.get(reference).getFileName().toString())
                        .type(LOCAL)
                        .build();
            }
        }

        private void commandStep(String tag, List<StepConfig> stepConfigs, Config step)
        {
            String[] args = step.getListOrEmpty("args", String.class).toArray(new String[0]);
            StepConfig stepConfig = stepConfig("Run", "Command", tag, step)
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar("command-runner.jar")
                            .withArgs(step.get("command", String.class))
                            .withArgs(args));
            stepConfigs.add(stepConfig);
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface StagingFile
    {
        boolean template();

        RemoteFile file();

        static StagingFile of(boolean template, RemoteFile remoteFile)
        {
            return ImmutableStagingFile.builder()
                    .template(template)
                    .file(remoteFile)
                    .build();
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface SubmissionResult
    {
        boolean newCluster();

        String clusterId();

        List<String> stepIds();

        static SubmissionResult ofNewCluster(String clusterId, List<String> stepIds)
        {
            return ImmutableSubmissionResult.builder()
                    .newCluster(true)
                    .clusterId(clusterId)
                    .stepIds(stepIds)
                    .build();
        }

        static SubmissionResult ofExistingCluster(String clusterId, List<String> stepIds)
        {
            return ImmutableSubmissionResult.builder()
                    .newCluster(false)
                    .clusterId(clusterId)
                    .stepIds(stepIds)
                    .build();
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface FileReference
    {
        enum Type
        {
            LOCAL,
            RESOURCE,
            S3,
            DIRECT,
        }

        Type type();

        default boolean local()
        {
            return type() != S3;
        }

        Optional<String> reference();

        Optional<byte[]> contents();

        String filename();

        @Value.Check
        default void validate()
        {
            if (type() == DIRECT) {
                Preconditions.checkArgument(!reference().isPresent());
                Preconditions.checkArgument(contents().isPresent());
            }
            else {
                Preconditions.checkArgument(reference().isPresent());
                Preconditions.checkArgument(!contents().isPresent());
            }
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface RemoteFile
    {
        FileReference reference();

        AmazonS3URI s3Uri();

        String relativePath();

        String localPath();
    }

    private interface Submitter
    {
        SubmissionResult submit();
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonDeserialize(as = ImmutableConfigurationJson.class)
    interface ConfigurationJson
    {
        @JsonProperty("Classification")
        Optional<String> classification();

        @JsonProperty("Configurations")
        List<ConfigurationJson> configurations();

        @JsonProperty("Properties")
        Map<String, String> properties();

        default Configuration toConfiguration()
        {
            return new Configuration()
                    .withClassification(classification().orNull())
                    .withConfigurations(configurations().stream()
                            .map(ConfigurationJson::toConfiguration)
                            .collect(Collectors.toList()))
                    .withProperties(properties());
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonDeserialize(as = ImmutableCommandRunnerConfiguration.class)
    @JsonSerialize(as = ImmutableCommandRunnerConfiguration.class)
    interface CommandRunnerConfiguration
    {
        List<DownloadConfig> download();

        Map<String, Parameter> env();

        List<Parameter> command();

        static ImmutableCommandRunnerConfiguration.Builder builder()
        {
            return ImmutableCommandRunnerConfiguration.builder();
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonDeserialize(as = ImmutableParameter.class)
    @JsonSerialize(as = ImmutableParameter.class)
    interface Parameter
    {
        String type();

        String value();

        static Parameter ofPlain(String value)
        {
            return ImmutableParameter.builder().type("plain").value(value).build();
        }

        static List<Parameter> ofPlain(String... values)
        {
            return ofPlain(asList(values));
        }

        static List<Parameter> ofPlain(Collection<String> values)
        {
            return values.stream().map(Parameter::ofPlain).collect(Collectors.toList());
        }

        static Parameter ofKmsEncrypted(String value)
        {
            return ImmutableParameter.builder().type("kms_encrypted").value(value).build();
        }

        static List<Parameter> ofKmsEncrypted(String... values)
        {
            return Stream.of(values).map(Parameter::ofKmsEncrypted).collect(Collectors.toList());
        }

        static List<Parameter> ofKmsEncrypted(Collection<String> values)
        {
            return values.stream().map(Parameter::ofKmsEncrypted).collect(Collectors.toList());
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonSerialize(as = ImmutableDownloadConfig.class)
    @JsonDeserialize(as = ImmutableDownloadConfig.class)
    interface DownloadConfig
    {
        String src();

        String dst();

        static DownloadConfig of(String src, String dst)
        {
            return ImmutableDownloadConfig.builder().src(src).dst(dst).build();
        }
    }
}
