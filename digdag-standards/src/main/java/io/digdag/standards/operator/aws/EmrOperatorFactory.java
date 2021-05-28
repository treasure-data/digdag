package io.digdag.standards.operator.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
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
import com.amazonaws.services.elasticmapreduce.model.FailureDetails;
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
import com.amazonaws.services.elasticmapreduce.model.StepStateChangeReason;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.core.Environment;
import io.digdag.spi.ImmutableTaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import io.digdag.util.RetryExecutor;
import io.digdag.util.UserSecretTemplate;
import io.digdag.util.Workspace;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.io.Closeables.closeQuietly;
import static io.digdag.standards.operator.aws.EmrOperatorFactory.FileReference.Type.DIRECT;
import static io.digdag.standards.operator.aws.EmrOperatorFactory.FileReference.Type.LOCAL;
import static io.digdag.standards.operator.aws.EmrOperatorFactory.FileReference.Type.S3;
import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class EmrOperatorFactory
        implements OperatorFactory
{
    private static final int LIST_STEPS_MAX_IDS = 10;

    private static final String LOCAL_STAGING_DIR = "/home/hadoop/digdag-staging";

    private static Logger logger = LoggerFactory.getLogger(EmrOperatorFactory.class);

    public String getType()
    {
        return "emr";
    }

    private final TemplateEngine templateEngine;
    private final ObjectMapper objectMapper;
    private final ConfigFactory cf;
    private final Map<String, String> environment;

    @Inject
    public EmrOperatorFactory(TemplateEngine templateEngine, ObjectMapper objectMapper, ConfigFactory cf, @Environment Map<String, String> environment)
    {
        this.templateEngine = templateEngine;
        this.objectMapper = objectMapper;
        this.cf = cf;
        this.environment = environment;
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new EmrOperator(context);
    }

    private class EmrOperator
            extends BaseOperator
    {
        private final TaskState state;
        private final Config params;
        private final String defaultActionOnFailure;

        public EmrOperator(OperatorContext context)
        {
            super(context);
            this.state = TaskState.of(request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws").getNestedOrGetEmpty("emr"))
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("aws"));
            this.defaultActionOnFailure = params.get("action_on_failure", String.class, "CANCEL_AND_WAIT");
        }

        @Override
        public TaskResult runTask()
        {
            String tag = state.constant("tag", String.class, EmrOperatorFactory::randomTag);

            AWSCredentials credentials = credentials(tag);

            SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
            SecretProvider s3Secrets = awsSecrets.getSecrets("s3");
            SecretProvider emrSecrets = awsSecrets.getSecrets("emr");
            SecretProvider kmsSecrets = awsSecrets.getSecrets("kms");

            Optional<String> s3RegionName = Aws.first(
                    () -> s3Secrets.getSecretOptional("region"),
                    () -> awsSecrets.getSecretOptional("region"),
                    () -> params.getOptional("s3.region", String.class));

            Optional<String> emrRegionName = Aws.first(
                    () -> emrSecrets.getSecretOptional("region"),
                    () -> awsSecrets.getSecretOptional("region"),
                    () -> params.getOptional("emr.region", String.class));

            Optional<String> kmsRegionName = Aws.first(
                    () -> kmsSecrets.getSecretOptional("region"),
                    () -> awsSecrets.getSecretOptional("region"),
                    () -> params.getOptional("kms.region", String.class));

            Optional<String> emrEndpoint = Aws.first(
                    () -> emrSecrets.getSecretOptional("endpoint"),
                    () -> params.getOptional("emr.endpoint", String.class),
                    () -> emrRegionName.transform(regionName -> "elasticmapreduce." + regionName + ".amazonaws.com"));

            Optional<String> s3Endpoint = Aws.first(
                    () -> s3Secrets.getSecretOptional("endpoint"),
                    () -> params.getOptional("s3.endpoint", String.class),
                    () -> s3RegionName.transform(regionName -> "s3." + regionName + ".amazonaws.com"));

            Optional<String> kmsEndpoint = Aws.first(
                    () -> kmsSecrets.getSecretOptional("endpoint"),
                    () -> params.getOptional("kms.endpoint", String.class),
                    () -> kmsRegionName.transform(regionName -> "kms." + regionName + ".amazonaws.com"));

            ClientConfiguration emrClientConfiguration = new ClientConfiguration();
            ClientConfiguration s3ClientConfiguration = new ClientConfiguration();
            ClientConfiguration kmsClientConfiguration = new ClientConfiguration();

            Aws.configureProxy(emrClientConfiguration, emrEndpoint, environment);
            Aws.configureProxy(s3ClientConfiguration, s3Endpoint, environment);
            Aws.configureProxy(kmsClientConfiguration, kmsEndpoint, environment);

            AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials, emrClientConfiguration);
            AmazonS3Client s3 = new AmazonS3Client(credentials, s3ClientConfiguration);
            AWSKMSClient kms = new AWSKMSClient(credentials, kmsClientConfiguration);

            Aws.configureServiceClient(s3, s3Endpoint, s3RegionName);
            Aws.configureServiceClient(emr, emrEndpoint, emrRegionName);
            Aws.configureServiceClient(kms, kmsEndpoint, kmsRegionName);

            // Set up file stager
            Optional<AmazonS3URI> staging = params.getOptional("staging", String.class).transform(s -> {
                try {
                    return new AmazonS3URI(s);
                }
                catch (IllegalArgumentException ex) {
                    throw new ConfigException("Invalid staging uri: '" + s + "'", ex);
                }
            });
            Filer filer = new Filer(s3, staging, workspace, templateEngine, params);

            // TODO: make it possible for operators to _reliably_ clean up
            boolean cleanup = false;

            try {
                TaskResult result = run(tag, emr, kms, filer);
                cleanup = true;
                return result;
            }
            catch (Throwable t) {
                boolean retry = t instanceof TaskExecutionException &&
                        ((TaskExecutionException) t).getRetryInterval().isPresent();
                cleanup = !retry;
                throw Throwables.propagate(t);
            }
            finally {
                if (cleanup) {
                    // Best effort clean up of staging
                    try {
                        filer.tryCleanup();
                    }
                    catch (Throwable t) {
                        logger.warn("Failed to clean up staging: {}", staging, t);
                    }
                }
                s3.shutdown();
                emr.shutdown();
            }
        }

        private AWSCredentials credentials(String tag)
        {
            SecretProvider awsSecrets = context.getSecrets().getSecrets("aws");
            SecretProvider emrSecrets = awsSecrets.getSecrets("emr");

            String accessKeyId = emrSecrets.getSecretOptional("access_key_id")
                    .or(() -> awsSecrets.getSecret("access_key_id"));

            String secretAccessKey = emrSecrets.getSecretOptional("secret_access_key")
                    .or(() -> awsSecrets.getSecret("secret_access_key"));

            AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

            Optional<String> roleArn = emrSecrets.getSecretOptional("role_arn")
                    .or(awsSecrets.getSecretOptional("role_arn"));

            if (!roleArn.isPresent()) {
                return credentials;
            }

            // use STS to assume role
            String roleSessionName = emrSecrets.getSecretOptional("role_session_name")
                    .or(awsSecrets.getSecretOptional("role_session_name"))
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

        private TaskResult run(String tag, AmazonElasticMapReduce emr, AWSKMSClient kms, Filer filer)
                throws IOException
        {
            ParameterCompiler parameterCompiler = new ParameterCompiler(kms, context);

            // Set up step compiler
            List<Config> steps = params.getListOrEmpty("steps", Config.class);
            StepCompiler stepCompiler = new StepCompiler(tag, steps, filer, parameterCompiler, objectMapper, defaultActionOnFailure);

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
                submitter = newClusterSubmitter(emr, tag, stepCompiler, cluster, filer, parameterCompiler);
            }
            else {
                // Cluster ID? Use existing cluster.
                String clusterId = params.get("cluster", String.class);
                submitter = existingClusterSubmitter(emr, tag, stepCompiler, clusterId, filer);
            }

            // Submit EMR job
            SubmissionResult submission = submitter.submit();

            // Wait for the steps to finish running
            if (!steps.isEmpty()) {
                waitForSteps(emr, submission);
            }

            return result(submission);
        }

        private void waitForSteps(AmazonElasticMapReduce emr, SubmissionResult submission)
        {
            String lastStepId = Iterables.getLast(submission.stepIds());
            pollingWaiter(state, "result")
                    .withWaitMessage("EMR job still running: %s", submission.clusterId())
                    .withPollInterval(DurationInterval.of(Duration.ofSeconds(15), Duration.ofMinutes(5)))
                    .awaitOnce(Step.class, pollState -> checkStepCompletion(emr, submission, lastStepId, pollState));
        }

        private Optional<Step> checkStepCompletion(AmazonElasticMapReduce emr, SubmissionResult submission, String lastStepId, TaskState pollState)
        {
            return pollingRetryExecutor(pollState, "poll")
                    .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                    .withRetryInterval(DurationInterval.of(Duration.ofSeconds(15), Duration.ofMinutes(5)))
                    .run(s -> {

                        ListStepsResult runningSteps = emr.listSteps(new ListStepsRequest()
                                .withClusterId(submission.clusterId())
                                .withStepStates("RUNNING"));

                        runningSteps.getSteps().stream().findFirst()
                                .ifPresent(step -> {
                                    int stepIndex = submission.stepIds().indexOf(step.getId());
                                    logger.info("Currently running EMR step {}/{}: {}: {}",
                                            stepIndex == -1 ? "?" : Integer.toString(stepIndex + 1),
                                            submission.stepIds().size(),
                                            step.getId(),
                                            step.getName());
                                });

                        Step lastStep = emr.describeStep(new DescribeStepRequest()
                                .withClusterId(submission.clusterId())
                                .withStepId(lastStepId))
                                .getStep();

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

                                List<StepSummary> steps = Lists.partition(submission.stepIds(), LIST_STEPS_MAX_IDS).stream()
                                        .flatMap(ids -> emr.listSteps(new ListStepsRequest()
                                                .withClusterId(submission.clusterId())
                                                .withStepIds(ids)).getSteps().stream())
                                        .collect(toList());

                                logger.error("EMR job failed: {}", submission.clusterId());

                                for (StepSummary step : steps) {
                                    StepStatus status = step.getStatus();
                                    FailureDetails details = status.getFailureDetails();
                                    StepStateChangeReason reason = status.getStateChangeReason();
                                    int stepIndex = submission.stepIds().indexOf(step.getId());
                                    logger.error("EMR step {}/{}: {}: state: {}, reason: {}, details: {}",
                                            stepIndex == -1 ? "?" : Integer.toString(stepIndex + 1),
                                            submission.stepIds().size(),
                                            step.getId(), status.getState(),
                                            reason != null ? reason : "{}",
                                            details != null ? details : "{}");
                                }

                                throw new TaskExecutionException("EMR job failed");

                            case "COMPLETED":
                                logger.info("EMR steps done");
                                return Optional.of(lastStep);

                            default:
                                throw new RuntimeException("Unknown step status: " + lastStep);
                        }
                    });
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

        private Submitter existingClusterSubmitter(AmazonElasticMapReduce emr, String tag, StepCompiler stepCompiler, String clusterId, Filer filer)
        {
            return () -> {
                List<String> stepIds = pollingRetryExecutor(state, "submission")
                        .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        .runOnce(new TypeReference<List<String>>() {}, s -> {

                            RemoteFile runner = prepareRunner(filer, tag);

                            // Compile steps
                            stepCompiler.compile(runner);

                            // Stage files to S3
                            filer.stageFiles();

                            AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
                                    .withJobFlowId(clusterId)
                                    .withSteps(stepCompiler.stepConfigs());

                            int steps = request.getSteps().size();
                            logger.info("Submitting {} EMR step(s) to {}", steps, clusterId);
                            AddJobFlowStepsResult result = emr.addJobFlowSteps(request);
                            logSubmittedSteps(clusterId, steps, i -> request.getSteps().get(i).getName(), i -> result.getStepIds().get(i));
                            return ImmutableList.copyOf(result.getStepIds());
                        });

                return SubmissionResult.ofExistingCluster(clusterId, stepIds);
            };
        }

        private void logSubmittedSteps(String clusterId, int n, Function<Integer, String> names, Function<Integer, String> ids)
        {
            logger.info("Submitted {} EMR step(s) to {}", n, clusterId);
            for (int i = 0; i < n; i++) {
                logger.info("Step {}/{}: {}: {}", i + 1, n, names.apply(i), ids.apply(i));
            }
        }

        private Submitter newClusterSubmitter(AmazonElasticMapReduce emr, String tag, StepCompiler stepCompiler, Config clusterConfig, Filer filer, ParameterCompiler parameterCompiler)
        {

            return () -> {
                // Start cluster
                NewCluster cluster = pollingRetryExecutor(state, "submission")
                        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        // TODO: EMR requests are not idempotent, thus retrying might produce duplicate cluster submissions.
                        .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                        .runOnce(NewCluster.class, s -> submitNewClusterRequest(emr, tag, stepCompiler, clusterConfig, filer, parameterCompiler));

                // Get submitted step IDs
                List<String> stepIds = pollingRetryExecutor(this.state, "steps")
                        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                        .runOnce(new TypeReference<List<String>>() {}, s -> {
                            List<StepSummary> steps = listSubmittedSteps(emr, tag, cluster);
                            logSubmittedSteps(cluster.id(), cluster.steps(), i -> steps.get(i).getName(), i -> steps.get(i).getId());
                            return steps.stream().map(StepSummary::getId).collect(toList());
                        });

                // Log cluster status while waiting for it to come up
                pollingWaiter(state, "bootstrap")
                        .withWaitMessage("EMR cluster still booting")
                        .withPollInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                        .awaitOnce(String.class, pollState -> checkClusterBootStatus(emr, cluster, pollState));

                return SubmissionResult.ofNewCluster(cluster.id(), stepIds);
            };
        }

        private Optional<String> checkClusterBootStatus(AmazonElasticMapReduce emr, NewCluster cluster, TaskState state)
        {
            // Only creating a cluster, with no steps?
            boolean createOnly = cluster.steps() == 0;

            DescribeClusterResult describeClusterResult = pollingRetryExecutor(state, "describe-cluster")
                    .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                    .retryUnless(AmazonServiceException.class, Aws::isDeterministicException)
                    .run(ds -> emr.describeCluster(new DescribeClusterRequest().withClusterId(cluster.id())));

            ClusterStatus clusterStatus = describeClusterResult.getCluster().getStatus();
            String clusterState = clusterStatus.getState();

            switch (clusterState) {
                case "STARTING":
                    logger.info("EMR cluster starting: {}", cluster.id());
                    return Optional.absent();
                case "BOOTSTRAPPING":
                    logger.info("EMR cluster bootstrapping: {}", cluster.id());
                    return Optional.absent();

                case "RUNNING":
                case "WAITING":
                    logger.info("EMR cluster up: {}", cluster.id());
                    return Optional.of(clusterState);

                case "TERMINATED_WITH_ERRORS":
                    if (createOnly) {
                        // TODO: log more information about the errors
                        // TODO: inspect state change reason to figure out whether it was the boot that failed or e.g. steps submitted by another agent
                        throw new TaskExecutionException("EMR boot failed: " + cluster.id());
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
        }

        private NewCluster submitNewClusterRequest(AmazonElasticMapReduce emr, String tag, StepCompiler stepCompiler,
                Config cluster, Filer filer, ParameterCompiler parameterCompiler)
                throws IOException
        {
            RemoteFile runner = prepareRunner(filer, tag);

            // Compile steps
            stepCompiler.compile(runner);

            List<StepConfig> stepConfigs = stepCompiler.stepConfigs();

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
                    .map(application -> new Application().withName(application))
                    .collect(toList());

            // TODO: merge configurations with the same classification?
            List<Configuration> configurations = cluster.getListOrEmpty("configurations", JsonNode.class).stream()
                    .map(this::configurations)
                    .flatMap(Collection::stream)
                    .collect(toList());

            List<JsonNode> bootstrap = cluster.getListOrEmpty("bootstrap", JsonNode.class);
            List<BootstrapActionConfig> bootstrapActions = new ArrayList<>();
            for (int i = 0; i < bootstrap.size(); i++) {
                bootstrapActions.add(bootstrapAction(i + 1, bootstrap.get(i), tag, filer, runner, parameterCompiler));
            }

            // Stage files to S3
            filer.stageFiles();

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
                            .withPlacement(cluster.getOptional("availability_zone", String.class)
                                    .transform(zone -> new PlacementType().withAvailabilityZone(zone)).orNull())
                            .withEc2SubnetId(subnetId.orNull())
                            .withEc2KeyName(ec2.get("key", String.class))
                            .withKeepJobFlowAliveWhenNoSteps(!cluster.get("auto_terminate", boolean.class, true)));

            logger.info("Submitting EMR job with {} steps(s)", request.getSteps().size());
            RunJobFlowResult result = emr.runJobFlow(request);
            logger.info("Submitted EMR job with {} step(s): {}", request.getSteps().size(), result.getJobFlowId(), result);

            return NewCluster.of(result.getJobFlowId(), request.getSteps().size());
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
                            .map(this::configurations)
                            .flatMap(Collection::stream)
                            .collect(toList()));
        }

        private EbsConfiguration ebsConfiguration(Config config)
        {
            return new EbsConfiguration()
                    .withEbsOptimized(config.get("optimized", Boolean.class, null))
                    .withEbsBlockDeviceConfigs(config.getListOrEmpty("devices", Config.class).stream()
                            .map(this::ebsBlockDeviceConfig)
                            .collect(toList()));
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

        private List<StepSummary> listSubmittedSteps(AmazonElasticMapReduce emr, String tag, NewCluster cluster)
        {
            List<StepSummary> steps = new ArrayList<>();
            ListStepsRequest request = new ListStepsRequest().withClusterId(cluster.id());
            while (steps.size() < cluster.steps()) {
                ListStepsResult result = emr.listSteps(request);
                for (StepSummary step : result.getSteps()) {
                    if (step.getName().contains(tag)) {
                        steps.add(step);
                    }
                }
                if (result.getMarker() == null) {
                    break;
                }
                request.setMarker(result.getMarker());
            }
            // The ListSteps api returns steps in reverse order. So reverse them to submission order.
            Collections.reverse(steps);
            return steps;
        }

        private BootstrapActionConfig bootstrapAction(int index, JsonNode action, String tag, Filer filer, RemoteFile runner, ParameterCompiler parameterCompiler)
                throws IOException
        {
            String script;
            String name;
            FileReference reference;

            Config config;
            if (action.isTextual()) {
                script = action.asText();
                reference = fileReference("bootstrap", script);
                name = reference.filename();
                config = request.getConfig().getFactory().create();
            }
            else if (action.isObject()) {
                config = request.getConfig().getFactory().create(action);
                script = config.get("path", String.class);
                reference = fileReference("bootstrap", script);
                name = config.get("name", String.class, reference.filename());
            }
            else {
                throw new ConfigException("Invalid bootstrap action: " + action);
            }

            RemoteFile file = filer.prepareRemoteFile(tag, "bootstrap", Integer.toString(index), reference, false);

            CommandRunnerConfiguration configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(bootstrapWorkingDirectory(index))
                    .env(parameterCompiler.parameters(config.getNestedOrGetEmpty("env"), (key, value) -> value))
                    .addDownload(DownloadConfig.of(file, 0777))
                    .addAllDownload(config.getListOrEmpty("files", String.class).stream()
                            .map(r -> fileReference("file", r))
                            .map(r -> filer.prepareRemoteFile(tag, "bootstrap", Integer.toString(index), r, false, bootstrapWorkingDirectory(index)))
                            .collect(toList()))
                    .addCommand(file.localPath())
                    .addAllCommand(parameterCompiler.parameters(config, "args"))
                    .build();

            FileReference configurationFileReference = ImmutableFileReference.builder()
                    .type(FileReference.Type.DIRECT)
                    .contents(objectMapper.writeValueAsBytes(configuration))
                    .filename("config.json")
                    .build();
            RemoteFile remoteConfigurationFile = filer.prepareRemoteFile(tag, "bootstrap", Integer.toString(index), configurationFileReference, false);

            return new BootstrapActionConfig()
                    .withName(name)
                    .withScriptBootstrapAction(new ScriptBootstrapActionConfig()
                            .withPath(runner.s3Uri().toString())
                            .withArgs(remoteConfigurationFile.s3Uri().toString()));
        }

        private String bootstrapWorkingDirectory(int index)
        {
            return LOCAL_STAGING_DIR + "/bootstrap/" + index + "/wd";
        }

        private List<Configuration> configurations(JsonNode node)
        {
            if (node.isTextual()) {
                // File
                String configurationJson;
                try {
                    configurationJson = workspace.templateFile(templateEngine, node.asText(), UTF_8, params);
                }
                catch (IOException | TemplateException e) {
                    throw new TaskExecutionException(e);
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
                        .collect(toList());
            }
            else if (node.isObject()) {
                // Embedded configuration
                Config config = cf.create(node);
                return ImmutableList.of(new Configuration()
                        .withConfigurations(config.getListOrEmpty("configurations", JsonNode.class).stream()
                                .map(this::configurations)
                                .flatMap(Collection::stream)
                                .collect(toList()))
                        .withClassification(config.get("classification", String.class, null))
                        .withProperties(config.get("properties", new TypeReference<Map<String, String>>() {}, null)));
            }
            else {
                throw new ConfigException("Invalid EMR configuration: '" + node + "'");
            }
        }

        @Override
        public boolean isBlocking()
        {
            return false;
        }
    }

    private static class Filer
    {
        private final AmazonS3Client s3;
        private final Optional<AmazonS3URI> staging;
        private final Workspace workspace;
        private final TemplateEngine templateEngine;
        private final Config params;

        private final List<StagingFile> files = new ArrayList<>();

        private final Set<String> ids = new HashSet<>();

        Filer(AmazonS3Client s3, Optional<AmazonS3URI> staging, Workspace workspace, TemplateEngine templateEngine, Config params)
        {
            this.s3 = s3;
            this.staging = staging;
            this.workspace = workspace;
            this.templateEngine = templateEngine;
            this.params = params;
        }

        RemoteFile prepareRemoteFile(String tag, String section, String path, FileReference reference, boolean template)
        {
            return prepareRemoteFile(tag, section, path, reference, template, null);
        }

        RemoteFile prepareRemoteFile(String tag, String section, String path, FileReference reference, boolean template, String localDir)
        {
            String id = randomTag(s -> !ids.add(s));

            String prefix = tag + "/" + section + "/" + path + "/" + id;

            if (localDir == null) {
                localDir = LOCAL_STAGING_DIR + "/" + prefix;
            }

            ImmutableRemoteFile.Builder builder =
                    ImmutableRemoteFile.builder()
                            .reference(reference)
                            .localPath(localDir + "/" + reference.filename());

            if (reference.local()) {
                // Local file? Then we need to upload it to S3.
                if (!staging.isPresent()) {
                    throw new ConfigException("Please configure a S3 'staging' directory");
                }
                String baseKey = staging.get().getKey();
                String key = (baseKey != null ? baseKey : "") + prefix + "/" + reference.filename();
                builder.s3Uri(new AmazonS3URI("s3://" + staging.get().getBucket() + "/" + key));
            }
            else {
                builder.s3Uri(new AmazonS3URI(reference.reference().get()));
            }

            RemoteFile remoteFile = builder.build();

            if (reference.local()) {
                files.add(StagingFile.of(template, remoteFile));
            }

            return remoteFile;
        }

        void stageFiles()
        {
            if (files.isEmpty()) {
                return;
            }

            TransferManager transferManager = new TransferManager(s3);
            List<PutObjectRequest> requests = new ArrayList<>();

            for (StagingFile f : files) {
                logger.info("Staging {} -> {}", f.file().reference().filename(), f.file().s3Uri());
                requests.add(stagingFilePutRequest(f));
            }

            try {
                List<Upload> uploads = requests.stream()
                        .map(transferManager::upload)
                        .collect(toList());

                for (Upload upload : uploads) {
                    try {
                        upload.waitForCompletion();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TaskExecutionException(e);
                    }
                }
            }
            finally {
                transferManager.shutdownNow(false);
                requests.forEach(r -> closeQuietly(r.getInputStream()));
            }
        }

        private PutObjectRequest stagingFilePutRequest(StagingFile file)
        {
            AmazonS3URI uri = file.file().s3Uri();
            FileReference reference = file.file().reference();

            switch (reference.type()) {
                case LOCAL: {
                    if (file.template()) {
                        String content;
                        try {
                            content = workspace.templateFile(templateEngine, reference.filename(), UTF_8, params);
                        }
                        catch (IOException | TemplateException e) {
                            throw new ConfigException("Failed to load file: " + file.file().reference().filename(), e);
                        }
                        byte[] bytes = content.getBytes(UTF_8);
                        ObjectMetadata metadata = new ObjectMetadata();
                        metadata.setContentLength(bytes.length);
                        return new PutObjectRequest(uri.getBucket(), uri.getKey(), new ByteArrayInputStream(bytes), metadata);
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
                        throw new TaskExecutionException(e);
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

        void tryCleanup()
        {
            if (!staging.isPresent()) {
                return;
            }

            String bucket = staging.get().getBucket();
            ListObjectsRequest req = new ListObjectsRequest()
                    .withBucketName(bucket)
                    .withPrefix(staging.get().getKey());
            do {
                ObjectListing res = s3.listObjects(req);
                String[] keys = res.getObjectSummaries().stream()
                        .map(S3ObjectSummary::getKey)
                        .toArray(String[]::new);
                for (String key : keys) {
                    logger.info("Removing s3://{}/{}", bucket, key);
                }
                try {
                    RetryExecutor.retryExecutor()
                            .withRetryLimit(3)
                            .withInitialRetryWait(100)
                            .retryIf(e -> !(e instanceof AmazonServiceException) || !Aws.isDeterministicException((AmazonServiceException) e))
                            .run(() -> s3.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys)));
                }
                catch (RetryExecutor.RetryGiveupException e) {
                    logger.info("Failed to delete staging files in {}", staging.get(), e.getCause());
                }

                req.setMarker(res.getMarker());
            }
            while (req.getMarker() != null);
        }
    }

    private static class StepCompiler
    {
        private final ParameterCompiler pc;
        private final String tag;
        private final List<Config> steps;
        private final Filer filer;
        private final ObjectMapper objectMapper;
        private final String defaultActionOnFailure;

        private List<StepConfig> configs;

        private int index;
        private Config step;
        private RemoteFile runner;

        StepCompiler(String tag, List<Config> steps, Filer filer, ParameterCompiler pc, ObjectMapper objectMapper, String defaultActionOnFailure)
        {
            this.tag = Preconditions.checkNotNull(tag, "tag");
            this.steps = Preconditions.checkNotNull(steps, "steps");
            this.filer = Preconditions.checkNotNull(filer, "filer");
            this.pc = Preconditions.checkNotNull(pc, "pc");
            this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
            this.defaultActionOnFailure = Preconditions.checkNotNull(defaultActionOnFailure, "defaultActionOnFailure");
            Preconditions.checkArgument(!steps.isEmpty(), "steps");
        }

        private void compile(RemoteFile runner)
                throws IOException
        {
            this.runner = runner;
            configs = new ArrayList<>();
            index = 1;
            for (int i = 0; i < steps.size(); i++, index++) {
                step = steps.get(i);
                String type = step.get("type", String.class);
                switch (type) {
                    case "flink":
                        flinkStep();
                        break;
                    case "hive":
                        hiveStep();
                        break;
                    case "spark":
                        sparkStep();
                        break;
                    case "spark-sql":
                        sparkSqlStep();
                        break;
                    case "script":
                        scriptStep();
                        break;
                    case "command":
                        commandStep();
                        break;
                    default:
                        throw new ConfigException("Unsupported step type: " + type);
                }
            }
        }

        List<StepConfig> stepConfigs()
        {
            Preconditions.checkState(configs != null);
            return configs;
        }

        private String localWorkingDirectory()
        {
            return LOCAL_STAGING_DIR + "/" + tag + "/steps/" + index + "/wd";
        }

        private RemoteFile prepareRemoteFile(FileReference reference, boolean template, String localDir)
        {
            return filer.prepareRemoteFile(tag, "steps", Integer.toString(index), reference, template, localDir);
        }

        private RemoteFile prepareRemoteFile(FileReference reference, boolean template)
        {
            return filer.prepareRemoteFile(tag, "steps", Integer.toString(index), reference, template);
        }

        private void sparkStep()
                throws IOException
        {
            FileReference applicationReference = fileReference("application", step);
            boolean scala = applicationReference.filename().endsWith(".scala");
            boolean python = applicationReference.filename().endsWith(".py");
            boolean script = scala || python;
            RemoteFile applicationFile = prepareRemoteFile(applicationReference, script);

            List<String> files = step.getListOrEmpty("files", String.class);
            List<RemoteFile> filesFiles = files.stream()
                    .map(r -> fileReference("file", r))
                    .map(r -> prepareRemoteFile(r, false, localWorkingDirectory()))
                    .collect(toList());

            List<String> filesArgs = filesFiles.isEmpty()
                    ? ImmutableList.of()
                    : ImmutableList.of("--files", filesFiles.stream().map(RemoteFile::localPath).collect(Collectors.joining(",")));

            List<String> jars = step.getListOrEmpty("jars", String.class);
            List<RemoteFile> jarFiles = jars.stream()
                    .map(r -> fileReference("jar", r))
                    .map(r -> prepareRemoteFile(r, false))
                    .collect(toList());

            List<String> jarArgs = jarFiles.isEmpty()
                    ? ImmutableList.of()
                    : ImmutableList.of("--jars", jarFiles.stream().map(RemoteFile::localPath).collect(Collectors.joining(",")));

            Config conf = step.getNestedOrderedOrGetEmpty("conf");
            List<Parameter> confArgs = pc.parameters("--conf", conf, (key, value) -> key + "=" + value);

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

            CommandRunnerConfiguration.Builder configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(localWorkingDirectory());

            configuration.addDownload(applicationFile);
            configuration.addAllDownload(jarFiles);
            configuration.addAllDownload(filesFiles);

            String command;
            List<String> applicationArgs;
            String deployMode;
            List<String> args = step.getListOrEmpty("args", String.class);
            if (scala) {
                // spark-shell needs the script to explicitly exit, otherwise it will wait forever for user input.
                // Fortunately spark-shell accepts multiple scripts on the command line, so we append a helper script to run last and exit the shell.
                // This could also have been accomplished by wrapping the spark-shell invocation in a bash session that concatenates the exit command onto the user script using
                // anonymous fifo's etc but that seems a bit more brittle. Also, this way the actual names of the scripts appear in logs instead of /dev/fd/47 etc.
                String exitHelperFilename = "exit-helper.scala";
                URL exitHelperResource = Resources.getResource(EmrOperatorFactory.class, exitHelperFilename);
                FileReference exitHelperFileReference = ImmutableFileReference.builder()
                        .reference(exitHelperResource.toString())
                        .type(FileReference.Type.RESOURCE)
                        .filename(exitHelperFilename)
                        .build();
                RemoteFile exitHelperFile = prepareRemoteFile(exitHelperFileReference, false);
                configuration.addDownload(exitHelperFile);

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

            configuration.addAllCommand(command, "--deploy-mode", deployMode);
            configuration.addAllCommand(step.getListOrEmpty("submit_options", String.class));
            configuration.addAllCommand(jarArgs);
            configuration.addAllCommand(filesArgs);
            configuration.addAllCommand(confArgs);
            configuration.addAllCommand(classArgs);
            configuration.addAllCommand(applicationArgs);

            addStep(name, configuration.build());
        }

        private void sparkSqlStep()
                throws IOException
        {
            FileReference wrapperFileReference = FileReference.ofResource("spark-sql-wrapper.py");
            RemoteFile wrapperFile = prepareRemoteFile(wrapperFileReference, false);

            FileReference queryReference = fileReference("query", step);
            RemoteFile queryFile = prepareRemoteFile(queryReference, true);

            List<String> jars = step.getListOrEmpty("jars", String.class);
            List<RemoteFile> jarFiles = jars.stream()
                    .map(r -> fileReference("jar", r))
                    .map(r -> prepareRemoteFile(r, false))
                    .collect(toList());

            List<String> jarArgs = jarFiles.isEmpty()
                    ? ImmutableList.of()
                    : ImmutableList.of("--jars", jarFiles.stream().map(RemoteFile::localPath).collect(Collectors.joining(",")));

            Config conf = step.getNestedOrderedOrGetEmpty("conf");
            List<Parameter> confArgs = pc.parameters("--conf", conf, (key, value) -> key + "=" + value);

            CommandRunnerConfiguration configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(localWorkingDirectory())
                    .addDownload(wrapperFile)
                    .addDownload(queryFile)
                    .addAllCommand("spark-submit")
                    .addAllCommand("--deploy-mode", step.get("deploy_mode", String.class, "cluster"))
                    .addAllCommand("--files", queryFile.localPath())
                    .addAllCommand(confArgs)
                    .addAllCommand(pc.parameters(step, "submit_options"))
                    .addAllCommand(jarArgs)
                    .addAllCommand(wrapperFile.localPath())
                    .addAllCommand(queryReference.filename(), step.get("result", String.class))
                    .build();

            addStep("Spark Sql", configuration);
        }

        private void scriptStep()
                throws IOException
        {
            FileReference scriptReference = fileReference("script", step);
            RemoteFile scriptFile = prepareRemoteFile(scriptReference, false);

            List<String> files = step.getListOrEmpty("files", String.class);
            List<RemoteFile> filesFiles = files.stream()
                    .map(r -> fileReference("file", r))
                    .map(r -> prepareRemoteFile(r, false, localWorkingDirectory()))
                    .collect(toList());

            CommandRunnerConfiguration configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(localWorkingDirectory())
                    .env(pc.parameters(step.getNestedOrGetEmpty("env"), (key, value) -> value))
                    .addDownload(DownloadConfig.of(scriptFile, 0777))
                    .addAllDownload(filesFiles)
                    .addAllCommand(scriptFile.localPath())
                    .addAllCommand(pc.parameters(step, "args"))
                    .build();

            addStep("Script", configuration);
        }

        private void flinkStep()
                throws IOException
        {
            String name = "Flink Application";

            FileReference fileReference = fileReference("application", step);
            RemoteFile remoteFile = prepareRemoteFile(fileReference, false);

            CommandRunnerConfiguration configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(localWorkingDirectory())
                    .addDownload(remoteFile)
                    .addAllCommand(
                            "flink", "run", "-m", "yarn-cluster",
                            "-yn", Integer.toString(step.get("yarn_containers", int.class, 2)),
                            remoteFile.localPath())
                    .addAllCommand(pc.parameters(step, "args"))
                    .build();

            addStep(name, configuration);
        }

        private void hiveStep()
                throws IOException
        {
            FileReference scriptReference = fileReference("script", step);
            RemoteFile remoteScript = prepareRemoteFile(scriptReference, false);

            CommandRunnerConfiguration configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(localWorkingDirectory())
                    .addAllCommand("hive-script", "--run-hive-script", "--args", "-f", remoteScript.s3Uri().toString())
                    .addAllCommand(pc.parameters("-d", step.getNestedOrGetEmpty("vars"), (key, value) -> key + "=" + value))
                    .addAllCommand(pc.parameters("-hiveconf", step.getNestedOrGetEmpty("hiveconf"), (key, value) -> key + "=" + value))
                    .build();

            addStep("Hive Script", configuration);
        }

        private void commandStep()
                throws IOException
        {
            CommandRunnerConfiguration configuration = CommandRunnerConfiguration.builder()
                    .workingDirectory(localWorkingDirectory())
                    .env(pc.parameters(step.getNestedOrGetEmpty("env"), (key, value) -> value))
                    .addAllDownload(step.getListOrEmpty("files", String.class).stream()
                            .map(r -> fileReference("file", r))
                            .map(r -> prepareRemoteFile(r, false, localWorkingDirectory()))
                            .collect(toList()))
                    .addAllCommand(step.get("command", String.class))
                    .addAllCommand(pc.parameters(step, "args"))
                    .build();

            addStep("Command", configuration);
        }

        private void addStep(String name, CommandRunnerConfiguration configuration)
                throws IOException
        {
            FileReference configurationFileReference = ImmutableFileReference.builder()
                    .type(FileReference.Type.DIRECT)
                    .contents(objectMapper.writeValueAsBytes(configuration))
                    .filename("config.json")
                    .build();
            RemoteFile remoteConfigurationFile = prepareRemoteFile(configurationFileReference, false);

            StepConfig runStep = stepConfig(name, tag, step)
                    .withHadoopJarStep(stepFactory().newScriptRunnerStep(runner.s3Uri().toString(), remoteConfigurationFile.s3Uri().toString()));

            configs.add(runStep);
        }

        private StepFactory stepFactory()
        {
            // TODO: configure region
            return new StepFactory();
        }

        private StepConfig stepConfig(String defaultName, String tag, Config step)
        {
            String name = step.get("name", String.class, defaultName);
            return new StepConfig()
                    .withName(name + " (" + tag + ")")
                    // TERMINATE_JOB_FLOW | TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE
                    .withActionOnFailure(step.get("action_on_failure", String.class, defaultActionOnFailure));
        }
    }

    private static class ParameterCompiler
    {
        private final AWSKMSClient kms;
        private final OperatorContext context;

        ParameterCompiler(AWSKMSClient kms, OperatorContext context)
        {
            this.kms = Preconditions.checkNotNull(kms, "kms");
            this.context = Preconditions.checkNotNull(context, "context");
        }

        private List<Parameter> parameters(String flag, Config config, BiFunction<String, String, String> f)
        {
            return parameters(config, f).values().stream()
                    .flatMap(p -> Stream.of(Parameter.ofPlain(flag), p))
                    .collect(toList());
        }

        private List<Parameter> parameters(Config config, String key)
        {
            return config.parseListOrGetEmpty(key, String.class).stream()
                    .map(value -> parameter(value, Function.identity()))
                    .collect(toList());
        }

        private Map<String, Parameter> parameters(Config config, BiFunction<String, String, String> f)
        {
            return config.getKeys().stream()
                    .collect(toMap(
                            Function.identity(),
                            key -> parameter(config.get(key, String.class), value -> f.apply(key, value))));
        }

        private Parameter parameter(String value, Function<String, String> f)
        {
            UserSecretTemplate secretTemplate = UserSecretTemplate.of(value);
            if (secretTemplate.containsSecrets()) {
                String secretValue = secretTemplate.format(context.getSecrets());
                return Parameter.ofKmsEncrypted(kmsEncrypt(f.apply(secretValue)));
            }
            else {
                return Parameter.ofPlain(f.apply(value));
            }
        }

        private String kmsEncrypt(String value)
        {
            String kmsKeyId = context.getSecrets().getSecret("aws.emr.kms_key_id");
            EncryptResult result = kms.encrypt(new EncryptRequest().withKeyId(kmsKeyId).withPlaintext(UTF_8.encode(value)));
            return base64(result.getCiphertextBlob());
        }

        private String base64(ByteBuffer bb)
        {
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    private static FileReference fileReference(String key, Config config)
    {
        String reference = config.get(key, String.class);
        return fileReference(key, reference);
    }

    private static FileReference fileReference(String key, String reference)
    {
        if (reference.startsWith("s3:")) {
            // File on S3
            AmazonS3URI uri;
            try {
                uri = new AmazonS3URI(reference);
                Preconditions.checkArgument(uri.getKey() != null & !uri.getKey().endsWith("/"), "must be a file");
            }
            catch (IllegalArgumentException e) {
                throw new ConfigException("Invalid " + key + ": '" + reference + "'", e);
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

    private static String randomTag()
    {
        byte[] bytes = new byte[8];
        ThreadLocalRandom.current().nextBytes(bytes);
        return BaseEncoding.base32().omitPadding().encode(bytes);
    }

    private static String randomTag(Function<String, Boolean> seen)
    {
        while (true) {
            String tag = randomTag();
            if (!seen.apply(tag)) {
                return tag;
            }
        }
    }

    private static RemoteFile prepareRunner(Filer filer, String tag)
    {
        URL commandRunnerResource = Resources.getResource(EmrOperatorFactory.class, "runner.py");
        FileReference commandRunnerFileReference = ImmutableFileReference.builder()
                .reference(commandRunnerResource.toString())
                .type(FileReference.Type.RESOURCE)
                .filename("runner.py")
                .build();
        return filer.prepareRemoteFile(tag, "shared", "scripts", commandRunnerFileReference, false);
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
        static FileReference ofResource(String name)
        {
            return ofResource(EmrOperatorFactory.class, name);
        }

        static FileReference ofResource(Class<?> contextClass, String name)
        {
            URL url = Resources.getResource(contextClass, name);
            return ImmutableFileReference.builder()
                    .reference(url.toString())
                    .type(Type.RESOURCE)
                    .filename(name)
                    .build();
        }

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
                            .collect(toList()))
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

        @JsonProperty("working_directory")
        String workingDirectory();

        Map<String, Parameter> env();

        List<Parameter> command();

        static Builder builder()
        {
            return new Builder();
        }

        class Builder
                extends ImmutableCommandRunnerConfiguration.Builder
        {
            Builder addDownload(RemoteFile remoteFile)
            {
                return addDownload(DownloadConfig.of(remoteFile));
            }

            Builder addAllDownload(RemoteFile... remoteFiles)
            {
                return addAllDownload(asList(remoteFiles));
            }

            Builder addAllDownload(Collection<RemoteFile> remoteFiles)
            {
                return addAllDownload(remoteFiles.stream()
                        .map(DownloadConfig::of)
                        .collect(toList()));
            }

            Builder addCommand(String command)
            {
                return addCommand(Parameter.ofPlain(command));
            }

            Builder addAllCommand(String... command)
            {
                return addAllCommand(asList(command));
            }

            Builder addAllCommand(Collection<String> command)
            {
                return addAllCommand(Parameter.ofPlain(command));
            }
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
            return values.stream().map(Parameter::ofPlain).collect(toList());
        }

        static Parameter ofKmsEncrypted(String value)
        {
            return ImmutableParameter.builder().type("kms_encrypted").value(value).build();
        }

        static List<Parameter> ofKmsEncrypted(String... values)
        {
            return Stream.of(values).map(Parameter::ofKmsEncrypted).collect(toList());
        }

        static List<Parameter> ofKmsEncrypted(Collection<String> values)
        {
            return values.stream().map(Parameter::ofKmsEncrypted).collect(toList());
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

        Optional<Integer> mode();

        static DownloadConfig of(String src, String dst)
        {
            return ImmutableDownloadConfig.builder().src(src).dst(dst).build();
        }

        static DownloadConfig of(String src, String dst, int mode)
        {
            return ImmutableDownloadConfig.builder().src(src).dst(dst).mode(mode).build();
        }

        static DownloadConfig of(RemoteFile remoteFile)
        {
            return of(remoteFile.s3Uri().toString(), remoteFile.localPath());
        }

        static DownloadConfig of(RemoteFile remoteFile, int mode)
        {
            return of(remoteFile.s3Uri().toString(), remoteFile.localPath(), mode);
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonSerialize(as = ImmutableNewCluster.class)
    @JsonDeserialize(as = ImmutableNewCluster.class)
    interface NewCluster
    {
        String id();

        int steps();

        static NewCluster of(String id, int steps)
        {
            return ImmutableNewCluster.builder()
                    .id(id)
                    .steps(steps)
                    .build();
        }
    }
}
