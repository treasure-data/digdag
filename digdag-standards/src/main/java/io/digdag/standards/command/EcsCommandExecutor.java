package io.digdag.standards.command;

import com.amazonaws.services.ecs.model.AssignPublicIp;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.LogConfiguration;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.ecs.model.TaskSetNotFoundException;
import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.ecs.EcsClient;
import io.digdag.standards.command.ecs.EcsClientConfig;
import io.digdag.standards.command.ecs.EcsClientFactory;
import io.digdag.standards.command.ecs.EcsTaskStatus;
import io.digdag.standards.command.ecs.TemporalProjectArchiveStorage;
import io.digdag.util.DurationParam;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class EcsCommandExecutor
        implements CommandExecutor
{
    private static Logger logger = LoggerFactory.getLogger(CommandExecutor.class);

    private static final String ECS_COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX = "agent.command_executor.ecs.";
    private static final String DEFAULT_COMMAND_TASK_TTL = ECS_COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX + "default_command_task_ttl";
    private static final String ECS_END_OF_TASK_LOG_MARK = "--RWNzQ29tbWFuZEV4ZWN1dG9y--"; // base64("EcsCommandExecutor")

    private static final String CONFIG_RETRY_TASK_SCRIPTS_DOWNLOADS = ECS_COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX + "retry_task_scripts_downloads";
    private static final String CONFIG_RETRY_TASK_OUTPUT_UPLOADS = ECS_COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX + "retry_task_output_uploads";
    private static final int DEFAULT_RETRY_TASK_SCRIPTS_DOWNLOADS = 8;
    private static final int DEFAULT_RETRY_TASK_OUTPUT_UPLOADS = 7;
    private static final String CONFIG_ENABLE_CURL_FAIL_OPT_ON_UPLOADS = ECS_COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX + "enable_curl_fail_opt_on_uploads";

    private final Config systemConfig;
    private final EcsClientFactory ecsClientFactory;
    private final DockerCommandExecutor docker;
    private final StorageManager storageManager;
    private final ProjectArchiveLoader projectArchiveLoader;
    private final CommandLogger clog;
    private final Optional<Duration> defaultCommandTaskTTL;
    private final int retryDownloads, retryUploads;
    private final boolean curlFailOptOnUploads; // false by the default

    @Inject
    public EcsCommandExecutor(
            final Config systemConfig,
            final EcsClientFactory ecsClientFactory,
            final DockerCommandExecutor docker,
            final StorageManager storageManager,
            final ProjectArchiveLoader projectArchiveLoader,
            final CommandLogger clog)
    {
        this.systemConfig = systemConfig;
        this.ecsClientFactory = ecsClientFactory;
        this.docker = docker;
        this.storageManager = storageManager;
        this.projectArchiveLoader = projectArchiveLoader;
        this.clog = clog;
        this.defaultCommandTaskTTL = systemConfig.getOptional(DEFAULT_COMMAND_TASK_TTL, DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(Optional.absent());
        this.retryDownloads = systemConfig.get(CONFIG_RETRY_TASK_SCRIPTS_DOWNLOADS, int.class, DEFAULT_RETRY_TASK_SCRIPTS_DOWNLOADS);
        this.retryUploads = systemConfig.get(CONFIG_RETRY_TASK_OUTPUT_UPLOADS, int.class, DEFAULT_RETRY_TASK_OUTPUT_UPLOADS);
        this.curlFailOptOnUploads = systemConfig.get(CONFIG_ENABLE_CURL_FAIL_OPT_ON_UPLOADS, boolean.class, false);
    }

    @Override
    public CommandStatus run(
            final CommandContext commandContext,
            final CommandRequest commandRequest)
            throws IOException
    {
        final Config taskConfig = commandContext.getTaskRequest().getConfig();
        final Optional<String> clusterName = Optional.absent();
        try {
            final EcsClientConfig clientConfig = createEcsClientConfig(clusterName, systemConfig, taskConfig); // ConfigException
            try (final EcsClient client = ecsClientFactory.createClient(clientConfig)) { // ConfigException
                final TaskDefinition td = findTaskDefinition(commandContext, client, taskConfig); // ConfigException
                // When RuntimeException is thrown by submitTask method, it will be handled and retried by BaseOperator, which is one of base
                // classes of operator implementation.
                final Task runTask = submitTask(commandContext, commandRequest, client, td); // ConfigException, RuntimeException
                final Optional<ObjectNode> awsLogs = getAwsLogsConfiguration(td, runTask.getTaskArn()); // Nullable, ConfigException

                final ObjectNode currentStatus = createCurrentStatus(commandRequest, clientConfig, runTask, awsLogs);
                return EcsCommandStatus.of(false, currentStatus);
            }
        }
        catch (ConfigException e) {
            logger.debug("Fall back to DockerCommandExecutor: {}", e.toString());
            return docker.run(commandContext, commandRequest); // fall back to DockerCommandExecutor
        }
    }

    protected Task submitTask(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final EcsClient client,
            final TaskDefinition td)
            throws ConfigException
    {
        final EcsClientConfig clientConfig = client.getConfig();
        final RunTaskRequest runTaskRequest = buildRunTaskRequest(commandContext, commandRequest, clientConfig, td); // RuntimeException,ConfigException
        final RunTaskResult runTaskResult = client.submitTask(runTaskRequest); // RuntimeException, ConfigException
        return findTask(td.getTaskDefinitionArn(), runTaskResult); // RuntimeException
    }

    protected TaskDefinition findTaskDefinition(
            final CommandContext commandContext,
            final EcsClient client,
            final Config taskConfig)
            throws ConfigException
    {
        TaskDefinition td;

        // find task definition by ecs.task_definition_arn
        td = findTaskDefinitionByTaskDefinitionArn(commandContext, client, taskConfig); // ConfigException
        if (td != null) {
            return td;
        }

        // find task definition by docker.image
        td = findTaskDefinitionByTaskTags(commandContext, client, taskConfig); // ConfigException
        if (td != null) {
            return td;
        }

        throw new ConfigException("Parameter ecs.task_definition_arn: or docker.image: is required but not set.");
    }

    @Nullable
    protected TaskDefinition findTaskDefinitionByTaskDefinitionArn(
            final CommandContext commandContext,
            final EcsClient client,
            final Config taskConfig)
            throws ConfigException
    {
        if (taskConfig.has("ecs")) {
            final Config ecsConfig = taskConfig.getNested("ecs");
            if (ecsConfig.has("task_definition_arn")) {
                final String taskDefinitionArn = ecsConfig.get("task_definition_arn", String.class);
                final TaskDefinition td = client.getTaskDefinition(taskDefinitionArn); // ConfigException
                if (td.getContainerDefinitions().size() > 1) {
                    throw new ConfigException("Task definition must not have multiple container definitions: " + taskDefinitionArn);
                }
                return td;
            }
        }
        return null;
    }

    @Nullable
    protected TaskDefinition findTaskDefinitionByTaskTags(
            final CommandContext commandContext,
            final EcsClient client,
            final Config taskConfig)
            throws ConfigException
    {
        if (taskConfig.has("docker")) {
            final Config dockerConfig = taskConfig.getNested("docker");
            if (!dockerConfig.has("image")) {
                throw new ConfigException("Parameter docker.image: is required but not set");
            }

            final Tag dockerImageTag = new Tag()
                    .withKey("digdag.docker.image")
                    .withValue(dockerConfig.get("image", String.class));
            final List<Tag> tags = ImmutableList.of(dockerImageTag);
            final Optional<TaskDefinition> td = client.getTaskDefinitionByTags(tags);
            if (!td.isPresent()) {
                throw new ConfigException("Not found task definition with 'digdag.docker.image' tag: " + dockerImageTag.getValue());
            }

            if (td.get().getContainerDefinitions().size() > 1) {
                throw new ConfigException("Task definition must not have multiple container definitions: " + td.get().getTaskDefinitionArn());
            }

            return td.get();
        }
        return null;
    }

    protected Optional<ObjectNode> getAwsLogsConfiguration(final TaskDefinition td, final String taskArn)
            throws ConfigException
    {
        // Assume that a single container will run in the task.
        final ContainerDefinition cd = td.getContainerDefinitions().get(0);
        final LogConfiguration logConfig = cd.getLogConfiguration();
        final String logDriver = logConfig.getLogDriver();
        final Optional<ObjectNode> awsLogs;
        if (logDriver.equals("awslogs")) {
            final ObjectNode logs = JsonNodeFactory.instance.objectNode();
            final String streamPrefix = logConfig.getOptions().get("awslogs-stream-prefix");
            final String containerDefinitionName = td.getContainerDefinitions().get(0).getName();
            final String taskArnPostfix = taskArn.substring(taskArn.lastIndexOf('/') + 1);
            final String awsLogsStream = String.format(Locale.ENGLISH, "%s/%s/%s", streamPrefix, containerDefinitionName, taskArnPostfix);
            logs.put("awslogs-group", logConfig.getOptions().get("awslogs-group"));
            logs.put("awslogs-stream", awsLogsStream);

            awsLogs = Optional.of(logs);
        }
        else {
            logger.warn("Not use 'awslogs' as log driver. EcsCommandExecutor doesn't fetch any task logs.");
            awsLogs = Optional.absent();
        }
        return awsLogs;
    }

    protected ObjectNode createCurrentStatus(
            final CommandRequest commandRequest,
            final EcsClientConfig clientConfig,
            final Task runTask,
            final Optional<ObjectNode> awsLogs)
    {
        final Path ioDirectoryPath = commandRequest.getIoDirectory(); // relative
        final ObjectNode currentStatus = JsonNodeFactory.instance.objectNode();
        currentStatus.put("cluster_name", clientConfig.getClusterName());
        currentStatus.put("task_arn", runTask.getTaskArn());
        currentStatus.put("task_creation_timestamp", runTask.getCreatedAt().getTime() / 1000);
        currentStatus.put("io_directory", ioDirectoryPath.toString());
        currentStatus.put("executor_state", JsonNodeFactory.instance.objectNode());
        currentStatus.put("awslogs", awsLogs.isPresent() ? awsLogs.get() : JsonNodeFactory.instance.nullNode());
        return currentStatus;
    }

    @Override
    public CommandStatus poll(
            final CommandContext commandContext,
            final ObjectNode previousStatus)
            throws IOException
    {
        // If executor is falled back in run method, this poll method needs not to be falled back because it's never
        // used in fall back mode. Please see more details of scripting operators.
        final Config taskConfig = commandContext.getTaskRequest().getConfig();
        final String clusterName = previousStatus.get("cluster_name").asText();
        final EcsClientConfig clientConfig = createEcsClientConfig(Optional.of(clusterName), systemConfig, taskConfig); // ConfigException
        try (final EcsClient client = ecsClientFactory.createClient(clientConfig)) { // ConfigException
            return createNextCommandStatus(commandContext, client, previousStatus);
        }
    }

    CommandStatus createNextCommandStatus(
            final CommandContext commandContext,
            final EcsClient client,
            final ObjectNode previousStatus)
            throws IOException
    {
        ObjectNode previousExecutorStatus = (ObjectNode) previousStatus.get("executor_state");
        ObjectNode nextExecutorStatus;

        // To fetch log until all logs is written in CloudWatch, it should wait until getting finish marker in end of task.
        // (finished previous poll once considering risk of crushing while running previous actual poll.)
        final Optional<Long> taskFinishedAt = !previousStatus.has("task_finished_at") ?
                Optional.absent() : Optional.of(previousStatus.get("task_finished_at").asLong());
        if (taskFinishedAt.isPresent()) {
            long timeout = taskFinishedAt.get() + 60;
            do {
                previousExecutorStatus = fetchLogEvents(client, previousStatus, previousExecutorStatus);
                if (previousExecutorStatus.get("logging_finished_at") != null) {
                    break;
                }
            } while (Instant.now().getEpochSecond() < timeout);

            final String outputArchivePathName = "archive-output.tar.gz";
            final String outputArchiveKey = createStorageKey(commandContext.getTaskRequest(), outputArchivePathName); // url format
            // Download output config archive
            final TemporalProjectArchiveStorage temporalStorage = createTemporalProjectArchiveStorage(commandContext.getTaskRequest().getConfig());
            try (final InputStream in = temporalStorage.getContentInputStream(outputArchiveKey)) {
                ProjectArchives.extractTarArchive(commandContext.getLocalProjectPath(), in); // IOException
            }

            final ObjectNode nextStatus = previousStatus.deepCopy();
            nextStatus.set("executor_state", previousExecutorStatus);

            return EcsCommandStatus.of(true, nextStatus);
        }

        final String cluster = previousStatus.get("cluster_name").asText();
        final String taskArn = previousStatus.get("task_arn").asText();
        final Task task;
        try {
            task = client.getTask(cluster, taskArn);
        } catch (TaskSetNotFoundException e) {
            // if task is not present, an operator will throw TaskExecutionException to retry polling the status.
            logger.info("Cannot get the Ecs task status. Will be retried.");
            return EcsCommandStatus.of(false, previousStatus.deepCopy());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Get task: " + task);
        }

        final EcsTaskStatus taskStatus = EcsTaskStatus.of(task.getLastStatus());
        // If the container doesn't start yet, it cannot extract any log messages from the container.
        if (taskStatus.isSameOrAfter(EcsTaskStatus.RUNNING)) {
            // if previous status has 'awslogs' log driver configuration, it tries to fetch log events by that.
            // Otherwise, it skips fetching logs.
            if (!previousStatus.get("awslogs").isNull()) { // awslogs?
                nextExecutorStatus = fetchLogEvents(client, previousStatus, previousExecutorStatus);
            }
            else {
                nextExecutorStatus = previousExecutorStatus.deepCopy();
            }
        }
        else { // before running
            // Write task status to the command logger to avoid users confusing.
            // It sometimes takes long time to start task running on AWS ECS by several reasons.
            log(s("Wait running a command task: status %s", taskStatus.getName()), clog);
            nextExecutorStatus = previousExecutorStatus.deepCopy();
        }

        final ObjectNode nextStatus = previousStatus.deepCopy();
        nextStatus.set("executor_state", nextExecutorStatus);

        if (taskStatus.isFinished()) {
            // To fetch log until all logs is written in CloudWatch,
            // finish this poll once and wait finish marker in head of this method in next poll, considering risk of crushing in this poll.
            nextStatus.put("task_finished_at", Instant.now().getEpochSecond());
            // Set exit code of container finished to nextStatus
            nextStatus.put("status_code", task.getContainers().get(0).getExitCode());
        }
        else if (defaultCommandTaskTTL.isPresent() && isRunningLongerThanTTL(previousStatus)) {
            final TaskRequest request = commandContext.getTaskRequest();
            final long attemptId = request.getAttemptId();
            final long taskId = request.getTaskId();

            final String message = s("Command task execution timeout: attempt=%d, task=%d", attemptId, taskId);
            logger.warn(message);

            logger.info(s("Stop command task %d", task.getTaskArn()));
            client.stopTask(cluster, taskArn);

            // Throw exception to stop the task as failure
            throw new TaskExecutionException(message);
        }
        // always return false to check if all logs are fetched. (return in head of this method after checking finish marker.)
        return EcsCommandStatus.of(false, nextStatus);
    }

    @VisibleForTesting
    void waitWithRandomJitter(long baseWaitSecs, long baseJitterSecs)
    {
        try {
            long jitterSecs = (long) (baseJitterSecs * Math.random());
            Thread.sleep((baseWaitSecs + jitterSecs) * 1000);
        }
        catch (InterruptedException ex) {
            // Nothing to do
        }
    }

    ObjectNode fetchLogEvents(final EcsClient client,
            final ObjectNode previousStatus,
            final ObjectNode previousExecutorStatus)
            throws IOException
    {
        final Optional<String> previousToken = !previousExecutorStatus.has("next_token") ?
                Optional.absent() : Optional.of(previousExecutorStatus.get("next_token").asText());
        final GetLogEventsResult result = client.getLog(toLogGroupName(previousStatus), toLogStreamName(previousStatus), previousToken);
        final List<OutputLogEvent> logEvents = result.getEvents();
        final String nextForwardToken = result.getNextForwardToken().substring(2); // trim "f/" prefix of the token
        final String nextBackwardToken = result.getNextBackwardToken().substring(2); // trim "b/" prefix of the token

        final ObjectNode nextExecutorStatus = previousExecutorStatus.deepCopy();
        if (!previousToken.isPresent() && nextForwardToken.equals(nextBackwardToken)) {
            // just in case, we'd better to drop "next_token" key
            nextExecutorStatus.remove("next_token");
        }
        else {
            for (final OutputLogEvent logEvent : logEvents) {
                String log = logEvent.getMessage();
                if (log.contains(ECS_END_OF_TASK_LOG_MARK)) {
                    nextExecutorStatus.put("logging_finished_at", Instant.now().getEpochSecond());
                }
                else {
                    log(log + "\n", clog);
                }
            }
            nextExecutorStatus.set("next_token", JsonNodeFactory.instance.textNode(nextForwardToken));
        }
        return nextExecutorStatus;
    }

    private static String toLogGroupName(final ObjectNode previousStatus)
    {
        return previousStatus.get("awslogs").get("awslogs-group").asText();
    }

    private static String toLogStreamName(final ObjectNode previousStatus)
    {
        return previousStatus.get("awslogs").get("awslogs-stream").asText();
    }

    private boolean isRunningLongerThanTTL(final ObjectNode previousStatus)
    {
        long creationTimestamp = previousStatus.get("pod_creation_timestamp").asLong();
        long currentTimestamp = Instant.now().getEpochSecond();
        return currentTimestamp > creationTimestamp + defaultCommandTaskTTL.get().getSeconds();
    }

    static class EcsCommandStatus
            implements CommandStatus
    {
        static EcsCommandStatus of(final boolean isFinished, final ObjectNode json)
        {
            return new EcsCommandStatus(isFinished, json);
        }

        private final boolean isFinished;
        private final ObjectNode json;

        private EcsCommandStatus(final boolean isFinished,
                final ObjectNode json)
        {
            this.isFinished = isFinished;
            this.json = json;
        }

        @Override
        public boolean isFinished()
        {
            return isFinished;
        }

        @Override
        public int getStatusCode()
        {
            return json.get("status_code").intValue();
        }

        @Override
        public String getIoDirectory()
        {
            return json.get("io_directory").textValue();
        }

        @Override
        public ObjectNode toJson()
        {
            return json;
        }
    }

    protected Task findTask(final String taskDefinitionArn, final RunTaskResult result) {
        for (final Task t : result.getTasks()) {
            if (t.getTaskDefinitionArn().equals(taskDefinitionArn)) {
                return t;
            }
        }
        throw new RuntimeException("Submitted task could not be found"); // TODO the message should be improved more understandably.
    }

    EcsClientConfig createEcsClientConfig(
            final Optional<String> clusterName,
            final Config systemConfig,
            final Config config)
    {
        return EcsClientConfig.of(clusterName, systemConfig, config);
    }

    RunTaskRequest buildRunTaskRequest(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final EcsClientConfig clientConfig,
            final TaskDefinition td)
            throws ConfigException
    {
        final RunTaskRequest runTaskRequest = new RunTaskRequest();
        setEcsCluster(clientConfig, runTaskRequest);
        setEcsGroup(runTaskRequest);
        setEcsTaskDefinition(commandContext, commandRequest, td, runTaskRequest);
        setEcsTaskCount(runTaskRequest);
        setEcsTaskOverride(commandContext, commandRequest, td, runTaskRequest); // RuntimeException,ConfigException
        setEcsTaskLaunchType(clientConfig, runTaskRequest);
        setEcsNetworkConfiguration(clientConfig, runTaskRequest);
        return runTaskRequest;
    }

    protected void setEcsTaskDefinition(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final TaskDefinition td,
            final RunTaskRequest request)
    {
        request.withTaskDefinition(td.getTaskDefinitionArn());
    }

    protected void setEcsCluster(final EcsClientConfig clientConfig, final RunTaskRequest request)
    {
        request.withCluster(clientConfig.getClusterName());
    }

    protected void setEcsTaskCount(final RunTaskRequest request)
    {
        request.withCount(1);
    }

    protected void setEcsGroup(final RunTaskRequest request)
    {
    }

    protected void setEcsTaskOverride(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final TaskDefinition td,
            final RunTaskRequest request)
            throws ConfigException
    {
        final ContainerOverride containerOverride = new ContainerOverride();
        // Assume that a single container will run in the task.
        final ContainerDefinition cd = td.getContainerDefinitions().get(0);
        setEcsContainerOverrideName(commandContext, commandRequest, containerOverride, cd);
        setEcsContainerOverrideCommand(commandContext, commandRequest, containerOverride); // RuntimeException,ConfigException
        setEcsContainerOverrideEnvironment(commandContext, commandRequest, containerOverride);
        setEcsContainerOverrideResource(commandContext, commandRequest, containerOverride);

        final TaskOverride taskOverride = new TaskOverride();
        taskOverride.withContainerOverrides(containerOverride);
        request.withOverrides(taskOverride);

        //final ContainerOverride containerOverride = new ContainerOverride();
        //containerOverride.withName()
        //containerOverride.withCommand()
        //containerOverride.withCpu();
        //containerOverride.withMemory();
        //containerOverride.withMemoryReservation();
        //containerOverride.withResourceRequirements();

        //final TaskOverride taskOverride = new TaskOverride();
        //taskOverride.withContainerOverrides();
        //taskOverride.withExecutionRoleArn();
        //taskOverride.withTaskRoleArn();
        //request.withOverrides(taskOverride);
    }

    protected void setEcsContainerOverrideName(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final ContainerOverride containerOverride,
            final ContainerDefinition cd)
    {
        containerOverride.withName(cd.getName());
    }

    TemporalProjectArchiveStorage createTemporalProjectArchiveStorage(final Config taskConfig)
            throws ConfigException
    {
        return TemporalProjectArchiveStorage.of(storageManager, systemConfig);
    }

    protected void setEcsContainerOverrideCommand(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final ContainerOverride containerOverride)
            throws ConfigException
    {
        final Path projectPath = commandContext.getLocalProjectPath();
        final Path ioDirectoryPath = commandRequest.getIoDirectory(); // relative
        final Config taskConfig = commandContext.getTaskRequest().getConfig();

        final TemporalProjectArchiveStorage temporalStorage = createTemporalProjectArchiveStorage(taskConfig); // config exception

        // Create project archive on local. Input contents, e.g. input config file and runner script, are included
        // in the project archive. It will be uploaded on temporal config storage and then, will be downloaded on
        // the container. The download URL is generated by pre-signed URL.
        final Path projectArchivePath;
        try {
            projectArchivePath = CommandExecutors.createArchiveFromLocal(projectArchiveLoader, projectPath,
                    commandRequest.getIoDirectory(), commandContext.getTaskRequest()); // IOException
        }
        catch (IOException e) {
            final String message = s("Cannot archive the project archive. It will be retried.");
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }

        final Path relativeProjectArchivePath = projectPath.relativize(projectArchivePath); // relative
        final Path lastPathElementOfArchivePath = projectPath.resolve(".digdag/tmp").relativize(projectArchivePath);
        final String projectArchiveStorageKey = createStorageKey(commandContext.getTaskRequest(), lastPathElementOfArchivePath.toString());
        final String projectArchiveDirectDownloadUrl;
        try {
            // Upload the temporal project archive on the temporal storage with the storage key
            temporalStorage.uploadFile(projectArchiveStorageKey, projectArchivePath); // IOException
            projectArchiveDirectDownloadUrl = temporalStorage.getDirectDownloadUrl(projectArchiveStorageKey);
        }
        catch (IOException e) {
            final String message = s("Cannot upload a temporal project archive '%s'with storage key '%s'. It will be retried.", projectArchivePath.toString(), projectArchiveStorageKey);
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }
        finally {
            try {
                Files.deleteIfExists(projectArchivePath); // IOException but ignored
            }
            catch (IOException e) {
                // can be ignored because agent will delete project dir once the task will be finished.
                logger.info(s("Cannot remove a temporal project archive: %s", projectArchivePath.toString()));
            }
        }

        // Create output archive path in the container
        // Upload the archive file to the S3 bucket
        final String outputProjectArchivePathName = ".digdag/tmp/archive-output.tar.gz"; // relative
        final String outputProjectArchiveStorageKey = createStorageKey(commandContext.getTaskRequest(), "archive-output.tar.gz");
        final String outputProjectArchiveDirectUploadUrl = temporalStorage.getDirectUploadUrl(outputProjectArchiveStorageKey);

        // Build command line arguments that will be passed to Kubernetes API here
        final ImmutableList.Builder<String> bashArguments = ImmutableList.builder();
        // TODO
        // Revisit we need it or not for debugging. If the command will be enabled, pods will show commands are executed
        // by the executor and will include pre-signed URLs in the commands.
        //bashArguments.add("set -eux");
        bashArguments.add(s("mkdir -p %s", ioDirectoryPath.toString()));
        // retry by exponential backoff 1+2+4+8+16+32+64+128=255 seconds by the default to avoid temporary network outage and/or S3 errors
        // exit 22 on non-transient http errors so that task can be retried
        bashArguments.add(s("curl --retry %d --retry-connrefused --fail -s \"%s\" --output %s", retryDownloads, projectArchiveDirectDownloadUrl, relativeProjectArchivePath.toString()));
        bashArguments.add(s("tar -zxf %s", relativeProjectArchivePath.toString()));
        if (!commandRequest.getWorkingDirectory().toString().isEmpty()) {
            bashArguments.add(s("pushd %s", commandRequest.getWorkingDirectory().toString()));
        }
        bashArguments.addAll(setEcsContainerOverrideArgumentsBeforeCommand());
        // Add command passed from operator
        bashArguments.add(commandRequest.getCommandLine().stream().map(Object::toString).collect(Collectors.joining(" ")));
        bashArguments.add(s("exit_code=$?"));
        bashArguments.addAll(setEcsContainerOverrideArgumentsAfterCommand());
        if (!commandRequest.getWorkingDirectory().toString().isEmpty()) {
            bashArguments.add(s("popd"));
        }
        bashArguments.add(s("tar -zcf %s  --exclude %s --exclude %s .digdag/tmp/", outputProjectArchivePathName, relativeProjectArchivePath.toString(), outputProjectArchivePathName));
        // retry by exponential backoff 1+2+4+8+16+32+64=127 seconds by the default
        // Note that it's intended to curl exit 0 on http errors since it's only for LogWatch logging
        bashArguments.add(s("curl --retry %d --retry-connrefused%s -s -X PUT -T %s -L \"%s\"", retryUploads, curlFailOptOnUploads ? " --fail": "", outputProjectArchivePathName, outputProjectArchiveDirectUploadUrl));
        bashArguments.add(s("echo \"%s\"", ECS_END_OF_TASK_LOG_MARK));
        bashArguments.add(s("exit $exit_code"));

        final List<String> bashCommand = ImmutableList.<String>builder()
                .add("/bin/bash")
                .add("-c")
                .add(bashArguments.build().stream().map(Object::toString).collect(Collectors.joining("; ")))
                .build();
        logger.debug("Submit command line arguments: " + bashCommand);

        containerOverride.withCommand(bashCommand);
    }

    protected List<String> setEcsContainerOverrideArgumentsBeforeCommand()
    {
        return ImmutableList.of();
    }


    protected List<String> setEcsContainerOverrideArgumentsAfterCommand()
    {
        return ImmutableList.of();
    }

    protected void setEcsContainerOverrideEnvironment(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final ContainerOverride containerOverride)
    {
        final ImmutableList.Builder<KeyValuePair> environments = ImmutableList.builder();
        for (final Map.Entry<String, String> e : commandRequest.getEnvironments().entrySet()) {
            environments.add(new KeyValuePair().withName(e.getKey()).withValue(e.getValue()));
        }
        containerOverride.withEnvironment(environments.build());
    }

    protected void setEcsContainerOverrideResource(
            final CommandContext commandContext,
            final CommandRequest commandRequest,
            final ContainerOverride containerOverride)
    { }

    private static void log(final String message, final CommandLogger to)
            throws IOException
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        final InputStream in = new ByteArrayInputStream(bytes);
        to.copy(in, System.out);
    }

    private static String s(final String format, final Object... args)
    {
        return String.format(Locale.ENGLISH, format, args);
    }

    private static String createStorageKey(final TaskRequest request, final String lastPathElementOfArchiveFile)
    {
        // file key: {taskId}/{lastPathElementOfArchiveFile}
        return new StringBuilder()
                .append(request.getTaskId()).append("/")
                .append(lastPathElementOfArchiveFile)
                .toString();
    }

    protected void setEcsTaskLaunchType(final EcsClientConfig clientConfig, final RunTaskRequest request)
    {
        final String type = clientConfig.getLaunchType();
        final LaunchType launchType = LaunchType.fromValue(type);
        request.withLaunchType(launchType);
    }

    protected void setEcsNetworkConfiguration(final EcsClientConfig clientConfig, final RunTaskRequest request)
    {
        request.withNetworkConfiguration(new NetworkConfiguration().withAwsvpcConfiguration(
                new AwsVpcConfiguration()
                        .withSubnets(clientConfig.getSubnets())
                        .withAssignPublicIp(AssignPublicIp.ENABLED) // TODO should be extracted
                ));

        //final AwsVpcConfiguration awsVpcConfig = new AwsVpcConfiguration();
        //awsVpcConfig.withAssignPublicIp();
        //awsVpcConfig.withAssignPublicIp();
        //awsVpcConfig.withSecurityGroups();
        //awsVpcConfig.withSubnets();
        //final NetworkConfiguration config = new NetworkConfiguration();
        //config.withAwsvpcConfiguration(vpcConfig);
        //request.withNetworkConfiguration(config);
    }
}
