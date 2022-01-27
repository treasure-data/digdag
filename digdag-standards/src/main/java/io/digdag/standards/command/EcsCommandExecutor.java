package io.digdag.standards.command;

import com.amazonaws.services.ecs.model.AssignPublicIp;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.CapacityProviderStrategyItem;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.LogConfiguration;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PlacementStrategy;
import com.amazonaws.services.ecs.model.PlacementStrategyType;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.ecs.model.TaskSetNotFoundException;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.core.log.LogMarkers;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.ecs.EcsClient;
import io.digdag.standards.command.ecs.EcsClientConfig;
import io.digdag.standards.command.ecs.EcsClientFactory;
import io.digdag.standards.command.ecs.EcsTaskStatus;
import io.digdag.standards.command.ecs.TemporalProjectArchiveStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class EcsCommandExecutor
        implements CommandExecutor
{
    private static Logger logger = LoggerFactory.getLogger(CommandExecutor.class);

    private static final String ECS_COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX = "agent.command_executor.ecs.";
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
    private final int retryDownloads, retryUploads;
    private final boolean curlFailOptOnUploads; // false by the default

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
            logger.debug("Fall back to DockerCommandExecutor: {} {}", e.getMessage(), e.getCause() != null ? e.getCause().getMessage() : "");
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
        logger.debug("Submit task request:" + dumpTaskRequest(runTaskRequest));
        final RunTaskResult runTaskResult = client.submitTask(runTaskRequest); // RuntimeException, ConfigException
        logger.debug("Submit task response:" + dumpTaskResult(runTaskResult));
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

    @Override
    public void cleanup(
            final CommandContext commandContext,
            final Config state)
            throws IOException
    {
        final TaskRequest request = commandContext.getTaskRequest();
        final long attemptId = request.getAttemptId();
        final long taskId = request.getTaskId();
        final Config taskConfig = request.getConfig();

        final ObjectNode commandStatus = state.get("commandStatus", ObjectNode.class);
        final String clusterName = commandStatus.get("cluster_name").asText();
        final String taskArn = commandStatus.get("task_arn").asText();
        final EcsClientConfig clientConfig = createEcsClientConfig(Optional.of(clusterName), systemConfig, taskConfig); // ConfigException

        try (final EcsClient client = ecsClientFactory.createClient(clientConfig)) { // ConfigException
            final Task task = client.getTask(clusterName, taskArn);
            final String message = s("Command task execution will be stopped: attempt_id=%d, task_id=%d", attemptId, taskId);
            logger.info(message);
            logger.debug(s("Stop command task: %s", task.getTaskArn()));
            client.stopTask(clusterName, task.getTaskArn());
        }
    }

    CommandStatus createNextCommandStatus(
            final CommandContext commandContext,
            final EcsClient client,
            final ObjectNode previousStatus)
            throws IOException
    {
        final String cluster = previousStatus.get("cluster_name").asText();
        final String taskArn = previousStatus.get("task_arn").asText();

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
                try {
                    // Just to avoid DOS attack to ECS endpoint
                    TimeUnit.SECONDS.sleep(2);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            while (Instant.now().getEpochSecond() < timeout);

            final String outputArchivePathName = "archive-output.tar.gz";
            final String outputArchiveKey = createStorageKey(commandContext.getTaskRequest(), outputArchivePathName); // url format
            // Download output config archive
            final TemporalProjectArchiveStorage temporalStorage = createTemporalProjectArchiveStorage(commandContext.getTaskRequest().getConfig());
            try (final InputStream in = temporalStorage.getContentInputStream(outputArchiveKey)) {
                ProjectArchives.extractTarArchive(commandContext.getLocalProjectPath(), in); // IOException
            }

            final ObjectNode nextStatus = previousStatus.deepCopy();
            nextStatus.set("executor_state", previousExecutorStatus);

            Optional<String> errorMessage = getErrorMessageFromTask(cluster, taskArn, client);
            return EcsCommandStatus.of(true, nextStatus, errorMessage);
        }

        final Task task;
        try {
            task = client.getTask(cluster, taskArn);
        }
        catch (TaskSetNotFoundException e) {
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
            // Set exit code of container finished to nextStatus.
            // If exit code doesn't exist, something's wrong with execution, so set status code to 1 to make workflow fail
            Integer exitCode = task.getContainers().get(0).getExitCode();
            if (exitCode == null) {
                logger.debug("Container has no exit code. The status code will be set as error (1)");
            }
            nextStatus.put("status_code", exitCode != null ? exitCode : 1);
        }

        // always return false to check if all logs are fetched. (return in head of this method after checking finish marker.)
        return EcsCommandStatus.of(false, nextStatus);
    }

    @VisibleForTesting
    static Optional<String> getErrorMessageFromTask(String cluster, String taskArn, EcsClient client)
    {
        Optional<String> errorMessage = Optional.absent();
        try {
            final Task task = client.getTask(cluster, taskArn);
            final List<String> reasons = task.getContainers().stream()
                    .map(c -> c.getReason())
                    .filter(r -> !Strings.isNullOrEmpty(r))
                    .collect(Collectors.toList());
            if (reasons.size() > 0) {
                errorMessage = Optional.of(String.join(",", reasons));
            }
            else {
                errorMessage = Optional.of("No container information");
            }
        }
        catch (TaskSetNotFoundException e) {
            errorMessage = Optional.fromNullable(e.getErrorMessage());

        }
        return errorMessage;
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

    static class EcsCommandStatus
            implements CommandStatus
    {
        static EcsCommandStatus of(final boolean isFinished, final ObjectNode json)
        {
            return of(isFinished, json, Optional.absent());
        }

        static EcsCommandStatus of(final boolean isFinished, final ObjectNode json, Optional<String> errorMessage)
        {
            return new EcsCommandStatus(isFinished, json, errorMessage);
        }

        private final boolean isFinished;
        private final ObjectNode json;
        private final Optional<String> errorMessage;

        private EcsCommandStatus(final boolean isFinished, final ObjectNode json, final Optional<String> errorMessage)
        {
            this.isFinished = isFinished;
            this.json = json;
            this.errorMessage = errorMessage;
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
        public Optional<String> getErrorMessage()
        {
            return errorMessage;
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

    protected Task findTask(final String taskDefinitionArn, final RunTaskResult result)
    {
        for (final Task t : result.getTasks()) {
            if (t.getTaskDefinitionArn().equals(taskDefinitionArn)) {
                return t;
            }
        }
        throw new RuntimeException("Submitted task could not be found"); // TODO the message should be improved more understandably.
    }

    protected EcsClientConfig createEcsClientConfig(
            final Optional<String> clusterName,
            final Config systemConfig,
            final Config taskConfig)
    {
        if (taskConfig.has(EcsClientConfig.TASK_CONFIG_ECS_KEY)) {
            return EcsClientConfig.createFromTaskConfig(clusterName, taskConfig, systemConfig);
        }
        else {
            return EcsClientConfig.createFromSystemConfig(clusterName, systemConfig);
        }
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
        setEcsTaskOverride(commandContext, commandRequest, td, runTaskRequest, clientConfig); // RuntimeException,ConfigException
        setEcsTaskLaunchType(clientConfig, runTaskRequest);
        setEcsTaskStartedBy(clientConfig, runTaskRequest);
        setEcsNetworkConfiguration(clientConfig, runTaskRequest);
        setCapacityProviderStrategy(clientConfig, runTaskRequest);
        setPlacementStrategy(clientConfig, runTaskRequest);
        return runTaskRequest;
    }

    private void setPlacementStrategy(EcsClientConfig clientConfig, RunTaskRequest runTaskRequest)
            throws ConfigException
    {
        if (clientConfig.getPlacementStrategyType().isPresent()) {
            final PlacementStrategyType placementStrategyType;
            try {
                placementStrategyType = PlacementStrategyType.fromValue(clientConfig.getPlacementStrategyType().get());
            }
            // The message of this exception object has the validation error message.
            catch (IllegalArgumentException validationError) {
                throw new ConfigException("PlacementStrategyType is invalid", validationError);
            }
            final PlacementStrategy placementStrategy = new PlacementStrategy();
            placementStrategy.setType(placementStrategyType);

            if (clientConfig.getPlacementStrategyField().isPresent()) {
                placementStrategy.setField(clientConfig.getPlacementStrategyField().get());
            }

            runTaskRequest.setPlacementStrategy(Arrays.asList(placementStrategy));
        }
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
            final RunTaskRequest request,
            final EcsClientConfig clientConfig)
            throws ConfigException
    {
        final ContainerOverride containerOverride = new ContainerOverride();
        // Assume that a single container will run in the task.
        final ContainerDefinition cd = td.getContainerDefinitions().get(0);
        setEcsContainerOverrideName(commandContext, commandRequest, containerOverride, cd);
        setEcsContainerOverrideCommand(commandContext, commandRequest, containerOverride); // RuntimeException,ConfigException
        setEcsContainerOverrideEnvironment(commandContext, commandRequest, containerOverride);
        setEcsContainerOverrideResource(clientConfig, containerOverride);

        final TaskOverride taskOverride = new TaskOverride();
        taskOverride.withContainerOverrides(containerOverride);
        setTaskOverrideResource(clientConfig, taskOverride);

        request.withOverrides(taskOverride);
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
            logger.error(LogMarkers.UNEXPECTED_SERVER_ERROR, message, e);
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
            logger.error(LogMarkers.UNEXPECTED_SERVER_ERROR, message, e);
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
        bashArguments.add(s("curl --retry %d --retry-connrefused%s -s -X PUT -T %s -L \"%s\"", retryUploads, curlFailOptOnUploads ? " --fail" : "", outputProjectArchivePathName, outputProjectArchiveDirectUploadUrl));
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
            final EcsClientConfig clientConfig,
            final ContainerOverride containerOverride)
    {
        if (clientConfig.getContainerCpu().isPresent()) {
            containerOverride.setCpu(clientConfig.getContainerCpu().get());
        }
        if (clientConfig.getContainerMemory().isPresent()) {
            containerOverride.setMemory(clientConfig.getContainerMemory().get());
        }
    }

    protected void setEcsTaskStartedBy(EcsClientConfig clientConfig, RunTaskRequest runTaskRequest)
    {
        if (clientConfig.getStartedBy().isPresent()) {
            runTaskRequest.setStartedBy(clientConfig.getStartedBy().get());
        }
    }

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
        // Note: `LaunchType` CAN NOT be specified with `CapacityProviderStrategy` in the same time.
        // So when you specify `CapacityProviderStrategy`, do not specify `LaunchType`.
        // https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_RunTask.html#ECS-RunTask-request-capacityProviderStrategy
        if (clientConfig.getLaunchType().isPresent()) {
            final LaunchType launchType = LaunchType.fromValue(clientConfig.getLaunchType().get());
            request.withLaunchType(launchType);
        }
    }

    protected void setEcsNetworkConfiguration(final EcsClientConfig clientConfig, final RunTaskRequest request)
    {
        if (clientConfig.getSubnets().isPresent()) {
            request.withNetworkConfiguration(new NetworkConfiguration().withAwsvpcConfiguration(
                    new AwsVpcConfiguration()
                            .withSubnets(clientConfig.getSubnets().get())
                            .withAssignPublicIp(clientConfig.isAssignPublicIp() ? AssignPublicIp.ENABLED : AssignPublicIp.DISABLED)
            ));
        }
    }

    protected void setCapacityProviderStrategy(final EcsClientConfig clientConfig, final RunTaskRequest request)
    {
        if (clientConfig.getCapacityProviderName().isPresent()) {
            CapacityProviderStrategyItem capacityProviderStrategyItem = new CapacityProviderStrategyItem()
                    .withCapacityProvider(clientConfig.getCapacityProviderName().get());
            request.setCapacityProviderStrategy(Arrays.asList(capacityProviderStrategyItem));
        }
    }

    private static String dumpTaskRequest(final RunTaskRequest request)
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (request.getCluster() != null) { sb.append("Cluster: ").append(request.getCluster()).append(","); }
        if (request.getTaskDefinition() != null) { sb.append("TaskDefinition: ").append(request.getTaskDefinition()).append(","); }
        if (request.getLaunchType() != null) { sb.append("LaunchType: ").append(request.getLaunchType()).append(","); }
        if (request.getCapacityProviderStrategy() != null) { sb.append("CapacityProviderStrategy: ").append(request.getCapacityProviderStrategy()).append(","); }
        if (request.getPlacementConstraints() != null) { sb.append("PlacementConstraints: ").append(request.getPlacementConstraints()).append(","); }
        if (request.getPlacementStrategy() != null) { sb.append("PlacementStrategy: ").append(request.getPlacementStrategy()).append(","); }
        if (request.getPlatformVersion() != null) { sb.append("PlatformVersion: ").append(request.getPlatformVersion()).append(","); }
        TaskOverride overrides = request.getOverrides();
        if (overrides != null) {
            if (overrides.getCpu() != null) {
                sb.append("CPU: ").append(overrides.getCpu()).append(",");
            }

            if (overrides.getCpu() != null) {
                sb.append("Memory: ").append(overrides.getMemory());
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String dumpTaskResult(final RunTaskResult result)
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Task task : result.getTasks()) {
            sb.append("{");
            if (task.getTaskArn() != null) { sb.append("TaskArn: ").append(task.getTaskArn()).append(","); }
            if (task.getClusterArn() != null) { sb.append("ClusterArn: ").append(task.getClusterArn()).append(","); }
            if (task.getContainerInstanceArn() != null) { sb.append("ContainerInstanceArn: ").append(task.getContainerInstanceArn()).append(","); }
            if (task.getTaskDefinitionArn() != null) { sb.append("TaskDefinitionArn: ").append(task.getTaskDefinitionArn()).append(","); }
            if (task.getHealthStatus() != null) { sb.append("HealthStatus: ").append(task.getHealthStatus()).append(","); }
            if (task.getPlatformVersion() != null) { sb.append("PlatformVersion: ").append(task.getPlatformVersion()).append(","); }
            if (task.getCreatedAt() != null) { sb.append("CreatedAt: ").append(task.getCreatedAt()).append(","); }
            if (task.getStartedAt() != null) { sb.append("StartedAt: ").append(task.getStartedAt()); }
            sb.append("}");
            sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    protected void setTaskOverrideResource(EcsClientConfig clientConfig, TaskOverride taskOverride)
    {
        if (clientConfig.getTaskCpu().isPresent()) {
            taskOverride.setCpu(clientConfig.getTaskCpu().get());
        }

        if (clientConfig.getTaskMemory().isPresent()) {
            taskOverride.setMemory(clientConfig.getTaskMemory().get());
        }
    }
}
