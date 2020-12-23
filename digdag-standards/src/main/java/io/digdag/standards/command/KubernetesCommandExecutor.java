package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.kubernetes.KubernetesClient;
import io.digdag.standards.command.kubernetes.KubernetesClientConfig;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

import io.digdag.standards.command.kubernetes.KubernetesClientFactory;
import io.digdag.standards.command.kubernetes.Pod;
import io.digdag.standards.command.kubernetes.TemporalConfigStorage;
import io.digdag.util.DurationParam;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesCommandExecutor
        implements CommandExecutor
{
    private static final String COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX = "agent.command_executor.kubernetes.";

    private static final String DEFAULT_POD_TTL = COMMAND_EXECUTOR_SYSTEM_CONFIG_PREFIX + "default_pod_ttl";

    private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;
    private static Logger logger = LoggerFactory.getLogger(KubernetesCommandExecutor.class);

    private final Config systemConfig;
    private final KubernetesClientFactory kubernetesClientFactory;
    private final DockerCommandExecutor docker;
    private final StorageManager storageManager;
    private final ProjectArchiveLoader projectLoader;
    private final CommandLogger clog;
    private final Optional<Duration> defaultPodTTL;

    @Inject
    public KubernetesCommandExecutor(
            final Config systemConfig,
            final KubernetesClientFactory kubernetesClientFactory,
            final DockerCommandExecutor docker,
            final StorageManager storageManager,
            final ProjectArchiveLoader projectLoader,
            final CommandLogger clog)
    {
        this.systemConfig = systemConfig;
        this.docker = docker;
        this.kubernetesClientFactory = kubernetesClientFactory;
        this.storageManager = storageManager;
        this.projectLoader = projectLoader;
        this.clog = clog;
        this.defaultPodTTL = systemConfig.getOptional(DEFAULT_POD_TTL, DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(Optional.absent());
    }

    @Override
    public CommandStatus run(final CommandContext context, final CommandRequest request)
            throws IOException
    {
        final Config config = context.getTaskRequest().getConfig();
        final Optional<String> clusterName = Optional.absent();
        try {
            final KubernetesClientConfig clientConfig = createKubernetesClientConfig(clusterName, systemConfig, config); // config exception
            final TemporalConfigStorage inConfigStorage = createTemporalConfigStorage(systemConfig, "in"); // config exception
            final TemporalConfigStorage outConfigStorage = createTemporalConfigStorage(systemConfig, "out"); // config exception
            try (final KubernetesClient client = kubernetesClientFactory.newClient(clientConfig)) {
                return runOnKubernetes(context, request, client, inConfigStorage, outConfigStorage);
            }
        }
        catch (ConfigException e) {
            logger.debug("Fall back to DockerCommandExecutor: {}", e.toString());
            return docker.run(context, request); // fall back to DockerCommandExecutor
        }
    }

    @Override
    public CommandStatus poll(final CommandContext context, final ObjectNode previousStatusJson)
            throws IOException
    {
        // If executor is falled back in run method, this poll method needs not to be falled back because it's never
        // used in fall back mode. Please see more details of command based operators.

        final Config config = context.getTaskRequest().getConfig();
        if (!previousStatusJson.has("cluster_name")) {
            // 'cluster_name' must be stored in previous status. previous status is generated during run method.
            throw new IllegalStateException("'cluster_name' must be stored in previous status");
        }
        final Optional<String> clusterName = Optional.of(previousStatusJson.get("cluster_name").asText());
        final KubernetesClientConfig clientConfig = createKubernetesClientConfig(clusterName, systemConfig, config); // config exception
        final TemporalConfigStorage outConfigStorage = createTemporalConfigStorage(systemConfig, "out"); // config exception
        // TODO We'd better to treat config exception here

        try (final KubernetesClient client = kubernetesClientFactory.newClient(clientConfig)) {
            return getCommandStatusFromKubernetes(context, previousStatusJson, client, outConfigStorage);
        }
    }

    @VisibleForTesting
    KubernetesClientConfig createKubernetesClientConfig(final Optional<String> clusterName, final Config systemConfig, final Config config)
            throws ConfigException
    {
        return KubernetesClientConfig.create(clusterName, systemConfig, config);
    }

    @VisibleForTesting
    TemporalConfigStorage createTemporalConfigStorage(final Config systemConfig, final String target)
            throws ConfigException
    {
        return TemporalConfigStorage.createByTarget(storageManager, target, systemConfig);
    }

    @VisibleForTesting
    CommandStatus runOnKubernetes(final CommandContext context,
            final CommandRequest request,
            final KubernetesClient client,
            final TemporalConfigStorage inConfigStorage,
            final TemporalConfigStorage outConfigStorage)
            throws IOException
    {
        final Path projectPath = context.getLocalProjectPath();
        final Path ioDirectoryPath = request.getIoDirectory(); // relative
        final Config config = context.getTaskRequest().getConfig();
        validateDockerConfig(config); // it requires docker: config

        // Build command line arguments that will be passed to Kubernetes API here
        final ImmutableList.Builder<String> bashArguments = ImmutableList.builder();

        // TODO
        // Revisit we need it or not for debugging. If the command will be enabled, pods will show commands are executed
        // by the executor and will include pre-signed URLs in the commands.
        //bashArguments.add("set -eux");
        bashArguments.add(s("mkdir -p %s", projectPath.toString()));
        bashArguments.add(s("cd %s", projectPath.toString()));
        bashArguments.add(s("mkdir -p %s", ioDirectoryPath.toString()));

        // Create project archive on local. Input contents, e.g. input config file and runner script, are included
        // in the project archive. It will be uploaded on temporal config storage and then, will be downloaded on
        // the container. The download URL is generated by pre-signed URL.
        final Path archivePath = createArchiveFromLocal(projectPath, request.getIoDirectory(), context.getTaskRequest());
        final Path relativeArchivePath = projectPath.relativize(archivePath); // relative
        final Path lastPathElementOfArchivePath = projectPath.resolve(".digdag/tmp").relativize(archivePath);
        try {
            // Upload archive on input config storage
            final String archiveKey = createStorageKey(context.getTaskRequest(), lastPathElementOfArchivePath.toString());
            inConfigStorage.uploadFile(archiveKey, archivePath); // IO exception
            final String url = inConfigStorage.getDirectDownloadUrl(archiveKey);
            bashArguments.add(s("curl -s \"%s\" --output %s", url, relativeArchivePath.toString()));
            bashArguments.add(s("tar -zxf %s", relativeArchivePath.toString()));
            final String pushdDir = request.getWorkingDirectory().toString();
            bashArguments.add(s("pushd %s", (pushdDir.isEmpty() ? "." : pushdDir)));
        }
        finally {
            if (archivePath != null) {
                try {
                    Files.deleteIfExists(archivePath); // throw IOException
                }
                catch (IOException e) {
                    // can be ignored because agent will delete project dir once the task will be finished.
                    logger.info(s("Cannot remove a temporal project archive: %s", archivePath.toString()));
                }
            }
        }

        bashArguments.addAll(setArgumentsBeforeScriptCommandLine());
        // Add command passed from operator.
        bashArguments.add(request.getCommandLine().stream().map(Object::toString).collect(Collectors.joining(" ")));
        bashArguments.add(s("exit_code=$?"));
        bashArguments.addAll(setArgumentsAfterScriptCommandLine());

        // Create output archive path in the container
        // Upload the archive file to the S3 bucket
        final String outputArchivePathName = ".digdag/tmp/archive-output.tar.gz"; // relative
        final String outputArchiveKey = createStorageKey(context.getTaskRequest(), "archive-output.tar.gz");
        final String url = outConfigStorage.getDirectUploadUrl(outputArchiveKey);
        bashArguments.add(s("popd"));
        bashArguments.add(s("tar -zcf %s  --exclude %s --exclude %s .digdag/tmp/", outputArchivePathName, relativeArchivePath.toString(), outputArchivePathName));
        bashArguments.add(s("curl -s -X PUT -T %s -L \"%s\"", outputArchivePathName, url));
        bashArguments.add(s("exit $exit_code"));

        final List<String> commands = ImmutableList.of(s("/bin/bash"));
        final List<String> arguments = ImmutableList.of(s("-c"),
                bashArguments.build().stream().map(Object::toString).collect(Collectors.joining("; ")));
        logger.debug("Submit command line arguments to Kubernetes API: " + arguments);

        // Create and submit a pod to Kubernetes master
        final Pod pod = client.runPod(context, request, createUniquePodName(context.getTaskRequest()), commands, arguments);

        final ObjectNode nextStatus = FACTORY.objectNode();
        nextStatus.set("cluster_name", FACTORY.textNode(client.getConfig().getName()));
        nextStatus.set("pod_name", FACTORY.textNode(pod.getName()));
        nextStatus.set("pod_creation_timestamp", FACTORY.numberNode(pod.getCreationTimestamp()));
        nextStatus.set("in_temporal_config_storage_expiration", FACTORY.numberNode(inConfigStorage.getDirectDownloadExpiration()));
        nextStatus.set("out_temporal_config_storage_expiration", FACTORY.numberNode(outConfigStorage.getDirectUploadExpiration()));
        nextStatus.set("io_directory", FACTORY.textNode(ioDirectoryPath.toString()));
        nextStatus.set("executor_state", FACTORY.objectNode());
        return createCommandStatus(pod, false, nextStatus);
    }

    @VisibleForTesting
    CommandStatus getCommandStatusFromKubernetes(final CommandContext context,
            final ObjectNode previousStatusJson,
            final KubernetesClient client,
            final TemporalConfigStorage outConfigStorage)
            throws IOException
    {
        final String podName = previousStatusJson.get("pod_name").asText();
        final Pod pod = client.pollPod(podName);

        if (logger.isDebugEnabled()) {
            logger.debug("Get pod: " + pod.toString());
        }

        final ObjectNode previousExecutorState = (ObjectNode) previousStatusJson.get("executor_state");
        final ObjectNode nextExecutorState = previousExecutorState.deepCopy();

        // If the container doesn't start yet, it cannot extract any log messages from the container.
        final String podPhase = pod.getPhase();
        if (!podPhase.equals("Pending") && !client.isWaitingContainerCreation(pod)) { // not 'waiting'
            // Read log and write it to CommandLogger
            final long offset = !previousExecutorState.has("log_offset") ? 0L : previousExecutorState.get("log_offset").asLong();
            final String logMessage = client.getLog(podName, offset);
            log(logMessage, clog);
            nextExecutorState.set("log_offset", FACTORY.numberNode(offset + logMessage.length())); // update log_offset
        }
        else if(isWaitingLongerThanInConfigStorageExpiration(previousStatusJson)){
            // Throw error because inTemporalConfigStorage expires
            TaskRequest request = context.getTaskRequest();
            long attemptId = request.getAttemptId();
            long taskId = request.getTaskId();

            final String message = s("Pod launch timeout: attempt=%d, task=%d", attemptId, taskId);
            logger.warn(message);

            // TODO
            // Once Kubernetes deletes pods, their information cannot be searched. We'd better to find another way to
            // handle long running pods.
            logger.info(s("Delete pod %d", pod.getName()));
            client.deletePod(pod.getName());

            // Throw exception to stop the task as failure
            throw new TaskExecutionException(message);
        }
        else { // 'waiting'
            // Write pod status to the command logger to avoid users confusing. For example, the container
            // waits starting if it will take long time to download container images.
            log(s("Wait starting a pod. The current pod phase is %s ...", pod.getPhase()), clog);
        }

        final ObjectNode nextStatusJson = previousStatusJson.deepCopy();
        nextStatusJson.set("executor_state", nextExecutorState);

        // If the Pod completed, it needs to create output contents to pass them to the command executor.
        final boolean isFinished = podPhase.equals("Succeeded") || podPhase.equals("Failed");

        if (isFinished) {
            final String outputArchivePathName = "archive-output.tar.gz";
            final String outputArchiveKey = createStorageKey(context.getTaskRequest(), outputArchivePathName); // url format

            // Download output config archive
            final InputStream in = outConfigStorage.getContentInputStream(outputArchiveKey);
            ProjectArchives.extractTarArchive(context.getLocalProjectPath(), in); // runtime exception
        }
        else if (isRunningLongerThanOutConfigStorageExpiration(previousStatusJson) || (defaultPodTTL.isPresent() && isRunningLongerThanTTL(previousStatusJson))) {
            TaskRequest request = context.getTaskRequest();
            long attemptId = request.getAttemptId();
            long taskId = request.getTaskId();

            final String message = s("Pod execution timeout: attempt=%d, task=%d", attemptId, taskId);
            logger.warn(message);

            // TODO
            // Once Kubernetes deletes pods, their information cannot be searched. We'd better to find another way to
            // handle long running pods.
            logger.info(s("Delete pod %d", pod.getName()));
            client.deletePod(pod.getName());

            // Throw exception to stop the task as failure
            throw new TaskExecutionException(message);
        }

        return createCommandStatus(pod, isFinished, nextStatusJson);
    }

    protected List<String> setArgumentsBeforeScriptCommandLine()
    {
        return ImmutableList.of();
    }

    protected List<String> setArgumentsAfterScriptCommandLine()
    {
        return ImmutableList.of();
    }

    private boolean isWaitingLongerThanInConfigStorageExpiration(final ObjectNode previousStatusJson)
    {
        long creationTimestamp = previousStatusJson.get("pod_creation_timestamp").asLong();
        long inTemporalConfigStorageExpiration = previousStatusJson.get("in_temporal_config_storage_expiration").asLong();
        long currentTimestamp = Instant.now().getEpochSecond();
        return currentTimestamp > creationTimestamp + inTemporalConfigStorageExpiration;
    }

    private boolean isRunningLongerThanOutConfigStorageExpiration(final ObjectNode previousStatusJson)
    {
        long creationTimestamp = previousStatusJson.get("pod_creation_timestamp").asLong();
        long outTemporalConfigStorageExpiration = previousStatusJson.get("out_temporal_config_storage_expiration").asLong();
        long currentTimestamp = Instant.now().getEpochSecond();
        return currentTimestamp > creationTimestamp + outTemporalConfigStorageExpiration;
    }

    private boolean isRunningLongerThanTTL(final ObjectNode previousStatusJson)
    {
        long creationTimestamp = previousStatusJson.get("pod_creation_timestamp").asLong();
        long currentTimestamp = Instant.now().getEpochSecond();
        return currentTimestamp > creationTimestamp + defaultPodTTL.get().getSeconds();
    }

    private static void log(final String message, final CommandLogger to)
            throws IOException
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        final InputStream in = new ByteArrayInputStream(bytes);
        to.copy(in, System.out);
    }

    private static void validateDockerConfig(final Config requestConfig)
    {
        if (!requestConfig.has("docker")) {
            throw new ConfigException("Parameter 'docker' is required but not set");
        }

        final Config dockerConfig = requestConfig.getNested("docker");
        if (!dockerConfig.has("image")) {
            throw new ConfigException("Parameter 'docker.image' is required but not set");
        }

        // TODO revisit to check other config parameters
    }

    private static String createStorageKey(final TaskRequest request, final String lastPathElementOfArchiveFile)
    {
        // file key: {taskId}/{lastPathElementOfArchiveFile}
        return new StringBuilder()
                .append(request.getTaskId()).append("/")
                .append(lastPathElementOfArchiveFile)
                .toString();
    }

    private static String createUniquePodName(final TaskRequest request)
    {
        // name format: digdag-pod-{taskId}-{siteId}-{UUIDv4}
        final int siteId = request.getSiteId();
        final long taskId = request.getTaskId();
        return new StringBuilder()
                .append("digdag-pod-")
                .append(taskId).append("-")
                .append(siteId).append("-")
                .append(UUID.randomUUID().toString()) // UUID v4
                .toString();
    }

    private static String s(final String format, final Object... args)
    {
        return String.format(Locale.ENGLISH, format, args);
    }

    private CommandStatus createCommandStatus(final Pod pod,
            final boolean isFinished,
            final ObjectNode nextStatus)
    {
        // we don't need to update "io_directory"
        if (isFinished) {
            nextStatus.set("status_code", FACTORY.numberNode(pod.getStatusCode()));
        }
        return KubernetesCommandStatus.of(isFinished, nextStatus);
    }

    /**
     * return relative path of project archive
     *
     * @param projectPath
     * @param request
     * @return
     * @throws IOException
     */
    private Path createArchiveFromLocal(final Path projectPath, final Path ioDirectoryPath, final TaskRequest request)
            throws IOException
    {
        final Path archivePath = Files.createTempFile(projectPath.resolve(".digdag/tmp"), "archive-input-", ".tar.gz"); // throw IOException
        logger.debug("Creating " + archivePath + "...");

        final Config config = request.getConfig();
        final WorkflowResourceMatcher workflowResourceMatcher = WorkflowResourceMatcher.defaultMatcher();
        final ProjectArchive projectArchive = projectLoader.load(projectPath, workflowResourceMatcher, config); // throw IOException
        try (final TarArchiveOutputStream tar = new TarArchiveOutputStream(
                new GzipCompressorOutputStream(Files.newOutputStream(archivePath.toAbsolutePath())))) { // throw IOException
            // Archive project files
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            projectArchive.listFiles((resourceName, absPath) -> { // throw IOException
                logger.debug("  Archiving " + resourceName);
                if (!Files.isDirectory(absPath)) {
                    final TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName); // throw IOException
                    tar.putArchiveEntry(e); // throw IOException
                    if (e.isSymbolicLink()) {
                        logger.debug("    symlink -> " + e.getLinkName());
                    }
                    else {
                        try (final InputStream in = Files.newInputStream(absPath)) { // throw IOException
                            ByteStreams.copy(in, tar); // throw IOException
                        }
                    }
                    tar.closeArchiveEntry(); // throw IOExcpetion
                }
                return true;
            });

            // Add .digdag/tmp/ files to the archive
            final Path absoluteIoDirectoryPath = projectPath.resolve(ioDirectoryPath);
            try (final DirectoryStream<Path> ds = Files.newDirectoryStream(absoluteIoDirectoryPath)) {
                for (final Path absPath : ds) {
                    final String resourceName = ProjectArchive.realPathToResourceName(projectPath, absPath);
                    final TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName);
                    tar.putArchiveEntry(e); // throw IOexception
                    if (!e.isSymbolicLink()) {
                        try (final InputStream in = Files.newInputStream(absPath)) { // throw IOException
                            ByteStreams.copy(in, tar); // throw IOException
                        }
                    }
                    tar.closeArchiveEntry(); // throw IOExcpetion
                }
            }
        }

        return archivePath;
    }

    private TarArchiveEntry buildTarArchiveEntry(final Path projectPath, final Path absPath, final String name)
            throws IOException
    {
        final TarArchiveEntry e;
        if (Files.isSymbolicLink(absPath)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            final Path rawDest = Files.readSymbolicLink(absPath);
            final Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

            if (!normalizedAbsDest.startsWith(projectPath)) {
                throw new IllegalArgumentException(s(
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'", normalizedAbsDest, projectPath));
            }

            // absolute path will be invalid on a server. convert it to a relative path
            final Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

            String linkName = normalizedRelativeDest.toString();

            // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
            linkName = linkName.replace(File.separatorChar, '/');
            e.setLinkName(linkName);
        }
        else {
            e = new TarArchiveEntry(absPath.toFile(), name);
            try {
                int mode = 0;
                for (final PosixFilePermission perm : Files.getPosixFilePermissions(absPath)) {
                    switch (perm) {
                        case OWNER_READ:
                            mode |= 0400;
                            break;
                        case OWNER_WRITE:
                            mode |= 0200;
                            break;
                        case OWNER_EXECUTE:
                            mode |= 0100;
                            break;
                        case GROUP_READ:
                            mode |= 0040;
                            break;
                        case GROUP_WRITE:
                            mode |= 0020;
                            break;
                        case GROUP_EXECUTE:
                            mode |= 0010;
                            break;
                        case OTHERS_READ:
                            mode |= 0004;
                            break;
                        case OTHERS_WRITE:
                            mode |= 0002;
                            break;
                        case OTHERS_EXECUTE:
                            mode |= 0001;
                            break;
                        default:
                            // ignore
                    }
                }
                e.setMode(mode);
            }
            catch (UnsupportedOperationException ex) {
                // ignore custom mode
            }
        }
        return e;
    }
}
