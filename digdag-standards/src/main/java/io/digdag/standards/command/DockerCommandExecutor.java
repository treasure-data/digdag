package io.digdag.standards.command;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.common.hash.Hashing;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskRequest;
import io.digdag.client.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DockerCommandExecutor
        implements CommandExecutor
{
    private static Logger logger = LoggerFactory.getLogger(DockerCommandExecutor.class);

    private final CommandLogger clog;
    private final SimpleCommandExecutor simple;

    @Inject
    public DockerCommandExecutor(final CommandLogger clog, final SimpleCommandExecutor simple)
    {
        this.clog = clog;
        this.simple = simple;
    }

    @Override
    public CommandStatus run(final CommandContext context, final CommandRequest request)
            throws IOException
    {
        final Config config = context.getTaskRequest().getConfig();
        if (config.has("docker")) {
            return runWithDocker(context, request);
        }
        else {
            return simple.run(context, request);
        }
    }

    private CommandStatus runWithDocker(final CommandContext context, final CommandRequest request)
            throws IOException
    {
        // TODO set TZ environment variable
        final Process p = startDockerProcess(context, request);

        // copy stdout to System.out and logger
        clog.copyStdout(p, System.out);

        // Need waiting and blocking. Because the process is running on a single instance.
        // The command task could not be taken by other digdag-servers on other instances.
        try {
            p.waitFor();
        }
        catch (InterruptedException e) {
            throw ThrowablesUtil.propagate(e);
        }

        return SimpleCommandStatus.of(p, request.getIoDirectory());
    }

    private Process startDockerProcess(final CommandContext context,
            final CommandRequest request)
            throws IOException
    {
        final TaskRequest taskRequest = context.getTaskRequest();
        final Path projectPath = context.getLocalProjectPath();
        final Config dockerConfig = taskRequest.getConfig().getNestedOrGetEmpty("docker");
        String baseImageName = dockerConfig.get("image", String.class);
        String dockerCommand = dockerConfig.get("docker", String.class, "docker");

        String imageName;
        if (dockerConfig.has("build")) {
            List<String> buildCommands = dockerConfig.getList("build", String.class);
            List<String> buildOptions = dockerConfig.getListOrEmpty("build_options", String.class);
            imageName = uniqueImageName(taskRequest, baseImageName, buildCommands);
            buildImage(dockerCommand, buildOptions, imageName, projectPath, baseImageName, buildCommands);
        }
        else {
            imageName = baseImageName;
            if (dockerConfig.get("pull_always", Boolean.class, false)) {
                pullImage(dockerCommand, imageName);
            }
        }

        ImmutableList.Builder<String> command = ImmutableList.builder();
        List<String> runOptions = dockerConfig.getListOrEmpty("run_options", String.class);
        command.add(dockerCommand).add("run").addAll(runOptions);

        try {
            // misc
            command.add("--rm");  // remove container when exits

            // mount
            command.add("-v").add(String.format(ENGLISH,
                        "%s:%s:rw", projectPath, projectPath));  // use projectPath to keep pb.directory() valid

            // working directory
            final Path workingDirectory = getAbsoluteWorkingDirectory(context, request); // absolute
            command.add("-w").add(workingDirectory.toString());

            logger.debug("Running in docker: {} {}", command.build().stream().collect(Collectors.joining(" ")), imageName);

            // env var
            // TODO deleting temp file right after start() causes "no such file or directory." error
            // because command execution is asynchronous. but using command-line is insecure.
            //Path envFile = Files.createTempFile("docker-env-", ".list");
            //tempFiles.add(envFile);
            //try (BufferedWriter out = Files.newBufferedWriter(envFile)) {
            //    for (Map.Entry<String, String> pair : pb.environment().entrySet()) {
            //        out.write(pair.getKey());
            //        out.write("=");
            //        out.write(pair.getValue());
            //        out.newLine();
            //    }
            //}
            //command.add("--env-file").add(envFile.toAbsolutePath().toString());
            for (Map.Entry<String, String> pair : request.getEnvironments().entrySet()) {
                command.add("-e").add(pair.getKey() + "=" + pair.getValue());
            }

            // image name
            command.add(imageName);

            // command and args
            command.addAll(request.getCommandLine());

            final ProcessBuilder pb = new ProcessBuilder(command.build());
            pb.directory(workingDirectory.toFile());
            pb.redirectErrorStream(true);

            return pb.start();
        }
        catch (IOException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    private static Path getAbsoluteWorkingDirectory(CommandContext context, CommandRequest request)
    {
        return context.getLocalProjectPath().resolve(request.getWorkingDirectory()).normalize();
    }

    private static String uniqueImageName(TaskRequest request,
            String baseImageName, List<String> buildCommands)
    {
        // Name should include project "id" for security reason because
        // conflicting SHA1 hash means that attacker can reuse an image
        // built by someone else.
        String name = "digdag-project-" + Integer.toString(request.getProjectId());

        Config config = request.getConfig().getFactory().create();
        config.set("image", baseImageName);
        config.set("build", buildCommands);
        config.set("revision", request.getRevision().or(UUID.randomUUID().toString()));
        String tag = Hashing.sha1().hashString(config.toString(), UTF_8).toString();

        return name + ':' + tag;
    }

    private void buildImage(String dockerCommand, List<String> buildOptions,
            String imageName, Path projectPath,
            String baseImageName, List<String> buildCommands)
    {
        try {
            String[] nameTag = imageName.split(":", 2);
            Pattern pattern;
            if (nameTag.length > 1) {
                pattern = Pattern.compile("\n" + Pattern.quote(nameTag[0]) + " +" + Pattern.quote(nameTag[1]));
            }
            else {
                pattern = Pattern.compile("\n" + Pattern.quote(imageName) + " ");
            }

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                ProcessBuilder pb = new ProcessBuilder(dockerCommand, "images");
                pb.redirectErrorStream(true);
                Process p = pb.start();

                // read stdout to buffer
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, buffer);
                }

                ecode = p.waitFor();
                message = buffer.toString();
            }

            Matcher m = pattern.matcher(message);
            if (m.find()) {
                // image is already available
                logger.debug("Reusing docker image {}", imageName);
                return;
            }
        }
        catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        logger.info("Building docker image {}", imageName);
        try {
            // create Dockerfile
            Path tmpPath = projectPath.resolve(".digdag/tmp/docker");  // TODO this should be configurable
            Files.createDirectories(tmpPath);
            Path dockerFilePath = tmpPath.resolve("Dockerfile." + imageName.replaceAll(":", "."));

            try (BufferedWriter out = Files.newBufferedWriter(dockerFilePath)) {
                out.write("FROM ");
                out.write(baseImageName.replace("\n", ""));
                out.write("\n");

                // Here shouldn't use 'ADD' because it spoils caching. Using the same base image
                // and build commands should share pre-build revisions. Using revision name
                // as the unique key is not good enough for local mode because revision name
                // is automatically generated based on execution time.

                for (String command : buildCommands) {
                    for (String line : command.split("\n")) {
                        out.write("RUN ");
                        out.write(line);
                        out.write("\n");
                    }
                }
            }

            ImmutableList.Builder<String> command = ImmutableList.builder();
            command.add(dockerCommand).add("build").addAll(buildOptions);
            command.add("-f").add(dockerFilePath.toString());
            command.add("--force-rm");
            command.add("-t").add(imageName);
            command.add(projectPath.toString());

            logger.debug("Building docker image: {}", command.build().stream().collect(Collectors.joining(" ")));

            ProcessBuilder docker = new ProcessBuilder(command.build());
            docker.redirectError(ProcessBuilder.Redirect.INHERIT);
            docker.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            docker.directory(projectPath.toFile());

            Process p = docker.start();
            int ecode = p.waitFor();
            if (ecode != 0) {
                throw new RuntimeException("Docker build failed");
            }
        }
        catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void pullImage(String dockerCommand, String imageName)
    {
        logger.info("Pulling docker image {}", imageName);
        try {
            ImmutableList.Builder<String> command = ImmutableList.builder();
            command.add(dockerCommand).add("pull").add(imageName);

            logger.debug("Pulling docker image: {}", command.build().stream().collect(Collectors.joining(" ")));

            ProcessBuilder docker = new ProcessBuilder(command.build());
            docker.redirectError(ProcessBuilder.Redirect.INHERIT);
            docker.redirectOutput(ProcessBuilder.Redirect.INHERIT);

            Process p = docker.start();
            int ecode = p.waitFor();
            if (ecode != 0) {
                throw new RuntimeException("Docker pull failed");
            }
        }
        catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * This method is never called. The status of the task that is executed by the executor cannot be
     * polled by non-blocking.
     */
    @Override
    public CommandStatus poll(final CommandContext context, final ObjectNode previousStatusJson)
            throws IOException
    {
        throw new UnsupportedOperationException("This method is never called.");
    }
}
