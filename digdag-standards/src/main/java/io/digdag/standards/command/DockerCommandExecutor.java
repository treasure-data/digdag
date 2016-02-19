package io.digdag.standards.command;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.client.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;

public class DockerCommandExecutor
    implements CommandExecutor
{
    private final SimpleCommandExecutor simple;

    private static Logger logger = LoggerFactory.getLogger(DockerCommandExecutor.class);

    @Inject
    public DockerCommandExecutor(SimpleCommandExecutor simple)
    {
        this.simple = simple;
    }

    public Process start(Path archivePath, TaskRequest request, ProcessBuilder pb)
        throws IOException
    {
        // TODO set TZ environment variable
        Config config = request.getConfig();
        if (config.has("docker")) {
            return startWithDocker(archivePath, request, pb);
        }
        else {
            return simple.start(archivePath, request, pb);
        }
    }

    private Process startWithDocker(Path archivePath, TaskRequest request, ProcessBuilder pb)
    {
        Config dockerConfig = request.getConfig().getNestedOrGetEmpty("docker");
        String image = dockerConfig.get("image", String.class);

        String buildImageName = null;
        if (dockerConfig.has("build")) {
            buildImageName = String.format(ENGLISH, "rev-%d-%s",
                    request.getRepositoryId(),
                    request.getRevision().or(UUID.randomUUID().toString()));

            buildImage(archivePath, dockerConfig, image, buildImageName);
        }

        ImmutableList.Builder<String> command = ImmutableList.builder();
        command.add("docker").add("run");

        try {
            // misc
            command.add("-i");  // enable stdin
            command.add("--rm");  // remove container when exits

            // mount
            if (buildImageName == null) {
                // build image already includes /digdag
            }
            else {
                command.add("-v").add(String.format(ENGLISH,
                            "%s:%s:rw", archivePath.toAbsolutePath(), "/digdag"));
            }

            // workdir
            command.add("-w").add("/digdag");

            logger.debug("Running in docker: {} {}", command.build().stream().collect(Collectors.joining(" ")), image);

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
            for (Map.Entry<String, String> pair : pb.environment().entrySet()) {
                command.add("-e").add(pair.getKey() + "=" + pair.getValue());
            }

            // image
            if (buildImageName == null) {
                command.add(image);
            }
            else {
                command.add(buildImageName);
            }

            // command and args
            command.addAll(pb.command());

            ProcessBuilder docker = new ProcessBuilder(command.build());
            docker.redirectError(pb.redirectError());
            docker.redirectErrorStream(pb.redirectErrorStream());
            docker.redirectInput(pb.redirectInput());
            docker.redirectOutput(pb.redirectOutput());
            docker.directory(archivePath.toFile());

            return docker.start();
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private void buildImage(Path archivePath, Config dockerConfig, String image, String buildImageName) {
        try {
            Pattern pattern = Pattern.compile("\n" + Pattern.quote(buildImageName) + " ");

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                ProcessBuilder pb = new ProcessBuilder("docker", "images");
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
                logger.debug("Reusing image {}", buildImageName);
                return;
            }
        }
        catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        logger.debug("Building image {}", buildImageName);
        try {
            // create Dockerfile
            Path tmpPath = archivePath.resolve("digdag.tmp");
            Files.createDirectories(tmpPath);
            Path dockerFilePath = tmpPath.resolve("Dockerfile." + buildImageName);

            List<String> buildCommands = dockerConfig.getList("build", String.class);
            try (BufferedWriter out = Files.newBufferedWriter(dockerFilePath)) {
                out.write("FROM ");
                out.write(image.replace("\n", ""));
                out.write("\n");

                out.write("ADD . /digdag\n");

                out.write("WORKDIR /digdag\n");
                for (String command : buildCommands) {
                    for (String line : command.split("\n")) {
                        out.write("RUN ");
                        out.write(line);
                        out.write("\n");
                    }
                }
            }

            ImmutableList.Builder<String> command = ImmutableList.builder();
            command.add("docker").add("build");
            command.add("-f").add(dockerFilePath.toString());
            command.add("--force-rm");
            command.add("-t").add(buildImageName);
            command.add(archivePath.toString());

            ProcessBuilder docker = new ProcessBuilder(command.build());
            docker.redirectError(ProcessBuilder.Redirect.INHERIT);
            docker.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            docker.directory(archivePath.toFile());

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
}
