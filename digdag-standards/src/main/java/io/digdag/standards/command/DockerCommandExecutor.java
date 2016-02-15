package io.digdag.standards.command;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.io.File;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.client.config.Config;
import io.digdag.standards.task.ArchiveFiles;
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
            return startWithDocker(archivePath, request.getConfig().getNestedOrGetEmpty("docker"), pb);
        }
        else {
            return simple.start(archivePath, request, pb);
        }
    }

    private Process startWithDocker(Path archivePath, Config dockerConfig, ProcessBuilder pb)
    {
        String image = dockerConfig.get("image", String.class);

        ImmutableList.Builder<String> command = ImmutableList.builder();
        command.add("docker").add("run");

        try {
            // stdio
            command.add("-i");

            // mount
            command.add("-v").add(String.format(ENGLISH,
                        "%s:%s:rw", archivePath.toAbsolutePath(), "/digdag"));

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
            command.add(image);

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
}
