package io.digdag.cli.client;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestAgentLauncherHandle;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class AgentRunner
{
    private final DigdagClient client;
    private final Path workingDir;
    private final Path binDir;

    // working-dir/agent/launcher.jar.download
    // working-dir/agent/launcher.jar
    // working-dir/agent.yml

    private Path latestJarPath = null;
    private byte[] latestMd5 = null;
    private RestAgentLauncherHandle latestHandle = null;

    private Process latestProcess;

    public AgentRunner(DigdagClient client, Path workingDir)
    {
        this.client = client;
        this.workingDir = workingDir;
        this.binDir = workingDir.resolve("bin");
    }

    public void run()
            throws IOException, InterruptedException
    {
        Files.createDirectories(binDir);

        downloadLauncher();

        startLauncher();

        long startMillis = System.currentTimeMillis();
        latestProcess.waitFor();
        long durationMillis = System.currentTimeMillis() - startMillis;

        // TODO implement retry & loop

        // TODO support docker
    }

    private void downloadLauncher()
        throws IOException
    {
        RestAgentLauncherHandle handle = client.getAgentLauncherHandle();

        // if md5 of remote file is same with md5 currently download local file,
        // skip downloading.
        byte[] expectedMd5 = Base64.getDecoder().decode(handle.getMd5());
        if (latestMd5 != null && Arrays.equals(expectedMd5, latestMd5)) {
            return;
        }

        // download jar to bin/launcher.jar.download
        Path jarDownloadPath = binDir.resolve("launcher.jar.download");

        try (InputStream in = client.getAgentJar(handle.getDirect())) {
            try (OutputStream out = Files.newOutputStream(jarDownloadPath)) {
                ByteStreams.copy(in, out);
            }
        }

        // check md5
        byte[] md5 = com.google.common.io.Files.hash(
                jarDownloadPath.toFile(), Hashing.md5()
                ).asBytes();
        if (!Arrays.equals(expectedMd5, md5)) {
            throw new IOException("MD5 digest of downloaded agent launcher doesn't match");
        }

        // move jar to bin/launcher.jar.a0b9f1
        Path jarPath = binDir.resolve("launcher.jar." + BaseEncoding.base16().lowerCase().encode(md5));
        Files.move(jarDownloadPath, jarPath);

        this.latestJarPath = jarPath;
        this.latestMd5 = md5;
        this.latestHandle = handle;
    }

    private Process startLauncher()
        throws IOException
    {
		String javaPath =
			Paths.get(System.getProperty("java.home"))
			.resolve("bin")
			.resolve("java")
			.toString();

        List<String> command = ImmutableList.<String>builder()
            .add(javaPath)
            .addAll(latestHandle.getJavaOptions())
            .addAll(latestHandle.getJavaArguments())
            .build();

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(workingDir.toFile());

        this.latestProcess = pb.start();
        return latestProcess;
    }
}
