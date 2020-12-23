package io.digdag.cli;

import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.digdag.cli.SystemExitException.systemExit;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Locale.ENGLISH;
import static javax.ws.rs.core.UriBuilder.fromUri;

public class SelfUpdate
    extends Command
{
    private static boolean isSelfRun()
    {
        return "selfrun".equals(System.getProperty("io.digdag.cli.launcher"));
    }

    private static boolean isWindows()
    {
        String osName = System.getProperty("os.name");
        return osName != null && osName.toLowerCase(ENGLISH).contains("windows");
    }

    private static List<String> getJavaOptions()
    {
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String arg : arguments) {
            if (arg.startsWith("-")) {
                builder.add(arg);
            }
            else {
                break;
            }
        }
        return builder.build();
    }

    @Parameter(names = {"-e", "--endpoint"})
    String endpoint = "http://dl.digdag.io";

    @Override
    public void main()
            throws Exception
    {
        switch (args.size()) {
        case 0:
            selfUpdate(null);
            break;
        case 1:
            selfUpdate(args.get(0));
            break;
        default:
            throw usage(null);
        }
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " selfupdate [version]]");
        err.println("  Options:");
        Main.showCommonOptions(env, err);
        err.println("");
        err.println("  Examples:");
        err.println("    $ " + programName + " selfupdate");
        err.println("    $ " + programName + " selfupdate 0.10.0-SNAPSHOT");
        err.println("");
        return systemExit(error);
    }

    private void selfUpdate(String version)
        throws IOException, SystemExitException, URISyntaxException, InterruptedException
    {
        Path dest = Paths.get(Command.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        if (!endpoint.startsWith("http")) {
            throw systemExit("-e option must be an HTTP URL (http://HOST:PORT or https://HOST:PORT)");
        }

        Client client = new ResteasyClientBuilder()
            .build();

        if (version == null) {
            out.println("Checking the latest version...");
            Response res = getWithRedirect(client, client
                    .target(fromUri(endpoint + "/digdag-latest-version"))
                    .request()
                    .buildGet());
            if (res.getStatus() != 200) {
                throw systemExit(String.format(ENGLISH,
                            "Failed to check the latest version. Response code: %d %s\n%s",
                            res.getStatus(), res.getStatusInfo(), res.readEntity(String.class)));
            }
            version = res.readEntity(String.class).trim();
        }

        // TODO abort if already this version

        out.println("Upgrading to " + version + "...");

        Response res = getWithRedirect(client, client
                .target(fromUri(endpoint + "/digdag-" + version))
                .request()
                .buildGet());
        if (res.getStatus() != 200) {
            throw systemExit(String.format(ENGLISH,
                        "Failed to download version %s. Response code: %d %s\n%s",
                        version, res.getStatus(), res.getStatusInfo(), res.readEntity(String.class)));
        }

        boolean backgroundMove = false;
        Path path = Files.createTempFile("digdag-", ".bat");
        try {
            try (InputStream in = res.readEntity(InputStream.class)) {
                try (OutputStream out = Files.newOutputStream(path)) {
                    ByteStreams.copy(in, out);
                }
            }
            path.toFile().setExecutable(true, false);
            path.toFile().setReadable(true, false);

            // try to move the file to make sure that the file is on the same file system
            // with the destination so that enable move more likely works. This is also
            // helpful if /tmp file system is mounted with noexec option on Linux.
            Path near = dest.getParent().resolve(".digdag.selfupdate.bat");
            try {
                Files.move(path, near, REPLACE_EXISTING);
                path = near;
            }
            catch (AccessDeniedException ex) {
                // ignore failure with AccessDeniedException
            }

            out.println("Verifying...");
            verify(path, version);

            try {
                try {
                    Files.move(path, dest, ATOMIC_MOVE);
                }
                catch (AccessDeniedException | AtomicMoveNotSupportedException noAtomic) {
                    Files.move(path, dest, REPLACE_EXISTING);
                }
            }
            catch (AccessDeniedException ex) {
                throw systemExit(String.format(ENGLISH,
                            "%s: permission denied\nhint: don't you need \"sudo\"?",
                            ex.getMessage()));
            }
            catch (FileSystemException ex) {
                if (isWindows()) {
                    // Windows can't change or delete a file if the file is still executing.
                    // To avoid this limitation, here creates a background process that
                    // repeats the move operation until it succeeds. The process deletes
                    // itself at the end (bat script can delete itself even if it's running).
                    backgroundMoveOnWindows(path, dest);
                    backgroundMove = true;
                }
                else {
                    throw ex;
                }
            }
        }
        finally {
            if (!backgroundMove) {
                try {
                    if (Files.exists(path)) {
                        Files.delete(path);
                    }
                }
                catch (IOException ex) {
                    // ignore errors and allow keeping the garbage temp file
                }
            }
        }

        if (backgroundMove) {
            out.println("Upgrading to " + version + " is in progress in background.");
        }
        else {
            out.println("Upgraded to " + version);
        }
    }

    private void verify(Path path, String expectedVersion)
        throws IOException, SystemExitException, InterruptedException
    {
        List<String> cmdline;
        String jarPath = path.toAbsolutePath().toString();
        if (isSelfRun()) {
            cmdline = ImmutableList.of(jarPath, "--version");
        }
        else {
            String javaPath =
                Paths.get(System.getProperty("java.home"))
                .resolve("bin")
                .resolve("java")
                .toString();
            cmdline = ImmutableList.<String>builder()
                .add(javaPath.toString())
                .addAll(getJavaOptions())
                .add("-jar")
                .add(jarPath)
                .add("--version")
                .build();
        }

        ProcessBuilder pb = new ProcessBuilder(cmdline);
        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pb.redirectErrorStream(true);

        Process p = pb.start();
        String output = new String(ByteStreams.toByteArray(p.getInputStream()));

        int ecode = p.waitFor();
        if (ecode != 0) {
            out.println(output);
            throw systemExit("Failed to verify version: command exists with error code " + ecode);
        }

        Matcher m = Pattern.compile("^" + Pattern.quote(expectedVersion) + "$").matcher(output);
        if (!m.find()) {
            out.println(output);
            throw systemExit("Failed to verify version: version mismatch");
        }
    }

    private Response getWithRedirect(Client client, Invocation req)
    {
        while (true) {
            Response res = req.invoke();
            String location =  res.getHeaderString("Location");
            if (res.getStatus() / 100 != 3 || location == null) {
                return res;
            }
            res.readEntity(String.class);
            if (!location.startsWith("http")) {
                location = endpoint + location;
            }
            req = client.target(fromUri(location))
                .request()
                .buildGet();
        }
    }

    private void backgroundMoveOnWindows(Path src, Path dest)
        throws IOException
    {
        // move the file in background using a bat script that also deletes itself at the end
        Path selfCopy = Files.createTempFile("digdag-selfcopy-", ".bat");
        Files.write(selfCopy, Resources.toByteArray(Resources.getResource("digdag/cli/selfcopy.bat")));
        selfCopy.toFile().setExecutable(true, false);

        ProcessBuilder pb = new ProcessBuilder(
                "cmd.exe",
                "/c",
                selfCopy.toAbsolutePath().toString(),
                src.toAbsolutePath().toString(),
                dest.toAbsolutePath().toString(),
                ">NUL",
                "2>NUL");
        pb.start();
    }
}
