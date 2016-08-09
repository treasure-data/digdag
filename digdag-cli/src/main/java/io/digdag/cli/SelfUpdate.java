package io.digdag.cli;

import java.io.PrintStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import javax.ws.rs.core.Response;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import com.google.common.io.ByteStreams;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.BasicAuthentication;
import com.beust.jcommander.Parameter;
import static io.digdag.cli.SystemExitException.systemExit;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static javax.ws.rs.core.UriBuilder.fromUri;

public class SelfUpdate
    extends Command
{
    @Parameter(names = {"-e", "--endpoint"})
    String endpoint = "http://dl.digdag.io";

    public SelfUpdate(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

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
        err.println("Usage: digdag selfupdate [version]]");
        err.println("  Options:");
        Main.showCommonOptions(err);
        err.println("");
        err.println("  Examples:");
        err.println("    $ digdag selfupdate");
        err.println("    $ digdag selfupdate 0.8.8-SNAPSHOT");
        err.println("");
        return systemExit(error);
    }

    private void selfUpdate(String version)
        throws IOException, SystemExitException, URISyntaxException, InterruptedException
    {
        Path dest = Paths.get(Command.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        Client client = new ResteasyClientBuilder()
            .build();

        if (version == null) {
            out.println("Checking the latest version...");
            Response res = getWithRedirect(client, client
                    .target(fromUri(endpoint + "/digdag-latest-version"))
                    .request()
                    .buildGet());
            if (res.getStatus() != 200) {
                throw new RuntimeException("Failed to check the latest version. Error code: " + res.getStatus() + " " + res.getStatusInfo() + "\n" + res.readEntity(String.class));
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
            throw new RuntimeException("Failed to download version " + version + ". Error code: " + res.getStatus() + " " + res.getStatusInfo() + "\n" + res.readEntity(String.class));
        }

        Path path = Files.createTempFile("digdag-", ".jar");
        try (InputStream in = res.readEntity(InputStream.class)) {
            try (OutputStream out = Files.newOutputStream(path)) {
                ByteStreams.copy(in, out);
            }
        }
        path.toFile().setExecutable(true);

        out.println("Verifying...");
        verify(path, version);

        Files.move(path, dest, REPLACE_EXISTING);

        out.println("Upgraded to " + version);
    }

    private void verify(Path path, String expectedVersion)
        throws IOException, InterruptedException
    {
        ProcessBuilder pb = new ProcessBuilder(path.toAbsolutePath().toString(), "--version");
        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pb.redirectErrorStream(true);

        Process p = pb.start();
        String output = new String(ByteStreams.toByteArray(p.getInputStream()));

        int ecode = p.waitFor();
        if (ecode != 0) {
            out.println(output);
            throw new RuntimeException("Failed to verify version: command exists with error code " + ecode);
        }

        Matcher m = Pattern.compile("^" + Pattern.quote(expectedVersion) + "$").matcher(output);
        if (!m.find()) {
            out.println(output);
            throw new RuntimeException("Failed to verify version: version mismatch");
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
}
