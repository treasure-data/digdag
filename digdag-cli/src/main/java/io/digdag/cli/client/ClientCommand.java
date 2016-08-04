package io.digdag.cli.client;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import io.digdag.cli.Command;
import io.digdag.cli.Main;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.YamlMapper;
import io.digdag.client.DigdagClient;
import io.digdag.core.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.PrintStream;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.digdag.cli.SystemExitException.systemExit;
import static java.util.Locale.ENGLISH;

public abstract class ClientCommand
        extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(ClientCommand.class);

    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:65432";

    protected final Version localVersion;

    @Parameter(names = {"-e", "--endpoint"})
    protected String endpoint = null;

    @DynamicParameter(names = {"-H", "--header"})
    Map<String, String> httpHeaders = new HashMap<>();

    @Parameter(names = {"--disable-version-check"})
    protected boolean disableVersionCheck;

    public ClientCommand(Version localVersion, PrintStream out, PrintStream err)
    {
        super(out, err);
        this.localVersion = Objects.requireNonNull(localVersion, "localVersion");
    }

    @Override
    public void main()
            throws Exception
    {
        try {
            mainWithClientException();
        }
        catch (ClientErrorException ex) {
            Response res = ex.getResponse();
            String body;
            try {
                body = res.readEntity(String.class);
            }
            catch (Exception readFailed) {
                body = ex.getMessage();
            }
            switch (res.getStatus()) {
                case 404:  // NOT_FOUND
                    throw systemExit("Resource not found: " + body);
                case 409:  // CONFLICT
                    throw systemExit("Request conflicted: " + body);
                case 422:  // UNPROCESSABLE_ENTITY
                    throw systemExit("Invalid option: " + body);
                default:
                    throw systemExit("Status code " + res.getStatus() + ": " + body);
            }
        }
    }

    public abstract void mainWithClientException()
            throws Exception;

    protected DigdagClient buildClient()
            throws IOException, SystemExitException
    {
        return buildClient(true);
    }

    protected DigdagClient buildClient(boolean checkServerVersion)
            throws IOException, SystemExitException
    {
        // load config file
        Properties props = loadSystemProperties();

        if (endpoint == null) {
            endpoint = props.getProperty("client.http.endpoint", DEFAULT_ENDPOINT);
        }

        String[] fragments = endpoint.split(":", 2);

        boolean useSsl = false;
        if (fragments.length == 2 && fragments[1].startsWith("//")) {
            // http:// or https://
            switch (fragments[0]) {
                case "http":
                    useSsl = false;
                    break;
                case "https":
                    useSsl = true;
                    break;
                default:
                    throw systemExit("Endpoint must start with http:// or https://: " + endpoint);
            }
            fragments = fragments[1].substring(2).split(":", 2);
        }

        String host;
        int port;
        if (fragments.length == 1) {
            host = fragments[0];
            port = useSsl ? 443 : 80;
        }
        else {
            host = fragments[0];
            port = Integer.parseInt(fragments[1]);
        }

        Map<String, String> headers = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("client.http.headers.")) {
                headers.put(key.substring("client.http.headers.".length()), props.getProperty(key));
            }
        }
        headers.putAll(this.httpHeaders);

        logger.debug("Using endpoint {}://{}:{}", useSsl ? "https" : "http", host, port);

        DigdagClient client = DigdagClient.builder()
                .host(host)
                .port(port)
                .ssl(useSsl)
                .headers(headers)
                .build();

        if (checkServerVersion && !disableVersionCheck) {
            Map<String, Object> remoteVersions = client.getVersion();
            String remoteVersion = String.valueOf(remoteVersions.getOrDefault("version", ""));

            if (!localVersion.version().equals(remoteVersion)) {
                throw systemExit(String.format(""
                                + "Client and server version mismatch: Client: %s, Server: %s.%n"
                                + "Please run following command locally to download a compatible version with the server:%n"
                                + "%n"
                                + "    digdag selfupdate %s%n",
                        localVersion, remoteVersion, remoteVersion));
            }
        }

        return client;
    }

    public void showCommonOptions()
    {
        err.println("    -e, --endpoint HOST[:PORT]       HTTP endpoint (default: http://127.0.0.1:65432)");
        Main.showCommonOptions(err);
    }

    protected long parseLongOrUsage(String arg)
            throws SystemExitException
    {
        try {
            return Long.parseLong(args.get(0));
        }
        catch (NumberFormatException ex) {
            throw usage(ex.getMessage());
        }
    }

    protected int parseIntOrUsage(String arg)
            throws SystemExitException
    {
        try {
            return Integer.parseInt(args.get(0));
        }
        catch (NumberFormatException ex) {
            throw usage(ex.getMessage());
        }
    }

    protected void ln(String format, Object... args)
    {
        out.println(String.format(format, args));
    }

    protected static YamlMapper yamlMapper()
    {
        return new YamlMapper(DigdagClient.objectMapper());
    }
}
