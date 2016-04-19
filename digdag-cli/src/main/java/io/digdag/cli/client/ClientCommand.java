package io.digdag.cli.client;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.DateTimeException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;
import com.google.common.base.Optional;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.cli.Main;
import io.digdag.cli.Command;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.YamlMapper;
import io.digdag.core.config.PropertyUtils;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.RestApiKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.cli.Main.systemExit;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Locale.ENGLISH;

public abstract class ClientCommand
    extends Command
{
    private static final Logger log = LoggerFactory.getLogger(ClientCommand.class);

    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:65432";

    @Parameter(names = {"-e", "--endpoint"})
    protected String endpoint = null;

    @Parameter(names = {"-k", "--apikey"})
    protected String apiKey = null;

    @Parameter(names = {"-c", "--config"})
    protected String configPath = null;

    @DynamicParameter(names = {"-H", "--header"})
    Map<String, String> httpHeaders = new HashMap<>();

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
        // load config file
        Properties props = loadSystemProperties();

        if (apiKey == null) {
            apiKey = props.getProperty("apikey");
        }

        // TODO remove support for api key
        Optional<RestApiKey> restApiKey = Optional.absent();
        if (apiKey != null && !apiKey.isEmpty()) {
            restApiKey = Optional.of(RestApiKey.of(apiKey));
        }

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

        return DigdagClient.builder()
            .host(host)
            .port(port)
            .ssl(useSsl)
            .headers(headers)
            .apiKeyHeaderBuilder(restApiKey)
            .build();
    }

    @Override
    protected Properties loadSystemProperties()
        throws IOException
    {
        Properties props = super.loadSystemProperties();

        if (configPath != null) {
            props.putAll(PropertyUtils.loadFile(new File(configPath)));
        }

        return props;
    }

    public static void showCommonOptions()
    {
        System.err.println("    -e, --endpoint HOST[:PORT]       HTTP endpoint (default: http://127.0.0.1:65432)");
        System.err.println("    -k, --apikey APIKEY              authentication API key");
        System.err.println("    -c, --config PATH.properties     additional config file to overwrite ~/.digdag/config");
        Main.showCommonOptions();
    }

    public long parseLongOrUsage(String arg)
        throws SystemExitException
    {
        try {
            return Long.parseLong(args.get(0));
        }
        catch (NumberFormatException ex) {
            throw usage(ex.getMessage());
        }
    }

    public int parseIntOrUsage(String arg)
        throws SystemExitException
    {
        try {
            return Integer.parseInt(args.get(0));
        }
        catch (NumberFormatException ex) {
            throw usage(ex.getMessage());
        }
    }

    protected static void ln(String format, Object... args)
    {
        System.out.println(String.format(format, args));
    }

    private static final DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH);

    public static String formatTime(Instant instant)
    {
        return formatter.withZone(ZoneId.systemDefault()).format(instant);
    }

    public static String formatTime(OffsetDateTime time)
    {
        return formatter.format(time);
    }

    public static String formatTimeDiff(Instant now, Instant from)
    {
        long seconds = now.until(from, ChronoUnit.SECONDS);
        long hours = seconds / 3600;
        seconds %= 3600;
        long minutes = seconds / 60;
        seconds %= 60;
        if (hours > 0) {
            return String.format("%2dh %2dm %2ds", hours, minutes, seconds);
        }
        else if (minutes > 0) {
            return String.format("    %2dm %2ds", minutes, seconds);
        }
        else {
            return String.format("        %2ds", seconds);
        }
    }

    private final DateTimeFormatter INSTANT_PARSER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH)
        .withZone(ZoneId.systemDefault());

    private static final DateTimeFormatter LOCAL_TIME_PARSER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]", ENGLISH);

    protected Instant parseTime(String s, String errorMessage)
        throws SystemExitException
    {
        try {
            Instant i = Instant.ofEpochSecond(Long.parseLong(s));
            return i;
        }
        catch (NumberFormatException notUnixTime) {
            try {
                return Instant.from(INSTANT_PARSER.parse(s));
            }
            catch (DateTimeException ex) {
                throw systemExit(errorMessage + ": " + s);
            }
        }
    }

    protected LocalTimeOrInstant parseLocalTimeOrInstant(String s, String errorMessage)
        throws SystemExitException
    {
        // First try parsing as UNIX epoch
        try {
            Instant i = Instant.ofEpochSecond(Long.parseLong(s));
            return LocalTimeOrInstant.of(i);
        }
        catch (NumberFormatException ex) {
            log.trace("Failed to parse as UNIX epoch: {}", s, ex);
        }

        // Then try ISO8601
        try {
            Instant i = Instant.from(ISO_DATE_TIME.parse(s));
            return LocalTimeOrInstant.of(i);
        }
        catch (DateTimeException ex) {
            log.trace("Failed to parse as ISO8601: {}", s, ex);
        }

        // Then try local time (without time zone)
        TemporalAccessor parsed;
        try {
            parsed = LOCAL_TIME_PARSER.parse(s);
        }
        catch (DateTimeParseException ex) {
            throw systemExit(errorMessage + ": " + s);
        }
        try {
            return LocalTimeOrInstant.of(LocalDateTime.from(parsed));
        }
        catch (DateTimeException ex) {
            log.trace("Failed to parse as local datetime: {}", s, ex);
        }

        // Finally try local date (without time and time zone)
        try {
            return LocalTimeOrInstant.of(LocalDateTime.of(LocalDate.from(parsed), LocalTime.of(0, 0, 0)));
        } catch (Exception e) {
            throw systemExit(errorMessage + ": " + s);
        }
    }

    protected static YamlMapper yamlMapper()
    {
        return new YamlMapper(DigdagClient.objectMapper());
    }
}
