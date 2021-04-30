package io.digdag.cli;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.digdag.cli.profile.TaskAnalyzer;
import io.digdag.cli.profile.WholeTasksSummary;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigModule;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.database.DatabaseModule;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.client.DigdagClient.objectMapper;


public class Profile
    extends Command
{
    @Parameter(
            names = {"--from"},
            required = true,
            converter = TimestampConverter.class
    )
    Instant timeRangeFrom;

    @Parameter(
            names = {"--to"},
            converter = TimestampConverter.class
    )
    Instant timeRangeTo = Instant.now();

    @Parameter(
            names = {"--fetched-attempts"}
    )
    int fetchedAttempts = 1000;

    @Parameter(
            names = {"--partition-size"}
    )
    int partitionSize = 100;

    @Parameter(
            names = {"--database-wait-millis"}
    )
    int databaseWaitMillis = 100;

    static class TimestampConverter
            implements IStringConverter<Instant>
    {
        @Override
        public Instant convert(String value) {
            String errorMessage = "The format of --from / --to option should be \"yyyy-MM-dd HH:mm:ss\"";
            try {
                return ZonedDateTime.of(TimeUtil.parseLocalTime(value, errorMessage), ZoneId.systemDefault()).toInstant();
            } catch (SystemExitException e) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }

    @Override
    public void main()
            throws Exception
    {
        checkArgs();

        ConfigElement configElement = PropertyUtils.toConfigElement(loadSystemProperties());

        Injector injector = Guice.createInjector(
                new ObjectMapperModule()
                        .registerModule(new GuavaModule())
                        .registerModule(new JacksonTimeModule()),
                new DatabaseModule(false),
                new ConfigModule(),
                (binder) -> {
                    binder.bind(ConfigElement.class).toInstance(configElement);
                    binder.bind(Config.class).toProvider(DigdagEmbed.SystemConfigProvider.class);
                }
        );

        WholeTasksSummary tasksSummary = new TaskAnalyzer(injector)
                .run(
                        timeRangeFrom,
                        timeRangeTo,
                        fetchedAttempts,
                        partitionSize,
                        databaseWaitMillis
                );
        objectMapper().writeValue(System.out, tasksSummary);
    }

    private void checkArgs()
        throws Exception
    {
        if (configPath == null) {
            throw usage("--config option is required");
        }
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " profile the performance by analyzing archived task information (experimental)");
        err.println("  Options:");
        err.println("    -c, --config               <path>           configuration file (required)");
        err.println("        --from                 <timestamp>      beginning of time range that the profiler scans based on attempts' finish timestamp (\"yyyy-MM-dd HH:mm:ss\", required)");
        err.println("        --to                   <timestamp>      end of time range that the profiler scans based on attempts' finish timestamp  (\"yyyy-MM-dd HH:mm:ss\", default: current time)");
        err.println("        --fetched-attempts     <attempts-count> number of fetched attempt records at once (default: 1000)");
        err.println("        --partition-size       <partition-size> number of internal partition size (default: 100)");
        err.println("        --database-wait-millis <milliseconds>   internal wait (milliseconds) between database transactions (default: 100)");
        return systemExit(error);
    }
}
