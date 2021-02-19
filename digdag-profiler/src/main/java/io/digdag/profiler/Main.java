package io.digdag.profiler;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigModule;
import io.digdag.core.database.DatabaseModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


public class Main
{
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    static class Args
    {
        @Parameter(
                names = {"--config", "-c"},
                description = "Configuration file path",
                required = true
        )
        File configFile;

        @Parameter(
                names = {"--help", "-h"},
                help = true
        )
        boolean help;

        @Parameter(
                names = {"--from"},
                description = "The start of time range (ISO-8601: yyyy-MM-dd'T'HH:mm:ss)",
                required = true,
                converter = ISO8601TimestampConverter.class
        )
        Instant timeRangeFrom;

        @Parameter(
                names = {"--to"},
                description = "The end of time range (ISO-8601: yyyy-MM-dd'T'HH:mm:ss)",
                converter = ISO8601TimestampConverter.class
        )
        Instant timeRangeTo = Instant.now();

        @Parameter(
                names = {"--fetched-attempts"},
                description = "The number of fetched attempt records at once (default: 1000)"
        )
        int fetchedAttempts = 1000;

        @Parameter(
                names = {"--partition-size"},
                description = "The number of internal partition size (default: 100)"
        )
        int partitionSize = 100;

        @Parameter(
                names = {"--database-wait-millis"},
                description = "The internal wait (milliseconds) between database transactions (default: 100)"
        )
        int databaseWaitMillis = 100;
    }

    static class ISO8601TimestampConverter
            implements IStringConverter<Instant>
    {
        private static final DateTimeFormatter FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        @Override
        public Instant convert(String value) {
            try {
                return LocalDateTime
                        .parse(value, FORMATTER)
                        .atOffset(ZoneOffset.UTC)
                        .toInstant();
            } catch (DateTimeParseException e) {
                throw new ParameterException(
                        "Invalid timestamp. It should be IS-O8601 format (yyyy-MM-dd'T'HH:mm:ss)", e);
            }
        }
    }

    void run(ConfigElement configElement, Args args)
            throws IOException
    {
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
        new TaskAnalyzer(injector)
                .run(
                        System.out,
                        args.timeRangeFrom,
                        args.timeRangeTo,
                        args.fetchedAttempts,
                        args.partitionSize,
                        args.databaseWaitMillis
                );
    }

    public static void main(final String[] args)
            throws Exception
    {
        Args commandArgs = new Args();
        new JCommander(commandArgs).parse(args);

        ConfigElement configElement = new ConfigElementLoader(commandArgs.configFile).load();

        Main main = new Main();
        main.run(configElement, commandArgs);
    }
}
