package io.digdag.cli;

import com.beust.jcommander.Parameter;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.database.DataSourceProvider;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.database.migrate.Migration;
import org.jdbi.v3.core.Jdbi;

import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import static io.digdag.cli.SystemExitException.systemExit;


public class Migrate
    extends Command
{
    @Parameter(names = {"-o", "--database"})
    String database = null;

    SubCommand subCommand = null;

    @Override
    public void main()
            throws Exception
    {
        checkArgs();
        DatabaseConfig dbConfig = DatabaseConfig.convertFrom(buildConfig());
        try (DataSourceProvider dsp = new DataSourceProvider(dbConfig)) {
            Jdbi dbi = Jdbi.create(dsp.get());
            DatabaseMigrator migrator = new DatabaseMigrator(dbi, dbConfig);
            switch (subCommand) {
                case RUN:
                    runMigrate(migrator);
                    break;
                case CHECK:
                    checkMigrate(migrator);
                    break;
                default:
                    throw new RuntimeException("No command");
            }
        }
    }

    // migrate run
    private void runMigrate(DatabaseMigrator migrator)
            throws Exception
    {
        int numApplied = migrator.migrate();
        if (numApplied == 0) {
            out.println("No update");
        }
        else {
            out.println("Migrations successfully finished");
        }
    }

    // migrate check
    private void checkMigrate(DatabaseMigrator migrator)
            throws Exception
    {
        if (!migrator.existsSchemaMigrationsTable()) {
            out.println("No table exist");
            return;
        }

        List<Migration> migrations = migrator.getApplicableMigration();
        for (Migration m : migrations) {
            out.println(m.getVersion());
        }
        if (migrations.size() == 0) {
            out.println("No update");
        }
    }

    // check and validate arguments
    private void checkArgs()
        throws Exception
    {
        switch (args.size()) {
            case 1:
                switch (args.get(0)) {
                    case "run":
                        subCommand = SubCommand.RUN;
                        break;
                    case "check":
                        subCommand = SubCommand.CHECK;
                        break;
                    default:
                        throw usage("Invalid command");
                }
                break;
            default:
                throw usage("Invalid parameters");
        }

        if (database == null && configPath == null) {
            throw usage("--database, or --config option is required");
        }
    }

    // build Config from arguments and properties
    private Config buildConfig()
            throws Exception
    {
        Properties props = loadSystemProperties();
        if (database != null) {
            props.setProperty("database.type", "h2");
            props.setProperty("database.path", Paths.get(database).toAbsolutePath().toString());
        }
        ConfigFactory cf = new ConfigFactory(DigdagClient.objectMapper());
        Config config = PropertyUtils.toConfigElement(props).toConfig(cf);
        return config;
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " migrate (run|check) run or check database migration");
        err.println("  Options:");
        err.println("    -c, --config PATH.properties     configuration file (default: ~/.config/digdag/config)");
        err.println("    -o, --database DIR               path to H2 database");
        return systemExit(error);
    }

    private enum SubCommand
    {
        RUN("run"),
        CHECK("check")
        ;
        private final String name;

        SubCommand(String name)
        {
            this.name = name;
        }
    }
}
