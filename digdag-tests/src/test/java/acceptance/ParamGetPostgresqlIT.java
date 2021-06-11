package acceptance;

import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.pg.PgConnectionConfig;
import org.junit.Test;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.main;

public class ParamGetPostgresqlIT
        extends BaseParamPostgresqlIT
{
    @Test
    public void testGetParams()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/get.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.type=postgresql",
                "param_server.user=" + user,
                "param_server.host=" + host,
                "param_server.database=" + tempDatabase
        ));

        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(String.format(
                    "insert into params (key, value, value_type, site_id, created_at, updated_at) " +
                            "values ('%s', '%s', %d, %d, now(), now())",
                    "key1", "{\"value\": \"value1\"}", 0, 0));
            conn.executeUpdate(String.format(
                    "insert into params (key, value, value_type, site_id, created_at, updated_at) " +
                            "values ('%s', '%s', %d, %d, now(), now())",
                    "key2", "{\"value\": \"value2\"}", 0, 0));
        }

        String output = folder.newFolder().getAbsolutePath();
        CommandStatus status = main("run",
                "-o", output,
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("get.dig").toString()
        );

        assertCommandStatus(status);
        assertThat(new String(Files.readAllBytes(projectDir.resolve("out"))).trim(), is("value1 value2"));
        cleanupParamTable();
    }

    @Test
    public void testGetExpiredParamsAreInvisible()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/get.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.type=postgresql",
                "param_server.user=" + user,
                "param_server.host=" + host,
                "param_server.database=" + tempDatabase
        ));

        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            int expiredUpdatedAt = 60 * 60 * 24 * 90 + 1;
            // expired param(last update is 90 days + 1second ago)
            conn.executeUpdate(String.format(
                    "insert into params (key, value, value_type, site_id, created_at, updated_at) " +
                            "values ('%s', '%s', %d, %d, now(), now() - interval '" + String.valueOf(expiredUpdatedAt) + " second')",
                    "key1", "{\"value\": \"value1\"}", 0, 0));
            int notExpiredUpdatedAt = 60 * 60 * 24 * 89;
            // not expired param(last update is 89 days ago)
            conn.executeUpdate(String.format(
                    "insert into params (key, value, value_type, site_id, created_at, updated_at) " +
                            "values ('%s', '%s', %d, %d, now(), now() - interval '" + String.valueOf(notExpiredUpdatedAt) + " second')",
                    "key2", "{\"value\": \"value2\"}", 0, 0));
        }

        String output = folder.newFolder().getAbsolutePath();
        CommandStatus status = main("run",
                "-o", output,
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("get.dig").toString()
        );

        assertCommandStatus(status);
        assertThat(new String(Files.readAllBytes(projectDir.resolve("out"))).trim(), is("value2"));
        cleanupParamTable();
    }

    @Test
    public void testParallelGetParams()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/parallel_get.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.type=postgresql",
                "param_server.user=" + user,
                "param_server.host=" + host,
                "param_server.database=" + tempDatabase
        ));

        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(String.format(
                    "insert into params (key, value, value_type, site_id, created_at, updated_at) " +
                            "values ('%s', '%s', %d, %d, now(), now())",
                    "key1", "{\"value\": \"value1\"}", 0, 0));
            conn.executeUpdate(String.format(
                    "insert into params (key, value, value_type, site_id, created_at, updated_at) " +
                            "values ('%s', '%s', %d, %d, now(), now())",
                    "key2", "{\"value\": \"value2\"}", 0, 0));
        }

        String output = folder.newFolder().getAbsolutePath();
        CommandStatus status = main("run",
                "-o", output,
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("parallel_get.dig").toString()
        );

        assertCommandStatus(status);
        assertThat(new String(Files.readAllBytes(projectDir.resolve("out"))).trim(), is("value1 value2"));
        cleanupParamTable();
    }
}
