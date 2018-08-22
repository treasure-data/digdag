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
import static org.junit.Assert.assertThat;
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
                "user_database.type=postgresql",
                "user_database.user=" + user,
                "user_database.host=" + host,
                "user_database.database=" + tempDatabase
        ));

        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(
                    "CREATE TABLE public.params (" +
                            "key text NOT NULL," +
                            "value text NOT NULL," +
                            "updated_at timestamp with time zone NOT NULL," +
                            "created_at timestamp with time zone NOT NULL)"
            );

            conn.executeUpdate(String.format(
                    "insert into params (key, value, created_at, updated_at) values ('%s', '%s', now(), now())",
                    "key1", "value1"));
            conn.executeUpdate(String.format(
                    "insert into params (key, value, created_at, updated_at) values ('%s', '%s', now(), now())",
                    "key2", "value2"));
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
    }
}
