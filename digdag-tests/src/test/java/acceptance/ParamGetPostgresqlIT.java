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
                "param_server.database.type=postgresql",
                "param_server.database.user=" + user,
                "param_server.database.host=" + host,
                "param_server.database.database=" + tempDatabase
        ));

        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(
                    "CREATE TABLE params (" +
                            "key text NOT NULL," +
                            "value text NOT NULL," +
                            "site_id integer," +
                            "updated_at timestamp with time zone NOT NULL," +
                            "created_at timestamp with time zone NOT NULL," +
                            "CONSTRAINT params_site_id_key_uniq UNIQUE(site_id, key)" +
                            ")"
            );

            conn.executeUpdate(String.format(
                    "insert into params (key, value, site_id, created_at, updated_at) values ('%s', '%s', %d, now(), now())",
                    "key1", "value1", 0));
            conn.executeUpdate(String.format(
                    "insert into params (key, value, site_id, created_at, updated_at) values ('%s', '%s', %d, now(), now())",
                    "key2", "value2", 0));
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
