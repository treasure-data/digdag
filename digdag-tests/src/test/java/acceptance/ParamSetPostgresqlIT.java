package acceptance;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.pg.PgConnectionConfig;
import org.junit.Test;
import utils.CommandStatus;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.main;

public class ParamSetPostgresqlIT
        extends BaseParamPostgresqlIT
{

    @Test
    public void setValueToPostgresql()
            throws IOException, NotReadOnlyException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.database.type=postgresql",
                "param_server.database.user=" + user,
                "param_server.database.host=" + host,
                "param_server.database.database=" + tempDatabase
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("set.dig").toString()
        );
        assertCommandStatus(status);

        SecretProvider secrets = getDatabaseSecrets();
        List<Map<String, Object>> expectedValues = Arrays.asList(
                ImmutableMap.of("key", "key1", "value", "{\"value\":\"value1\"}"),
                ImmutableMap.of("key", "key2", "value", "{\"value\":\"value2\"}"));

        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeReadOnlyQuery("select * from params order by created_at asc",
                    (rs) -> {
                        assertThat(rs.getColumnNames(), is(Arrays.asList("key", "value", "value_type", "site_id", "updated_at", "created_at")));
                        List<Object> row;
                        int index = 0;
                        while ((row = rs.next()) != null) {
                            Map<String, Object> expected = expectedValues.get(index);
                            assertThat(row.get(0), is(expected.get("key")));
                            assertThat(row.get(1), is(expected.get("value")));
                            ++index;
                        }
                    });
        }
    }

    @Test
    public void testErrorIfParamServerTypeIsNotSet()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.database.user=" + user,
                "param_server.database.host=" + host,
                "param_server.database.database=" + tempDatabase
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("set.dig").toString()
        );
        assertCommandStatus(status, Optional.of("param_server.database.type is required to use this operator."));
    }

    @Test
    public void testErrorIfParamServerTypeIsMySQL()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.database.type=mysql",
                "param_server.database.user=" + user,
                "param_server.database.host=" + host,
                "param_server.database.database=" + tempDatabase
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("set.dig").toString()
        );
        assertCommandStatus(status, Optional.of("Not supported database type: mysql"));
    }

    @Test
    public void testErrorIfUserIsNotSpecified()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.database.type=postgresql",
                "param_server.database.host=" + host
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("set.dig").toString()
        );
        assertCommandStatus(status, Optional.of("Parameter 'param_server.database.user' is required but not set"));
    }
}
