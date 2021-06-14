package acceptance;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharSource;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptFailure;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.main;
import static utils.TestUtils.pushProject;
import static utils.TestUtils.startWorkflow;

public class SecretsIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TemporaryDigdagServer server;

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.newFolder().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
    }

    @After
    public void tearDownClient()
            throws Exception
    {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @After
    public void tearDownServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    private void startServer()
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .configuration("digdag.secret-encryption-key = " + Base64.getEncoder().encodeToString(RandomUtils.nextBytes(16)))
                .build();

        server.start();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void testSetListDeleteProjectSecrets()
            throws Exception
    {
        startServer();

        String projectName = "test";

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                projectName,
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Command line args
        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";

        // Single file value
        String key3 = "key3";
        String value3 = "value3";

        // Single stdin value
        String key4 = "key4";
        String value4 = "value4";

        // Multiple stdin values
        String key5 = "key5";
        String value5 = "value5";
        String key6 = "key6";
        String value6 = "value6";

        // Multiple file values
        String key7 = "key7";
        String value7 = "value7";
        String key8 = "key8";
        String value8 = "value8";

        Path value3File = folder.newFile().toPath();
        Files.write(value3File, ImmutableList.of(value3));

        // Set secrets on command line and from file
        {
            CommandStatus setStatus = main("secrets",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectName,
                    "--set",
                    key1 + '=' + value1,
                    key2 + '=' + value2,
                    key3 + "=@" + value3File.toString());
            assertThat(setStatus.errUtf8(), setStatus.code(), is(0));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key1 + "' set"));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key2 + "' set"));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key3 + "' set"));
        }

        // Set secret from stdin
        {
            InputStream in = new ByteArrayInputStream(value4.getBytes(US_ASCII));
            CommandStatus setStatus = main(
                    in,
                    "secrets",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectName,
                    "--set",
                    key4 + "=-");
            assertThat(setStatus.errUtf8(), setStatus.code(), is(0));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key4 + "' set"));
        }

        YAMLFactory yamlFactory = new YAMLFactory();

        // Set secrets from stdin
        {
            ByteArrayOutputStream yaml = new ByteArrayOutputStream();
            YAMLGenerator generator = yamlFactory.createGenerator(yaml);
            TestUtils.objectMapper().writeValue(generator, ImmutableMap.of(key5, value5, key6, value6));
            InputStream in = new ByteArrayInputStream(yaml.toByteArray());

            CommandStatus setStatus = main(
                    in,
                    "secrets",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectName,
                    "--set", "@-");
            assertThat(setStatus.errUtf8(), setStatus.code(), is(0));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key5 + "' set"));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key6 + "' set"));
        }

        // Set secrets from file
        Path secretsFileYaml = folder.newFolder().toPath().resolve("secrets.yaml");
        try (OutputStream o = Files.newOutputStream(secretsFileYaml)) {
            YAMLGenerator generator = yamlFactory.createGenerator(o);
            TestUtils.objectMapper().writeValue(generator, ImmutableMap.of(key7, value7, key8, value8));

            CommandStatus setStatus = main(
                    "secrets",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectName,
                    "--set", "@" + secretsFileYaml);
            assertThat(setStatus.errUtf8(), setStatus.code(), is(0));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key7 + "' set"));
            assertThat(setStatus.errUtf8(), containsString("Secret '" + key8 + "' set"));
        }

        // List secrets
        CommandStatus listStatus = main("secrets",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "--project", projectName);
        assertThat(listStatus.errUtf8(), listStatus.code(), is(0));

        List<String> listedKeys = CharSource.wrap(listStatus.outUtf8()).readLines();
        assertThat(listedKeys, containsInAnyOrder(key1, key2, key3, key4, key5, key6, key7, key8));

        // Delete secrets
        CommandStatus deleteStatus = main("secrets",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "--project", projectName,
                "--delete", key1,
                "--delete", key2, key3);
        assertThat(deleteStatus.errUtf8(), deleteStatus.code(), is(0));
        assertThat(deleteStatus.errUtf8(), containsString("Secret 'key1' deleted"));
        assertThat(deleteStatus.errUtf8(), containsString("Secret 'key2' deleted"));
        assertThat(deleteStatus.errUtf8(), containsString("Secret 'key3' deleted"));

        // List secrets after deletion
        CommandStatus listStatus2 = main("secrets",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "--project", projectName);
        assertThat(listStatus2.errUtf8(), listStatus2.code(), is(0));

        List<String> listedKeys2 = CharSource.wrap(listStatus2.outUtf8()).readLines();
        assertThat(listedKeys2, containsInAnyOrder(key4, key5, key6, key7, key8));
    }

    private void testUseProjectSecret(String baseValue)
            throws Exception
    {
        startServer();

        String projectName = "test";

        // Push the project
        {
            copyResource("acceptance/secrets/echo_secret.dig", projectDir);
            copyResource("acceptance/secrets/echo_secret_parameterized.dig", projectDir);
            pushProject(server.endpoint(), projectDir, projectName);
        }

        String key1 = "key1";
        String key2 = "key2";

        String value1 = baseValue + "1";
        String value2 = baseValue + "2";

        // Set secrets
        {
            CommandStatus setStatus = main("secrets",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectName,
                    "--set", key1 + '=' + value1, key2 + '=' + value2);
            assertThat(setStatus.errUtf8(), setStatus.code(), is(0));
        }

        // Start workflows using secrets
        {
            Path outfile = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out");
            Path outfileParameterized = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out-parameterized");

            Id attemptId = startWorkflow(
                    server.endpoint(), projectName, "echo_secret",
                    ImmutableMap.of("OUTFILE", outfile.toString()));

            Id attemptIdParameterized = startWorkflow(
                    server.endpoint(), projectName, "echo_secret_parameterized",
                    ImmutableMap.of("secret_key", "key2",
                            "OUTFILE", outfileParameterized.toString()));

            expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
            expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptIdParameterized));

            List<String> output = Files.readAllLines(outfile);
            assertThat(output, contains(value1));

            List<String> outputParameterized = Files.readAllLines(outfileParameterized);
            assertThat(outputParameterized, contains(value2));
        }

        // Delete a secret
        {
            CommandStatus deleteStatus = main("secrets",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectName,
                    "--delete", key1);
            assertThat(deleteStatus.errUtf8(), deleteStatus.code(), is(0));
            assertThat(deleteStatus.errUtf8(), containsString("Secret 'key1' deleted"));
        }

        // Attempt to run a workflow that uses the deleted secret
        {
            Path outfile = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out");

            Id attemptId = startWorkflow(
                    server.endpoint(), projectName, "echo_secret",
                    ImmutableMap.of("OUTFILE", outfile.toString()));

            expect(Duration.ofMinutes(5), attemptFailure(server.endpoint(), attemptId));

            String logs = getAttemptLogs(client, attemptId);
            assertThat(logs, containsString("Secret not found for key: 'key1'"));
        }
    }

    @Test
    public void useProjectSecretWithNormalValue()
            throws Exception
    {
        testUseProjectSecret("value");
    }

    @Test
    public void useProjectSecretWithSymbolValue()
            throws Exception
    {
        testUseProjectSecret("!#$%*+-=?@^_$");
    }

    @Test
    public void useProjectSecretWithSymbolValueConsideringCompatibility()
            throws Exception
    {
        testUseProjectSecret("!#\\$%*+-=?@^_\\$");
    }

    @Test
    public void testUseProjectSecret()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/secrets/echo_secret.dig");

        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";
        String value3 = "123";

        Map<String, String> env = ImmutableMap.of("DIGDAG_CONFIG_HOME", folder.newFolder().toPath().toString());

        // Set secrets
        {
            CommandStatus status = main(env, "secrets",
                    "-c", config.toString(),
                    "--local",
                    "--set", key1 + '=' + value1, key2 + '=' + value2);
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(status.errUtf8(), containsString("Secret 'key1' set"));
            assertThat(status.errUtf8(), containsString("Secret 'key2' set"));
        }

        // List secrets
        {
            CommandStatus status = main(env, "secrets",
                    "-c", config.toString(),
                    "--local");
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(status.outUtf8(), containsString("key1"));
            assertThat(status.outUtf8(), containsString("key2"));
            assertThat(status.outUtf8(), not(containsString("value1")));
            assertThat(status.outUtf8(), not(containsString("value2")));
            assertThat(status.outUtf8(), not(containsString("value3")));
        }

        // Run workflows using secrets
        {
            Path outfile = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out");

            CommandStatus status = TestUtils.main(env, "run",
                    "-c", config.toString(),
                    "-o", folder.newFolder().toString(),
                    "--project", projectDir.toString(),
                    "-p", "OUTFILE=" + outfile.toString(),
                    "echo_secret");

            assertThat(status.errUtf8(), status.code(), is(0));

            List<String> output = Files.readAllLines(outfile);
            assertThat(output, contains(value1));
        }

        // Set secrets existing key and run workflow
        {
            CommandStatus secretsStatus = main(env, "secrets",
                    "-c", config.toString(),
                    "--local",
                    "--set", key1 + '=' + value3); // update key1 by value3
            assertThat(secretsStatus.errUtf8(), secretsStatus.code(), is(0));
            assertThat(secretsStatus.errUtf8(), containsString("Secret 'key1' set"));

            Path outfile = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out");

            CommandStatus runStatus = TestUtils.main(env, "run",
                    "-c", config.toString(),
                    "-o", folder.newFolder().toString(),
                    "--project", projectDir.toString(),
                    "-p", "OUTFILE=" + outfile.toString(),
                    "echo_secret");

            assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

            List<String> output = Files.readAllLines(outfile);
            assertThat(output, contains(value3));
        }

        // Delete a secret
        {
            CommandStatus deleteStatus = main(env, "secrets",
                    "-c", config.toString(),
                    "--local",
                    "--delete", key1);
            assertThat(deleteStatus.errUtf8(), deleteStatus.code(), is(0));
            assertThat(deleteStatus.errUtf8(), containsString("Secret 'key1' deleted"));
        }

        // List secrets
        {
            CommandStatus status = main(env, "secrets",
                    "-c", config.toString(),
                    "--local");
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(status.outUtf8(), not(containsString("key1")));
            assertThat(status.outUtf8(), containsString("key2"));
            assertThat(status.outUtf8(), not(containsString("value1")));
            assertThat(status.outUtf8(), not(containsString("value2")));
            assertThat(status.outUtf8(), not(containsString("value3")));
        }

        // Attempt to run a workflow that uses the deleted secret
        {
            Path outfile = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out");

            CommandStatus status = main(env, "run",
                    "-c", config.toString(),
                    "-o", folder.newFolder().toString(),
                    "--project", projectDir.toString(),
                    "-p", "OUTFILE=" + outfile.toString(),
                    "echo_secret");
            assertThat(status.errUtf8(), status.code(), not(is(0)));
            assertThat(status.errUtf8(), containsString("Secret not found for key: 'key1'"));
        }
    }

    @Test
    public void verifyInvalidSecretUseFails()
            throws Exception
    {
        startServer();

        String projectName = "test";

        // Push the project
        copyResource("acceptance/secrets/invalid_secret_use.dig", projectDir);
        copyResource("acceptance/secrets/echo_secret_parameterized.dig", projectDir);
        pushProject(server.endpoint(), projectDir, projectName);

        String key1 = "key1";
        String key2 = "key2";

        String value1 = "value1";
        String value2 = "value2";

        // Set secrets
        CommandStatus setStatus = main("secrets",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "--project", projectName,
                "--set", key1 + '=' + value1, key2 + '=' + value2);
        assertThat(setStatus.errUtf8(), setStatus.code(), is(0));

        Id attemptId = startWorkflow(server.endpoint(), projectName, "invalid_secret_use", ImmutableMap.of());

        expect(Duration.ofMinutes(5), attemptFailure(server.endpoint(), attemptId));

        String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("\"key1\" is not defined"));
    }

    @Test
    public void verifyAccessIsGrantedToUserSecretTemplateKeys()
            throws Exception
    {
        startServer();

        String projectName = "test";

        // Push the project
        copyResource("acceptance/secrets/user_secret_template.dig", projectDir);
        pushProject(server.endpoint(), projectDir, projectName);

        // Set secrets
        CommandStatus setStatus = main("secrets",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "--project", projectName,
                "--set", "foo=foo_value", "nested.bar=bar_value");
        assertThat(setStatus.errUtf8(), setStatus.code(), is(0));

        Path outfile = folder.newFolder().toPath().toAbsolutePath().normalize().resolve("out");
        Id attemptId = startWorkflow(server.endpoint(), projectName, "user_secret_template", ImmutableMap.of(
                "OUTFILE", outfile.toString()
        ));

        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));

        List<String> output = Files.readAllLines(outfile);
        assertThat(output, contains("foo=foo_value bar=bar_value"));
    }
}
