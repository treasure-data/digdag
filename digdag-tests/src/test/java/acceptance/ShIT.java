package acceptance;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import utils.CommandStatus;

public class ShIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path projectDir;
    private Path config;
    private Path outfile;

    @Before
    public void setUp()
        throws Exception
    {
        //This checks Windows environment and then skip
        assumeThat(runEcho(), is(true));

        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        outfile = projectDir.resolve("outfile");
    }

    @Test
    public void verifyEnvVars()
            throws Exception
    {
        Files.write(config, ImmutableList.of(
                    "secrets.my_secrets.d = secret-shared",
                    "secrets.my_secrets.e = secret-only"
                    ));

        copyResource("acceptance/sh/env.dig", projectDir.resolve("workflow.dig"));

        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "cmd=" + "command-line",
                "-p", "outfile=" + outfile,
                "workflow.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        assertThat(Files.readAllLines(outfile, UTF_8).get(0), is("exported 1 command-line secret-shared secret-only"));
    }

    @Test
    public void verifySecretOnlyAccess()
            throws Exception
    {
        copyResource("acceptance/sh/env_secret_only_reject.dig", projectDir.resolve("workflow.dig"));

        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "workflow.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), not(is(0)));

        assertThat(runStatus.errUtf8(), containsString("Secret not found"));
    }

    private boolean runEcho()
        throws Exception
    {
        ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", "echo $var");
        pb.environment().put("var", "unix");
        try {
            Process p = pb.start();
            try (BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream(), UTF_8))) {
                return stdout.readLine().trim().equals("unix") && p.waitFor() == 0;
            }
        }
        catch (IOException ex) {
            return false;
        }
    }
}
