package acceptance;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class TdForEachIT
{
    private static final String TD_API_KEY = System.getenv("TD_API_KEY");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        projectDir = folder.getRoot().toPath();
        config = folder.newFile().toPath();
        Files.write(config, ("params.td.apikey = " + TD_API_KEY).getBytes("UTF-8"));
    }

    @Test
    public void testTdForEach()
            throws Exception
    {
        copyResource("acceptance/td/td_for_each/td_for_each.dig", projectDir.resolve("workflow.dig"));
        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "workflow.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        // Check that the expected files were created
        assertThat(Files.exists(projectDir.resolve("out-a1-b1")), is(true));
        assertThat(Files.exists(projectDir.resolve("out-a2-b2")), is(true));
    }
}
