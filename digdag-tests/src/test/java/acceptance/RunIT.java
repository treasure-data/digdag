package acceptance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RunIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void testRun()
            throws Exception
    {
        copyResource("acceptance/basic.dig", root().resolve("basic.dig"));
        CommandStatus runStatus = main("run",
                "-c", "/dev/null",
                "-o", root().toString(),
                "--project", root().toString(),
                "basic.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(Files.exists(root().resolve("foo.out")), is(true));
        assertThat(Files.exists(root().resolve("bar.out")), is(true));
    }
}
