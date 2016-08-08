package acceptance;

import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import utils.TestUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static utils.TestUtils.copyResource;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RunParamsPriorityIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void runAll()
            throws IOException
    {
        copyResource("acceptance/params_priority/params_priority.dig", root().resolve("params_priority.dig"));
        copyResource("acceptance/params_priority/helper.py", root().resolve("helper.py"));
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "params_priority.dig", "--session", "2016-01-01 00:00:00");
    }

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private void assertOutputContents(String name, String contents)
            throws IOException
    {
        assertThat(
                new String(Files.readAllBytes(root().resolve(name + ".out")), UTF_8).trim(),
                is(contents));
    }

    @Test
    public void testParams()
            throws Exception
    {
        assertOutputContents("simple_export", "simple_export");
        assertOutputContents("export_overwrite", "export_overwrite");
        assertOutputContents("simple_store", "simple_store");
        assertOutputContents("store_overwrite", "store_overwrite");
        assertOutputContents("parallel_store", "b");
        assertOutputContents("dag_store", "c");
    }
}
