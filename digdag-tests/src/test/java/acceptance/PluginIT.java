package acceptance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static utils.TestUtils.copyResource;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PluginIT
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
        Path pluginPath = Paths.get("src/test/resources/acceptance/plugin/digdag-plugin-example").toAbsolutePath();

        copyResource("acceptance/plugin/plugin.dig", root().resolve("plugin.dig"));
        copyResource("acceptance/plugin/template.txt", root().resolve("template.txt"));
        CommandStatus cmd = TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "plugin.dig", "-p", "repository_path=" + pluginPath.resolve("build").resolve("repo"), "-X", "plugin.local-path=" + root().resolve(".digdag/plugins").toString());
        assertThat(cmd.errUtf8(), cmd.code(), is(0));
        assertThat(Files.exists(root().resolve("example.out")), is(true));
        assertThat(
                new String(Files.readAllBytes(root().resolve("example.out")), UTF_8).trim(),
                is("Worked? yes"));
    }
}
