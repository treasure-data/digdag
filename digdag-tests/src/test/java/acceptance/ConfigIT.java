package acceptance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConfigIT
{

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void propertyByFile()
            throws Exception
    {
        copyResource("acceptance/params.dig", root().resolve("params.dig"));
        TestUtils.fakeHome(root().resolve("home").toString(), () -> {
            Path configFile = root().resolve("home").resolve(".config").resolve("digdag").resolve("config");
            Files.write(configFile, "params.mysql.password=secret".getBytes(UTF_8));
            main("run", "-o", root().toString(), "--project", root().toString(), "params.dig");
        });
        assertThat(Files.readAllBytes(root().resolve("foo.out")), is("secret\n".getBytes(UTF_8)));
    }

    @Test
    public void verifyThatCommandLineParamsOverridesConfigFileParams()
            throws Exception
    {
        copyResource("acceptance/params.dig", root().resolve("params.dig"));
        TestUtils.fakeHome(root().resolve("home").toString(), () -> {
            Path configFile = root().resolve("home").resolve(".config").resolve("digdag").resolve("config");
            Files.write(configFile, "params.mysql.password=secret".getBytes(UTF_8));
            main("run",
                    "-o", root().toString(),
                    "--project", root().toString(),
                    "params.dig",
                    "-p", "mysql.password=override");
        });
        assertThat(Files.readAllLines(root().resolve("foo.out")), contains("override"));
    }
}
