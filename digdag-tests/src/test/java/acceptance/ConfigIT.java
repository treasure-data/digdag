package acceptance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;
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
    public void propertyByFile() throws Exception
    {
        copyResource("acceptance/params.yml", root().resolve("params.yml"));
        TestUtils.fakeHome(root().resolve("home").toString(), () -> {
            Files.write(root().resolve("home").resolve(".digdag").resolve("config"), "params.mysql.password=secret".getBytes(UTF_8));
            main("run", "-o", root().toString(), "-f", root().resolve("params.yml").toString());
        });
        assertThat(Files.readAllBytes(root().resolve("foo.out")), is("secret\n".getBytes(UTF_8)));
    }
}
