package acceptance;

import java.io.IOException;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import io.digdag.cli.Main;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BasicIT
{
    public static int main(String... args)
    {
        return new Main().cli(args);
    }

    private interface Action
    {
        public void run() throws Exception;
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path definition;

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private void copyResource(String resource, Path dest) throws IOException
    {
        try (InputStream input = Resources.getResource(resource).openStream()) {
            Files.copy(input, dest);
        }
    }

    private void fakeHome(String home, Action a) throws Exception
    {
        String orig = System.setProperty("user.home", home);
        try {
            Files.createDirectories(Paths.get(home).resolve(".digdag"));
            a.run();
        }
        finally {
            System.setProperty("user.home", orig);
        }
    }

    @Test
    public void name() throws Exception
    {
        copyResource("acceptance/basic.yml", root().resolve("basic.yml"));
        main("run", "-o", root().toString(), "-f", root().resolve("basic.yml").toString());
        assertThat(Files.exists(root().resolve("foo.out")), is(true));
        assertThat(Files.exists(root().resolve("bar.out")), is(true));
    }

    @Test
    public void propertyByFile() throws Exception
    {
        copyResource("acceptance/params.yml", root().resolve("params.yml"));
        fakeHome(root().resolve("home").toString(), () -> {
            Files.write(root().resolve("home").resolve(".digdag").resolve("config"), "params.mysql.password=secret".getBytes(UTF_8));
            main("run", "-o", root().toString(), "-f", root().resolve("params.yml").toString());
        });
        assertThat(Files.readAllBytes(root().resolve("foo.out")), is("secret\n".getBytes(UTF_8)));
    }
}
