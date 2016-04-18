package acceptance;

import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.digdag.cli.Main.main;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BasicIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path definition;

    @Before
    public void setUp() throws Exception {
        definition = folder.getRoot().toPath().resolve("workflow.yml");
        try (InputStream input = Resources.getResource("acceptance/basic.yml").openStream()) {
            Files.copy(input, definition);
        }
    }

    @Test
    public void name() throws Exception {
        main("run", "-o", folder.getRoot().toString(), "-f", definition.toString(), "-p", "_workdir=" + folder.getRoot());
        assertThat(Files.exists(folder.getRoot().getAbsoluteFile().toPath().resolve("foo.out")), is(true));
        assertThat(Files.exists(folder.getRoot().getAbsoluteFile().toPath().resolve("bar.out")), is(true));
    }
}
