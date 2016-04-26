package acceptance;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import io.digdag.cli.Main;
import io.digdag.core.Version;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static io.digdag.core.Version.buildVersion;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

class TestUtils
{
    static CommandStatus main(String... args)
    {
        return main(buildVersion(), args);
    }

    static CommandStatus main(Version localVersion, String... args)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
        final int code;
        try {
            code = new Main(localVersion, new PrintStream(out), new PrintStream(err)).cli(args);
        }
        catch (RuntimeException e) {
            e.printStackTrace();
            Assert.fail();
            throw e;
        }
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    static void copyResource(String resource, Path dest) throws IOException
    {
        try (InputStream input = Resources.getResource(resource).openStream()) {
            Files.copy(input, dest, REPLACE_EXISTING);
        }
    }
}
