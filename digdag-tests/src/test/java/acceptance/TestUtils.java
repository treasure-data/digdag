package acceptance;

import com.google.common.io.Resources;
import io.digdag.cli.Main;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

class TestUtils
{
    static int main(String... args)
    {
        return new Main().cli(args);
    }

    static void copyResource(String resource, Path dest) throws IOException
    {
        try (InputStream input = Resources.getResource(resource).openStream()) {
            Files.copy(input, dest);
        }
    }
}
