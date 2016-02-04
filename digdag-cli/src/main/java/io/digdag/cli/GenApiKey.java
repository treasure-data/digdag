package io.digdag.cli;

import java.io.File;
import java.io.PrintStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import com.beust.jcommander.Parameter;
import com.google.common.io.ByteStreams;
import io.digdag.client.api.RestApiKey;
import static io.digdag.cli.Main.systemExit;

public class GenApiKey
    extends Command
{
    @Parameter(names = {"-o", "--output"})
    String outputPath = null;

    @Override
    public void main()
        throws Exception
    {
        if (args.size() != 0) {
            throw usage(null);
        }
        genApiKey();
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag genapikey");
        System.err.println("  Options:");
        System.err.println("    -o, --output DIR                 creates server and client configration files");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void genApiKey()
        throws Exception
    {
        RestApiKey key = RestApiKey.randomGenerate();

        if (outputPath == null) {
            System.out.println("Generated API key:");
            System.out.println(key.toString());
            System.out.println("");
            System.out.println("Use `digdag geapikey -o <DIR>` command to create key files.");
        }
        else {
            Path path = FileSystems.getDefault().getPath(outputPath);
            System.out.println("Creating " + path);
            Files.createDirectories(path);

            Path clientPropPath = path.resolve("client.properties");
            System.out.println("  Creating " + clientPropPath);
            try (PrintStream out = new PrintStream(Files.newOutputStream(clientPropPath))) {
                out.println("endpoint = 127.0.0.1:65432");
                out.println("apikey = " + key.toString());
            }

            Path serverPropPath = path.resolve("server.properties");
            System.out.println("  Creating " + serverPropPath);
            try (PrintStream out = new PrintStream(Files.newOutputStream(serverPropPath))) {
                out.println("database.type = memory");
                out.println("server.bind = 0.0.0.0");
                out.println("server.port = 65432");
                out.println("server.apikey = " + key.toString());
            }

            System.out.println("Done.");
            System.out.println("For server, please run `digdag server` command with `-c server.properties` option.");
            System.out.println("For client, please copy `client.properties file to ~/.digdag/cilent.properties.");
        }
    }
}
