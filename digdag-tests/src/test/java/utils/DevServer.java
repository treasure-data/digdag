package utils;

import com.google.common.collect.ImmutableMap;
import io.digdag.cli.Main;
import io.digdag.core.Version;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DevServer
{
    public static void main(String[] args)
            throws IOException
    {
        Path logdir = Files.createTempDirectory("digdag-task-logs");
        Main main = new Main(Version.buildVersion(), ImmutableMap.of(), System.out, System.err, System.in);
        main.cli("server",
                "-c", "/dev/null",
                "-m",
                "-O", logdir.toString(),
                "-H", "Access-Control-Allow-Origin=http://localhost:8080",
                "-H", "Access-Control-Allow-Headers=origin, content-type, accept, authorization",
                "-H", "Access-Control-Allow-Credentials=true",
                "-H", "Access-Control-Allow-Methods=GET, POST, PUT, DELETE, OPTIONS, HEAD",
                "-H", "Access-Control-Max-Age=1209600");
    }
}
