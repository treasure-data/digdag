package io.digdag.cli;

import java.io.PrintStream;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;
import static io.digdag.cli.SystemExitException.systemExit;

public class Init
    extends Command
{
    public Init(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    public void main()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        init(args.get(0));
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag init <dir>");
        err.println("  Options:");
        Main.showCommonOptions(err);
        err.println("  Example:");
        err.println("    $ digdag init mydag");
        return systemExit(error);
    }

    private void init(String name)
        throws Exception
    {
        Path destDir = Paths.get(name);
        boolean isCurrentDirectory;

        Path currentDirectory = Paths.get("").toAbsolutePath();
        String workflowName;
        if (destDir.normalize().toAbsolutePath().equals(currentDirectory)) {
            isCurrentDirectory = true;
            workflowName = currentDirectory.getFileName().toString();
        }
        else {
            isCurrentDirectory = false;
            workflowName = destDir.getFileName().toString();
        }
        String workflowFileName = workflowName + WORKFLOW_FILE_SUFFIX;

        ResourceGenerator gen = new ResourceGenerator("/digdag/cli/", destDir);

        gen.mkdir(".");  // creates destDir itself

        //if (!gen.exists("digdag")) {
        //    gen.cp("digdag.sh", "digdag");
        //    gen.setExecutable("digdag");
        //}
        //
        //gen.mkdir(".digdag-wrapper");
        //File localJarPath = getFatJarPath();
        //if (localJarPath != null) {
        //    gen.cpAbsoluteSource(localJarPath, ".digdag-wrapper/digdag.jar");
        //    gen.setExecutable(".digdag-wrapper/digdag.jar");
        //}

        if (gen.exists(workflowFileName)) {
            out.println("File " + gen.path(workflowFileName) + " already exists.");
        }
        else {
            if (!gen.exists(".gitignore")) {
                gen.cp("gitignore", ".gitignore");
            }

            gen.mkdir("tasks");
            gen.cp("tasks/shell_sample.sh", "tasks/shell_sample.sh");
            gen.setExecutable("tasks/shell_sample.sh");
            gen.cp("tasks/repeat_hello.sh", "tasks/repeat_hello.sh");
            gen.setExecutable("tasks/repeat_hello.sh");
            gen.cp("tasks/__init__.py", "tasks/__init__.py");
            gen.cpWithReplace("workflow.dig", workflowFileName,
                    ImmutableMap.of("@@name@@", workflowName));
            if (isCurrentDirectory) {
                out.println("Done. Type `digdag run " + workflowFileName + "` to run the workflow. Enjoy!");
            }
            else {
                out.println("Done. Type `cd " + destDir + "` and then `digdag run " + workflowFileName + "` to run the workflow. Enjoy!");
            }
        }
    }

    //private static File getFatJarPath()
    //    throws URISyntaxException
    //{
    //    URI uri = Main.class.getProtectionDomain().getCodeSource().getLocation().toURI();
    //    if (!"file".equals(uri.getScheme())) {
    //        return null;
    //    }
    //    return new File(uri.getPath());
    //}

    private class ResourceGenerator
    {
        private final String sourcePrefix;
        private final Path destDir;

        private ResourceGenerator(String sourcePrefix, Path destDir)
        {
            this.sourcePrefix = sourcePrefix;
            this.destDir = destDir;
        }

        private void cpAbsoluteDest(String src, Path dest)
            throws IOException
        {
            out.println("  Creating " + dest);
            try (InputStream in = getClass().getResourceAsStream(sourcePrefix + src)) {
                if (in == null) {
                    throw new RuntimeException("Resource not exists: " + sourcePrefix + src);
                }
                try (OutputStream out = Files.newOutputStream(dest)) {
                    ByteStreams.copy(in, out);
                }
            }
        }

        public void cpAbsoluteSource(Path file, String name)
            throws IOException
        {
            Path dest = path(name);
            out.println("  Creating " + dest);
            try (InputStream in = Files.newInputStream(file)) {
                try (OutputStream out = Files.newOutputStream(dest)) {
                    ByteStreams.copy(in, out);
                }
            }
        }

        private void cp(String src, String name)
            throws IOException
        {
            cpAbsoluteDest(src, path(name));
        }

        private void cpWithReplace(String src, String name, Map<String, String> replacements)
            throws IOException
        {
            String data = getResource(src);
            for (Map.Entry<String, String> pair : replacements.entrySet()) {
                data = data.replaceAll(pair.getKey(), pair.getValue());
            }
            Path dest = path(name);
            out.println("  Creating " + dest);
            try (OutputStream out = Files.newOutputStream(dest)) {
                out.write(data.getBytes(UTF_8));
            }
        }

        private void setExecutable(String name)
        {
            boolean success = path(name).toFile().setExecutable(true);
            if (!success) {
                // ignore
            }
        }

        private boolean exists(String name)
            throws IOException
        {
            return Files.exists(path(name));
        }

        private void mkdir(String name)
            throws IOException
        {
            Files.createDirectories(path(name));
        }

        private Path path(String name)
        {
            Path path = destDir;
            for (String f : name.split("/")) {
                path = path.resolve(f);
            }
            return path;
        }

        private String getResource(String src)
            throws IOException
        {
            try (InputStream in = getClass().getResourceAsStream(sourcePrefix + src)) {
                if (in == null) {
                    throw new RuntimeException("Resource not exists: " + sourcePrefix + src);
                }
                return new String(ByteStreams.toByteArray(in), UTF_8);
            }
        }
    }
}
