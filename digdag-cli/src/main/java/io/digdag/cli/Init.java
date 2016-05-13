package io.digdag.cli;

import java.io.PrintStream;
import java.util.Map;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import static java.nio.charset.StandardCharsets.UTF_8;
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

    private void init(String path)
        throws Exception
    {
        File dir = new File(path);
        String workflowName = dir.getName();

        ResourceGenerator gen = new ResourceGenerator("/digdag/cli/", dir);

        gen.mkdir(".");  // creates dir itself

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

        if (gen.exists(Run.DEFAULT_DAGFILE)) {
            out.println("File " + gen.file(Run.DEFAULT_DAGFILE) + " already exists.");
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
            gen.cpWithReplace("workflow.dig", workflowName + ".dig",
                    ImmutableMap.of("@@name@@", workflowName));
            gen.cpWithReplace("digdag.dig", Run.DEFAULT_DAGFILE,
                    ImmutableMap.of("@@name@@", workflowName));
            if (path.equals(".")) {
                out.println("Done. Type `digdag r` to run the workflow. Enjoy!");
            }
            else {
                out.println("Done. Type `cd " + path + "` and then `digdag r` to run the workflow. Enjoy!");
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
        private final File destDir;

        private ResourceGenerator(String sourcePrefix, File destDir)
        {
            this.sourcePrefix = sourcePrefix;
            this.destDir = destDir;
        }

        private void cpAbsoluteDest(String src, File dest)
            throws IOException
        {
            out.println("  Creating " + dest);
            try (InputStream in = getClass().getResourceAsStream(sourcePrefix + src)) {
                if (in == null) {
                    throw new RuntimeException("Resource not exists: " + sourcePrefix + src);
                }
                try (FileOutputStream out = new FileOutputStream(dest)) {
                    ByteStreams.copy(in, out);
                }
            }
        }

        public void cpAbsoluteSource(File file, String name)
            throws IOException
        {
            File dest = file(name);
            out.println("  Creating " + dest);
            try (InputStream in = new FileInputStream(file)) {
                try (FileOutputStream out = new FileOutputStream(dest)) {
                    ByteStreams.copy(in, out);
                }
            }
        }

        private void cp(String src, String name)
            throws IOException
        {
            cpAbsoluteDest(src, file(name));
        }

        private void cpWithReplace(String src, String name, Map<String, String> replacements)
            throws IOException
        {
            String data = getResource(src);
            for (Map.Entry<String, String> pair : replacements.entrySet()) {
                data = data.replaceAll(pair.getKey(), pair.getValue());
            }
            File dest = file(name);
            out.println("  Creating " + dest);
            try (FileOutputStream out = new FileOutputStream(dest)) {
                out.write(data.getBytes(UTF_8));
            }
        }

        private void setExecutable(String name)
        {
            boolean success = file(name).setExecutable(true);
            if (!success) {
                // ignore
            }
        }

        private boolean exists(String name)
            throws IOException
        {
            return file(name).exists();
        }

        private void mkdir(String name)
        {
            boolean success = file(name).mkdirs();
            if (!success) {
                // ignore
            }
        }

        private File file(String name)
        {
            File path = destDir;
            for (String f : name.split("/")) {
                path = new File(path, f);
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
