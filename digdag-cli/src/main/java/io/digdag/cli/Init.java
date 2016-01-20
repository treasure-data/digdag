package io.digdag.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import com.google.common.io.ByteStreams;
import static io.digdag.cli.Main.systemExit;

public class Init
    extends Command
{
    @Override
    public void main()
        throws Exception
    {
        String dagfilePath;
        switch (args.size()) {
        case 0:
            dagfilePath = Run.DEFAULT_DAGFILE;
            break;
        case 1:
            dagfilePath = args.get(0);
            break;
        default:
            throw usage(null);
        }
        init(dagfilePath);
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag init [PATH.yml]");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void init(String dagfilePath)
        throws Exception
    {
        File file = new File(dagfilePath);
        File dir = file.getParentFile();

        ResourceGenerator gen = new ResourceGenerator("/digdag/cli/", dir);

        if (!gen.exists("digdag")) {
            gen.cp("digdag.sh", "digdag");
            gen.setExecutable("digdag");
        }
        gen.mkdir(".digdag-wrapper");
        File f = getFatJarPath();
        if (f != null) {
            gen.cpAbsoluteSource(f, ".digdag-wrapper/digdag.jar");
            gen.setExecutable(".digdag-wrapper/digdag.jar");
        }

        if (file.exists()) {
            System.out.println("File "+file+" already exists. Canceled");
        }
        else {
            gen.cpAbsoluteDest("digdag.yml", file);
            System.out.println("Done. Use `./digdag r` to run this workflow.");
        }
    }

    private static File getFatJarPath()
        throws URISyntaxException
    {
        URI uri = Main.class.getProtectionDomain().getCodeSource().getLocation().toURI();
        if (!"file".equals(uri.getScheme())) {
            return null;
        }
        return new File(uri.getPath());
    }

    private static class ResourceGenerator
    {
        private final String sourcePrefix;
        private final File destDir;

        public ResourceGenerator(String sourcePrefix, File destDir)
        {
            this.sourcePrefix = sourcePrefix;
            this.destDir = destDir;
        }

        public void cpAbsoluteDest(String src, File dest)
            throws IOException
        {
            System.out.println("  Creating " + dest);
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
            System.out.println("  Creating " + dest);
            try (InputStream in = new FileInputStream(file)) {
                try (FileOutputStream out = new FileOutputStream(dest)) {
                    ByteStreams.copy(in, out);
                }
            }
        }

        public void cp(String src, String name)
            throws IOException
        {
            cpAbsoluteDest(src, file(name));
        }

        public void setExecutable(String name)
        {
            boolean success = file(name).setExecutable(true);
            if (!success) {
                // ignore
            }
        }

        public boolean exists(String name)
            throws IOException
        {
            return file(name).exists();
        }

        public void mkdir(String name)
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
    }
}
