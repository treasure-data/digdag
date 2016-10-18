package io.digdag.cli;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.WorkflowDefinition;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Init
    extends Command
{
    private static final String EXAMPLES_RESOURCE_PREFIX = "/digdag/cli/init_examples/";

    private static final Map<String, ExampleProjectGenerator> TYPES_TABLE = ImmutableMap.<String, ExampleProjectGenerator>builder()
            .put("echo", (gen) -> {})
            .put("sh", (gen) -> {
                gen.mkdir("scripts");
                gen.cp("scripts/myscript.sh", "scripts/myscript.sh");
                gen.setExecutable("scripts/myscript.sh");
            })
            .put("ruby", (gen) -> {
                gen.mkdir("scripts");
                gen.cp("scripts/myclass.rb", "scripts/myclass.rb");
            })
            .put("python", (gen) -> {
                gen.mkdir("scripts");
                gen.cp("scripts/__init__.py", "scripts/__init__.py");
                gen.cp("scripts/myclass.py", "scripts/myclass.py");
            })
            .put("td", (gen) -> {
                gen.mkdir("queries");
                gen.cp("queries/query.sql", "queries/query.sql");
            })
            .put("postgresql", (gen) -> {
                gen.mkdir("queries");
                gen.cp("queries/create_src_table.sql", "queries/create_src_table.sql");
                gen.cp("queries/insert_data_to_src_table.sql", "queries/insert_data_to_src_table.sql");
                gen.cp("queries/summarize_src_table.sql", "queries/summarize_src_table.sql");
            })
            .build();

    @Parameter(names = {"-t", "--type"})
    String exampleType = "echo";

    @Override
    public void main()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        if (!TYPES_TABLE.containsKey(exampleType)) {
            throw usage("--type has an invalid value");
        }
        init(args.get(0));
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " init <dir> [options...]");
        err.println("  Options:");
        err.println("    -t, --type EXAMPLE_TYPE          example project type (echo / sh / ruby / python / td / postgresql. default: echo)");
        Main.showCommonOptions(env, err);
        err.println("  Example:");
        err.println("    $ " + programName + " init mydag");
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

        // validate workflow name
        WorkflowDefinition.of(workflowName, new ConfigFactory(objectMapper()).create(), ZoneId.of("UTC"));

        String workflowFileName = workflowName + WORKFLOW_FILE_SUFFIX;

        String sourcePrefix = EXAMPLES_RESOURCE_PREFIX + exampleType + "/";
        ResourceGenerator gen = new ResourceGenerator(sourcePrefix, destDir);

        gen.mkdir(".");  // creates destDir itself

        if (gen.exists(workflowFileName)) {
            out.println("File " + gen.path(workflowFileName) + " already exists.");
        }
        else {
            TYPES_TABLE.get(exampleType).generate(gen);
            gen.cp("workflow.dig", workflowFileName);
            if (!gen.exists(".gitignore")) {
                gen.cpAbsoluteSource(EXAMPLES_RESOURCE_PREFIX + "gitignore", ".gitignore");
            }
            if (exampleType.equals("td")) {
                if (isCurrentDirectory) {
                    out.println("Done. Set up you Treasure Data API key and type `" + programName + " push " + workflowName + "` to push the workflow to Treasure Data. Enjoy!");
                }
                else {
                    out.println("Done. Set up you Treasure Data API key and type `cd " + destDir + "` and then `" + programName + " push " + workflowName + "` to push the workflow to Treasure Data. Enjoy!");
                }
            }
            else {
                if (isCurrentDirectory) {
                    out.println("Done. Type `" + programName + " run " + workflowFileName + "` to run the workflow. Enjoy!");
                }
                else {
                    out.println("Done. Type `cd " + destDir + "` and then `" + programName + " run " + workflowFileName + "` to run the workflow. Enjoy!");
                }
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
            cpAbsoluteSourceDest(sourcePrefix + src, dest);
        }

        private void cpAbsoluteSourceDest(String src, Path dest)
                throws IOException
        {
            out.println("  Creating " + dest);
            try (InputStream in = getClass().getResourceAsStream(src)) {
                if (in == null) {
                    throw new RuntimeException("Resource does not exist: " + src);
                }
                try (OutputStream out = Files.newOutputStream(dest)) {
                    ByteStreams.copy(in, out);
                }
            }
        }

        public void cpAbsoluteSource(String src, String name)
            throws IOException
        {
            cpAbsoluteSourceDest(src, path(name));
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

    private interface ExampleProjectGenerator
    {
        void generate(ResourceGenerator gen)
            throws IOException;
    }
}
