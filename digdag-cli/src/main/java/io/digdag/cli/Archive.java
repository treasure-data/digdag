package io.digdag.cli;

import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.charset.StandardCharsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import static io.digdag.cli.Main.systemExit;

public class Archive
    extends Command
{
    @Parameter(names = {"-f", "--file"})
    String dagfilePath = Run.DEFAULT_DAGFILE;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-o", "--output"})
    String output = "digdag.archive.tar.gz";

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 0) {
            throw usage(null);
        }
        archive();
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag archive [-f workflow.yml...] [options...]");
        System.err.println("  Options:");
        System.err.println("    -f, --file PATH                  use this file to load tasks (default: digdag.yml)");
        System.err.println("    -o, --output ARCHIVE.tar.gz      output path (default: digdag.archive.tar.gz)");
        //System.err.println("    -C           DIR                  change directory before reading files");
        Main.showCommonOptions();
        System.err.println("  Stdin:");
        System.err.println("    Names of the files to add the archive.");
        System.err.println("");
        System.err.println("  Examples:");
        System.err.println("    $ git ls-files | digdag archive");
        System.err.println("    $ find . | digdag archive -o digdag.archive.tar.gz");
        return systemExit(error);
    }

    private void archive()
            throws IOException
    {
        System.out.println("Creating "+output+"...");

        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(YamlMapper.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final YamlMapper yamlMapper = injector.getInstance(YamlMapper.class);

        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.setAll(loader.loadParameterizedFile(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        Path absoluteCurrentPath = FileSystems.getDefault().getPath("").toAbsolutePath().normalize();

        // normalize the path
        String dagfileName = normalizeRelativePath(absoluteCurrentPath, dagfilePath);
        boolean dagfileLoaded = false;

        Dagfile dagfile = loader.loadParameterizedFile(new File(dagfilePath), overwriteParams).convert(Dagfile.class);
        ArchiveMetadata meta = ArchiveMetadata.of(dagfile, dagfileName);

        List<String> stdinLines;
        if (System.console() != null) {
            stdinLines = ImmutableList.of();
        }
        else {
            stdinLines = CharStreams.readLines(new BufferedReader(new InputStreamReader(System.in)));
        }

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(new File(output)))))) {
            for (String line : stdinLines) {
                File file = new File(line);
                if (file.isDirectory()) {
                    continue;
                }

                String name = normalizeRelativePath(absoluteCurrentPath, line);
                System.out.println("  Archiving "+name);

                tar.putArchiveEntry(new TarArchiveEntry(new File(name)));
                if (file.isFile()) {
                    try (FileInputStream in = new FileInputStream(file)) {
                        ByteStreams.copy(in, tar);
                    }
                    tar.closeArchiveEntry();
                }

                if (name.equals(dagfileName)) {
                    dagfileLoaded = true;
                }
            }

            if (!dagfileLoaded) {
                System.out.println("  Archiving "+dagfileName);
                tar.putArchiveEntry(new TarArchiveEntry(new File(dagfileName)));
                try (FileInputStream in = new FileInputStream(new File(dagfilePath))) {
                    ByteStreams.copy(in, tar);
                }
                tar.closeArchiveEntry();
            }

            // create .digdag.yml
            // TODO set default time zone if not set?
            byte[] metaBody = yamlMapper.toYaml(meta).getBytes(StandardCharsets.UTF_8);
            TarArchiveEntry metaEntry = new TarArchiveEntry(ArchiveMetadata.FILE_NAME);
            metaEntry.setSize(metaBody.length);
            metaEntry.setModTime(new Date());
            tar.putArchiveEntry(metaEntry);
            tar.write(metaBody);
            tar.closeArchiveEntry();
        }

        System.out.println("Workflows:");
        for (WorkflowDefinition workflow : meta.getWorkflowList().get()) {
            System.out.println("  "+workflow.getName());
        }

        System.out.println("");
        System.out.println("Created "+output+".");
        System.out.println("Use `digdag upload <path.tar.gz> <repository> <revision>` to upload it a server.");
        System.out.println("");
        System.out.println("  Examples:");
        System.out.println("    $ digdag upload "+output+" $(basename $(pwd)) $(date +%Y%m%d-%H%M%S)");
        System.out.println("    $ digdag upload "+output+" $(git rev-parse --abbrev-ref HEAD) $(git rev-parse HEAD)");
        System.out.println("");
    }

    private String normalizeRelativePath(Path absoluteCurrentPath, String rawPath)
    {
        Path absPath = FileSystems.getDefault().getPath(rawPath).toAbsolutePath().normalize();
        Path relPath = absoluteCurrentPath.relativize(absPath).normalize();

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Path fragment : relPath) {
            String name = fragment.toString();
            if (name.contains("/")) {
                throw new IllegalArgumentException("File name can't include '/': " + rawPath);
            }
            else if (name.equals("..") || name.equals(".")) {
                throw new IllegalArgumentException("Relative file path from current working directory can't include . or ..: " + relPath);
            }
            if (first) {
                first = false;
            }
            else {
                sb.append("/");
            }
            sb.append(name);
        }
        return sb.toString();
    }
}
