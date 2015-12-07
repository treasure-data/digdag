package io.digdag.cli;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Date;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.WorkflowSourceList;
import io.digdag.core.repository.WorkflowSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import io.digdag.cli.Main.SystemExitException;
import io.digdag.core.spi.config.ConfigFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Archive
{
    private static Logger logger = LoggerFactory.getLogger(Archive.class);

    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        parser.acceptsAll(asList("o", "output")).withRequiredArg().ofType(String.class);
        // TODO support -p option? for jinja template rendering

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.isEmpty()) {
            throw usage(null);
        }

        List<File> workflowFiles = op.nonOptionArguments()
            .stream()
            .map(arg -> new File(arg.toString()))
            .collect(Collectors.toList());
        File outputPath = new File(Optional.fromNullable((String) op.valueOf("o")).or("archive.tar.gz"));

        new Archive().archive(outputPath, workflowFiles);
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag archive <workflow.yml...> [options...]");
        System.err.println("  Options:");
        System.err.println("    -o, --output ARCHIVE.tar.gz      output path");
        //System.err.println("    -C           DIR                  change directory before reading files");
        Main.showCommonOptions();
        System.err.println("");
        System.err.println("  Stdin:");
        System.err.println("    Names of the files to add the archive.");
        System.err.println("");
        System.err.println("  Examples:");
        System.err.println("    $ find . | digdag archive myflow.yml");
        System.err.println("    $ git ls-files | digdag archive -o archive.tar.gz workflows/*.yml");
        System.err.println("");
        return systemExit(error);
    }

    public void archive(File outputPath, List<File> workflowFiles)
            throws IOException
    {
        Injector injector = Main.embed().getInjector();

        System.out.println("Creating "+outputPath+"...");

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);
        final FileMapper mapper = injector.getInstance(FileMapper.class);

        List<WorkflowSource> workflows = new ArrayList<>();
        for (File workflowFile : workflowFiles) {
            // TODO validate workflow
            workflows.addAll(
                loader.load(workflowFile, cf.create()).convert(WorkflowSourceList.class).get());
        }

        List<String> stdinLines;
        if (System.console() != null) {
            stdinLines = ImmutableList.of();
        }
        else {
            stdinLines = CharStreams.readLines(new BufferedReader(new InputStreamReader(System.in)));
        }

        Set<File> missingWorkflowFiles = new HashSet<>(workflowFiles);

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(outputPath))))) {
            for (String line : stdinLines) {
                File file = new File(line);

                if (file.isDirectory()) {
                    continue;
                }
                System.out.println("   Archiving "+file);

                tar.putArchiveEntry(new TarArchiveEntry(file));
                if (file.isFile()) {
                    try (FileInputStream in = new FileInputStream(file)) {
                        ByteStreams.copy(in, tar);
                    }
                    tar.closeArchiveEntry();
                }
                missingWorkflowFiles.remove(file);
            }

            for (File file : missingWorkflowFiles) {
                System.out.println("  Archiving "+file);
                tar.putArchiveEntry(new TarArchiveEntry(file));
                try (FileInputStream in = new FileInputStream(file)) {
                    ByteStreams.copy(in, tar);
                }
                tar.closeArchiveEntry();
            }

            // create .digdag.yml
            byte[] meta = mapper.toYaml(ArchiveMetadata.of(WorkflowSourceList.of(workflows))).getBytes(StandardCharsets.UTF_8);
            TarArchiveEntry metaEntry = new TarArchiveEntry(ArchiveMetadata.FILE_NAME);
            metaEntry.setSize(meta.length);
            metaEntry.setModTime(new Date());
            tar.putArchiveEntry(metaEntry);
            tar.write(meta);
            tar.closeArchiveEntry();
        }

        System.out.println("  Workflows:");
        for (WorkflowSource workflow : workflows) {
            System.out.println("    "+workflow.getName());
        }
    }
}
