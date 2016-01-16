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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.WorkflowSourceList;
import io.digdag.core.repository.ScheduleSource;
import io.digdag.core.repository.ScheduleSourceList;
import com.beust.jcommander.Parameter;
import io.digdag.spi.config.ConfigFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import static io.digdag.cli.Main.systemExit;

public class Archive
    extends Command
{
    @Parameter(names = {"-o", "--output"})
    String output = "archive.tar.gz";

    // TODO support -p option? for jinja template rendering

    @Override
    public void main()
            throws Exception
    {
        if (args.isEmpty()) {
            throw usage(null);
        }

        archive(args);
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag archive <workflow.yml...> [options...]");
        System.err.println("  Options:");
        System.err.println("    -o, --output ARCHIVE.tar.gz      output path");
        //System.err.println("    -C           DIR                  change directory before reading files");
        Main.showCommonOptions();
        System.err.println("  Stdin:");
        System.err.println("    Names of the files to add the archive.");
        System.err.println("");
        System.err.println("  Examples:");
        System.err.println("    $ find . | digdag archive myflow.yml");
        System.err.println("    $ git ls-files | digdag archive -o archive.tar.gz workflows/*.yml");
        return systemExit(error);
    }

    private void archive(List<String> workflowFiles)
            throws IOException
    {
        System.out.println("Creating "+output+"...");

        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(binder -> {
                binder.bind(FileMapper.class).in(Scopes.SINGLETON);
                binder.bind(ArgumentConfigLoader.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);
        final FileMapper mapper = injector.getInstance(FileMapper.class);

        List<WorkflowSource> workflows = new ArrayList<>();
        for (String workflowFile : workflowFiles) {
            // TODO validate workflow
            workflows.addAll(
                loader.load(new File(workflowFile), cf.create()).convert(WorkflowSourceList.class).get());
        }

        List<String> stdinLines;
        if (System.console() != null) {
            stdinLines = ImmutableList.of();
        }
        else {
            stdinLines = CharStreams.readLines(new BufferedReader(new InputStreamReader(System.in)));
        }

        Set<File> missingWorkflowFiles = new HashSet<>(
                workflowFiles.stream().map(arg -> new File(arg)).collect(Collectors.toList())
                );

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(new File(output)))))) {
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
            // TODO make ScheduleSourceList from Dagfile
            // TODO set default time zone if not set
            byte[] meta = mapper.toYaml(ArchiveMetadata.of(WorkflowSourceList.of(workflows), ScheduleSourceList.of(ImmutableList.of()))).getBytes(StandardCharsets.UTF_8);
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
