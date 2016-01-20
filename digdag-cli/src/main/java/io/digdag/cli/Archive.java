package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
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
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.ScheduleSource;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
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
    String output = "archive.tar.gz";

    // TODO support -p option? for jinja template rendering

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
        System.err.println("    -o, --output ARCHIVE.tar.gz      output path");
        //System.err.println("    -C           DIR                  change directory before reading files");
        Main.showCommonOptions();
        System.err.println("  Stdin:");
        System.err.println("    Names of the files to add the archive.");
        System.err.println("");
        System.err.println("  Examples:");
        System.err.println("    $ find . | digdag archive");
        System.err.println("    $ git ls-files | digdag archive -o archive.tar.gz workflows/*.yml");
        return systemExit(error);
    }

    private void archive()
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

        Config overwriteParams = cf.create();
        if (paramsFile != null) {
            overwriteParams.setAll(loader.load(new File(paramsFile), cf.create()));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            overwriteParams.set(pair.getKey(), pair.getValue());
        }

        Dagfile dagfile = loader.load(new File(dagfilePath), overwriteParams).convert(Dagfile.class);
        ArchiveMetadata meta = ArchiveMetadata.of(
            dagfile.getWorkflowList(),
            dagfile.getScheduleList(),
            dagfile.getDefaultParams().setAll(overwriteParams));

        List<String> stdinLines;
        if (System.console() != null) {
            stdinLines = ImmutableList.of();
        }
        else {
            stdinLines = CharStreams.readLines(new BufferedReader(new InputStreamReader(System.in)));
        }

        Set<File> requiredFiles = new HashSet<>();
        requiredFiles.add(new File(dagfilePath));

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
                requiredFiles.remove(file);
            }

            for (File file : requiredFiles) {
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
            byte[] metaBody = mapper.toYaml(meta).getBytes(StandardCharsets.UTF_8);
            TarArchiveEntry metaEntry = new TarArchiveEntry(ArchiveMetadata.FILE_NAME);
            metaEntry.setSize(metaBody.length);
            metaEntry.setModTime(new Date());
            tar.putArchiveEntry(metaEntry);
            tar.write(metaBody);
            tar.closeArchiveEntry();
        }

        System.out.println("  Workflows:");
        for (WorkflowSource workflow : meta.getWorkflowList().get()) {
            System.out.println("    "+workflow.getName());
        }
        System.out.println("  Schedules:");
        for (ScheduleSource schedule : meta.getScheduleList().get()) {
            System.out.println("    "+schedule.getName());
        }
    }
}
