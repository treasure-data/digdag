package io.digdag.cli.client;

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
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.Files;
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
import io.digdag.core.archive.Dagfile;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.cli.Command;
import io.digdag.cli.Run;
import io.digdag.cli.Main;
import io.digdag.cli.YamlMapper;
import io.digdag.cli.SystemExitException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import static io.digdag.cli.Main.systemExit;
import static io.digdag.cli.Arguments.loadParams;

public class Archive
    extends Command
{
    @Parameter(names = {"-f", "--file"})
    List<String> dagfilePaths = ImmutableList.of(Run.DEFAULT_DAGFILE);

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
        Main.showCommonOptions();
        System.err.println("  Stdin:");
        System.err.println("    Names of the files to add the archive.");
        System.err.println("");
        System.err.println("  Examples:");
        System.err.println("    $ git ls-files | digdag archive");
        System.err.println("    $ find . | digdag archive -o digdag.archive.tar.gz");
        System.err.println("");
        return systemExit(error);
    }

    // used by Push.push
    static void archive(List<String> dagfilePaths, Map<String, String> params, String paramsFile, String output)
        throws IOException
    {
        Archive cmd = new Archive();
        cmd.dagfilePaths = dagfilePaths;
        cmd.params = params;
        cmd.paramsFile = paramsFile;
        cmd.output = output;
        cmd.runArchive();
    }

    private void archive()
            throws IOException
    {
        runArchive();

        System.out.println("Created "+output+".");
        System.out.println("Use `digdag upload <path.tar.gz> <project> <revision>` to upload it a server.");
        System.out.println("");
        System.out.println("  Examples:");
        System.out.println("    $ digdag upload "+output+" $(basename $(pwd)) $(date +%Y%m%d-%H%M%S)");
        System.out.println("    $ digdag upload "+output+" $(git rev-parse --abbrev-ref HEAD) $(git rev-parse HEAD)");
        System.out.println("");
    }

    private void runArchive()
            throws IOException
    {
        System.out.println("Creating "+output+"...");

        Injector injector = new DigdagEmbed.Bootstrap()
            .withWorkflowExecutor(false)
            .withScheduleExecutor(false)
            .withLocalAgent(false)
            .addModules(binder -> {
                binder.bind(YamlMapper.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final ProjectArchiveLoader projectLoader = injector.getInstance(ProjectArchiveLoader.class);
        final YamlMapper yamlMapper = injector.getInstance(YamlMapper.class);

        Config overwriteParams = loadParams(cf, loader, paramsFile, params);

        Path absoluteCurrentPath = Paths.get("").toAbsolutePath().normalize();

        ProjectArchive project = projectLoader.load(
                dagfilePaths.stream().map(str -> Paths.get(str)).collect(Collectors.toList()),
                overwriteParams);

        project.listFiles(absoluteCurrentPath, relPath -> {
            try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(new File(output)))))) {
                Path path = absoluteCurrentPath.resolve(relPath);
                if (!Files.isDirectory(path)) {
                    String name = relPath.toString();
                    System.out.println("  Archiving "+name);

                    TarArchiveEntry e = buildTarArchiveEntry(absoluteCurrentPath, path, name);
                    tar.putArchiveEntry(e);
                    if (!e.isSymbolicLink()) {
                        try (InputStream in = Files.newInputStream(path)) {
                            ByteStreams.copy(in, tar);
                        }
                        tar.closeArchiveEntry();
                    }
                }

                // create .digdag.yml
                // TODO set default time zone if not set?
                byte[] metaBody = yamlMapper.toYaml(project.getMetadata()).getBytes(StandardCharsets.UTF_8);
                TarArchiveEntry metaEntry = new TarArchiveEntry(ArchiveMetadata.FILE_NAME);
                metaEntry.setSize(metaBody.length);
                metaEntry.setModTime(new Date());
                tar.putArchiveEntry(metaEntry);
                tar.write(metaBody);
                tar.closeArchiveEntry();
            }
        });

        System.out.println("Workflows:");
        for (WorkflowDefinition workflow : project.getMetadata().getWorkflowList().get()) {
            System.out.println("  "+workflow.getName());
        }
        System.out.println("");
    }

    private TarArchiveEntry buildTarArchiveEntry(Path absoluteCurrentPath, Path path, String name)
        throws IOException
    {
        TarArchiveEntry e;
        if (Files.isSymbolicLink(path)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            Path dest = Files.readSymbolicLink(path);
            if (!dest.isAbsolute()) {
                dest = path.getParent().resolve(dest);
            }
            String fromCurrentPath = normalizeRelativePath(absoluteCurrentPath, dest.toString());
            Path relativeFromPath = path.getParent().toAbsolutePath().relativize(absoluteCurrentPath.resolve(fromCurrentPath).normalize());
            e.setLinkName(relativeFromPath.toString());
        }
        else {
            e = new TarArchiveEntry(path.toFile(), name);
            try {
                int mode = 0;
                for (PosixFilePermission perm : Files.getPosixFilePermissions(path)) {
                    switch (perm) {
                    case OWNER_READ:
                        mode |= 0400;
                        break;
                    case OWNER_WRITE:
                        mode |= 0200;
                        break;
                    case OWNER_EXECUTE:
                        mode |= 0100;
                        break;
                    case GROUP_READ:
                        mode |= 0040;
                        break;
                    case GROUP_WRITE:
                        mode |= 0020;
                        break;
                    case GROUP_EXECUTE:
                        mode |= 0010;
                        break;
                    case OTHERS_READ:
                        mode |= 0004;
                        break;
                    case OTHERS_WRITE:
                        mode |= 0002;
                        break;
                    case OTHERS_EXECUTE:
                        mode |= 0001;
                        break;
                    default:
                        // ignore
                    }
                }
                e.setMode(mode);
            }
            catch (UnsupportedOperationException ex) {
                // ignore custom mode
            }
        }
        return e;
    }

    private String normalizeRelativePath(Path absoluteCurrentPath, String rawPath)
    {
        Path absPath = Paths.get(rawPath).toAbsolutePath().normalize();
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
