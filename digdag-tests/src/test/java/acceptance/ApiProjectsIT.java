package acceptance;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestWorkflowDefinition;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;
import static utils.TestUtils.copyResource;

public class ApiProjectsIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    private Path projectDir;

    private DigdagClient digdagClient;

    private DBI dbi;

    @Before()
    public void setUp()
            throws Exception
    {
        server =  TemporaryDigdagServer.of();
        server.start();

        digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
        digdagClient.getVersion();

        projectDir = folder.getRoot().toPath().resolve("foobar");
        Files.createDirectory(projectDir);
    }

    @After
    public void tearDown()
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void putProjectsWithClearSchedule()
            throws Exception {
        assumeTrue(server.isRemoteDatabase());

        dbi = new DBI(server.getTestDBDataSource());

        // Prepare project archive
        copyResource("acceptance/api_projects/schedule1.dig", projectDir.resolve("wf1.dig"));
        copyResource("acceptance/api_projects/schedule2.dig", projectDir.resolve("wf2.dig"));
        Path archivePath = Files.createTempFile(folder.getRoot().toPath(), "archive-", ".tar.gz");
        archivePath.toFile().deleteOnExit();
        archiveProject(archivePath, projectDir);


        // Push the project and get schedule of wf1
        digdagClient.putProjectRevision("prj1", "rev1", archivePath.toFile(), Optional.absent());
        RestProject prj1 = digdagClient.getProject("prj1");
        RestWorkflowDefinition wf1 = digdagClient.getWorkflowDefinition(prj1.getId(), "wf1");
        RestSchedule wf1sch1 = digdagClient.getSchedule(prj1.getId(), "wf1");

        Instant tomorrowMidnightUTC = Instant.now().truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
        Instant nextOfTomorrowMidnightUTC = tomorrowMidnightUTC.plus(1, ChronoUnit.DAYS);

        // initial schedule is tomorrowMidnightUTC for "daily>: 00:00:00"
        {
            assertThat(wf1sch1.getNextRunTime(), is(tomorrowMidnightUTC));
        }

        // update last_session_time and next run time will be calculated from it.
        {
            updateLastSessionTime(wf1sch1.getId().asInt(), tomorrowMidnightUTC);
            digdagClient.putProjectRevision("prj1", "rev2", archivePath.toFile(), Optional.absent());
            RestSchedule wf1sch2 = digdagClient.getSchedule(prj1.getId(), "wf1");
            assertThat(wf1sch2.getNextRunTime(), is(nextOfTomorrowMidnightUTC));
        }

        // clearSchedule option for "wf1". So next run time is calculated as initial schedule
        {
            updateLastSessionTime(wf1sch1.getId().asInt(), tomorrowMidnightUTC);
            digdagClient.putProjectRevision("prj1", "rev3", archivePath.toFile(), Optional.absent(), Arrays.asList("wf1"), Optional.absent());
            RestSchedule wf1sch2 = digdagClient.getSchedule(prj1.getId(), "wf1");
            assertThat(wf1sch2.getNextRunTime(), is(tomorrowMidnightUTC));
        }

        // clearSchedule for "wf2", no impact to wf1
        {
            updateLastSessionTime(wf1sch1.getId().asInt(), tomorrowMidnightUTC);
            digdagClient.putProjectRevision("prj1", "rev4", archivePath.toFile(), Optional.absent(), Arrays.asList("wf2"), Optional.absent());
            RestSchedule wf1sch2 = digdagClient.getSchedule(prj1.getId(), "wf1");
            assertThat(wf1sch2.getNextRunTime(), is(nextOfTomorrowMidnightUTC));
        }

        // set clearAllSchedule. next run time is calculated as initial schedule
        {
            updateLastSessionTime(wf1sch1.getId().asInt(), tomorrowMidnightUTC);
            digdagClient.putProjectRevision("prj1", "rev5", archivePath.toFile(), Optional.absent(), Arrays.asList(), Optional.of(true));
            RestSchedule wf1sch2 = digdagClient.getSchedule(prj1.getId(), "wf1");
            assertThat(wf1sch2.getNextRunTime(), is(tomorrowMidnightUTC));
        }
    }

    private void updateLastSessionTime(int id, Instant lastSessionTime)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("update schedules set last_session_time = :last_session_time, updated_at = now() where id = :id")
                    .bind("id", id)
                    .bind("last_session_time", lastSessionTime.getEpochSecond())
                    .execute();
        }
    }

    private List<Path> listFiles(Path dir)
            throws IOException
    {
        try (Stream<Path> stream = Files.walk(dir, 10)) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .collect(Collectors.toList());
        }
    }

    private void archiveProject(Path archivePath, Path projectPath)
            throws IOException
    {
        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(archivePath)))) {
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            listFiles(projectPath).stream().forEach((path) -> {
                try {
                    if (Files.isReadable(path.toAbsolutePath())) {
                        TarArchiveEntry e  = new TarArchiveEntry(path.toFile(), path.toFile().getName());
                        tar.putArchiveEntry(e);
                        try (InputStream in = Files.newInputStream(path)) {
                            ByteStreams.copy(in, tar);
                        }
                        tar.closeArchiveEntry();
                    }
                }
                catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }
            });
        }
    }
}
