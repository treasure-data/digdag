package io.digdag.core.agent;

import com.google.common.base.Optional;
import io.digdag.core.TempFileManager;
import io.digdag.spi.StorageObject;
import io.digdag.spi.TaskRequest;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExtractArchiveWorkspaceManagerTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private ExtractArchiveWorkspaceManager extractArchiveWorkspaceManager;
    private byte[] archived;
    private TaskRequest taskRequest;
    private StorageObject storageObject;
    private StorageObject wrongStorageObject;

    @Before
    public void setUp()
            throws IOException
    {
        extractArchiveWorkspaceManager = new ExtractArchiveWorkspaceManager(new TempFileManager(temporaryFolder.getRoot().toPath()));

        File digFile = temporaryFolder.newFile();

        String wf = "+task:\n" +
                "  echo>: hello\n" +
                "";

        Files.write(digFile.toPath(), wf.getBytes(UTF_8), StandardOpenOption.CREATE);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(gzipOutputStream);
        tarArchiveOutputStream.createArchiveEntry(digFile, "mydig.dig");
        tarArchiveOutputStream.close();

        archived = outputStream.toByteArray();

        taskRequest = mock(TaskRequest.class);
        when(taskRequest.getTaskName()).thenReturn("zzz");

        storageObject = mock(StorageObject.class);
        when(storageObject.getContentLength()).thenReturn((long) archived.length);
        when(storageObject.getContentInputStream()).thenReturn(new ByteArrayInputStream(archived));

        byte[] wrongArchive = "This isn't a proper archive content".getBytes(UTF_8);
        wrongStorageObject = mock(StorageObject.class);
        when(wrongStorageObject.getContentLength()).thenReturn((long) wrongArchive.length);
        when(wrongStorageObject.getContentInputStream()).thenReturn(new ByteArrayInputStream(wrongArchive));
    }

    @Test
    public void withExtractedArchive()
            throws Exception
    {
        WorkspaceManager.ArchiveProvider archiveProvider = mock(WorkspaceManager.ArchiveProvider.class);

        when(archiveProvider.open()).thenReturn(Optional.of(storageObject));

        AtomicInteger funcCounter = new AtomicInteger();
        extractArchiveWorkspaceManager.withExtractedArchive(taskRequest, archiveProvider, (path) -> funcCounter.incrementAndGet());
        verify(archiveProvider, times(1)).open();
        assertThat(funcCounter.get(), is(1));

        assertEmptyWorkspace();
    }

    @Test
    public void withExtractedArchiveWithRetry()
            throws Exception
    {
        WorkspaceManager.ArchiveProvider archiveProvider = mock(WorkspaceManager.ArchiveProvider.class);

        when(archiveProvider.open()).thenReturn(Optional.of(wrongStorageObject),
                Optional.of(wrongStorageObject),
                Optional.of(storageObject));

        AtomicInteger funcCounter = new AtomicInteger();
        extractArchiveWorkspaceManager.withExtractedArchive(taskRequest, archiveProvider, (path) -> funcCounter.incrementAndGet());
        verify(archiveProvider, times(3)).open();
        assertThat(funcCounter.get(), is(1));

        assertEmptyWorkspace();
    }

    @Test
    public void withoutExtractedArchive()
            throws Exception
    {
        WorkspaceManager.ArchiveProvider archiveProvider = mock(WorkspaceManager.ArchiveProvider.class);

        when(archiveProvider.open()).thenReturn(Optional.absent());

        AtomicInteger funcCounter = new AtomicInteger();
        extractArchiveWorkspaceManager.withExtractedArchive(taskRequest, archiveProvider, (path) -> funcCounter.incrementAndGet());
        verify(archiveProvider, times(1)).open();
        assertThat(funcCounter.get(), is(1));

        assertEmptyWorkspace();
    }

    private void assertEmptyWorkspace()
    {
        assertThat(temporaryFolder.getRoot().toPath().resolve("workspace").toFile().listFiles().length, is(0));
    }
}