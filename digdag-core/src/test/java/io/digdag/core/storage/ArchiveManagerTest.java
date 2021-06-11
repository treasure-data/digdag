package io.digdag.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ArchiveType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class ArchiveManagerTest
{
    private ArchiveManager archiveManager;

    @Before
    public void setUp()
            throws IOException
    {
        ObjectMapper objectMapper = DigdagClient.objectMapper();
        StorageManager storageManager = mock(StorageManager.class);
        Config config = Config.deserializeFromJackson(objectMapper,
                objectMapper.readTree(
                        "{\"archive.type\":\"s3\"," +
                                "\"archive.s3.path\":\"projects\"," +
                                "\"archive.s3.bucket\":\"digdag-bucket\"," +
                                "\"archive.s3.credentials.access-key-id\":\"my-access-key-id\"," +
                                "\"archive.s3.credentials.secret-access-key\":\"my-secret-acccess-key\"}"));
        archiveManager = new ArchiveManager(storageManager, config);
    }

    @Test
    public void newArchiveLocation()
            throws IOException
    {
        String rev = UUID.randomUUID().toString();
        ArchiveManager.Location location = archiveManager.newArchiveLocation(42, "my-proj", rev, 123456);
        assertThat(location.getArchiveType(), is(ArchiveType.of("s3")));
        String encodeProjectName = new String(Base64.getEncoder().encode("my-proj".getBytes(UTF_8)), UTF_8).replace("=", "");
        assertThat(location.getPath(), is(startsWith(String.format("projects/42/%s/%s.", encodeProjectName, rev))));
        assertThat(location.getPath(), is(endsWith(".tar.gz")));
    }
}