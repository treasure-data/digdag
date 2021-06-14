package io.digdag.storage.gcs;

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.io.ByteStreams;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObjectSummary;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.time.Instant;

import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.storage.StorageManager.encodeHex;
import static io.digdag.util.Md5CountInputStream.digestMd5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

public class GCSStorageTest
{
    private Storage storage;
    @Mock private GoogleCredentials googleCredentials;

    @Before
    public void setUp()
            throws Exception
    {
        com.google.cloud.storage.Storage gcsStorage = LocalStorageHelper.getOptions().getService();

        ConfigFactory cf = new ConfigFactory(objectMapper());
        String bucket = UUID.randomUUID().toString();
        Config config = cf.create()
                .set("bucket", bucket);  // use unique bucket name
        storage = new GCSStorageFactory().newStorage(gcsStorage, config);
    }

    @Test
    public void putReturnsMd5()
            throws Exception
    {
        String checksum1 = storage.put("key1", 10, contents("0123456789"));
        String checksum2 = storage.put("key2", 5, contents("01234"));
        // if use LocalStorageHelper, return value is "".
        assertThat(checksum1, either(is(md5hex("0123456789"))).or(is("")));
        assertThat(checksum2, either(is(md5hex("01234"))).or(is("")));
    }

    @Test
    public void putGet()
            throws Exception
    {
        storage.put("key/file/1", 3, contents("xxx"));
        storage.put("key/file/2", 1, contents("a"));
        storage.put("key/file/3", 4, contents("data"));
        assertThat(readString(storage.open("key/file/1").getContentInputStream()), is("xxx"));
        assertThat(readString(storage.open("key/file/2").getContentInputStream()), is("a"));
        assertThat(readString(storage.open("key/file/3").getContentInputStream()), is("data"));
    }

    @Test
    public void listAll()
            throws Exception
    {
        storage.put("key/file/1", 3, contents("xxx"));
        storage.put("key/file/2", 1, contents("1"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("key", (chunk) -> all.addAll(chunk));

        // if use LocalStorageHelper, returned elements order is randomize.
        assertThat(all.size(), is(2));
        assertThat(all, containsInAnyOrder(dummyStorageObjectSummary("key/file/1", 3), dummyStorageObjectSummary("key/file/2", 1)));
    }

    @Test
    public void listWithPrefix()
            throws Exception
    {
        storage.put("key1", 1, contents("0"));
        storage.put("test/file/1", 1, contents("1"));
        storage.put("test/file/2", 1, contents("1"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("test", (chunk) -> all.addAll(chunk));

        // if use LocalStorageHelper, returned elements order is randomize.
        assertThat(all.size(), is(2));
        assertThat(all, containsInAnyOrder(dummyStorageObjectSummary("test/file/1", 1), dummyStorageObjectSummary("test/file/2", 1)));
    }

    public static StorageObjectSummary dummyStorageObjectSummary(String object, int contentLength){
        return StorageObjectSummary.builder()
                .key(object)
                .contentLength(contentLength)
                .lastModified(Instant.ofEpochMilli(0L))
                .build();
    }

    private static Storage.UploadStreamProvider contents(String data)
    {
        return () -> new ByteArrayInputStream(data.getBytes(UTF_8));
    }

    private static String md5hex(String data)
    {
        return md5hex(data.getBytes(UTF_8));
    }

    private static String md5hex(byte[] data)
    {
        return encodeHex(digestMd5(data));
    }

    private static String readString(InputStream in)
            throws IOException
    {
        return new String(ByteStreams.toByteArray(in), UTF_8);
    }
}
