package io.digdag.storage.s3;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.Before;
import org.junit.Test;
import com.google.common.io.ByteStreams;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObjectSummary;

import static org.junit.Assume.assumeThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.MatcherAssert.assertThat;


import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.util.Md5CountInputStream.digestMd5;
import static io.digdag.core.storage.StorageManager.encodeHex;
import static java.nio.charset.StandardCharsets.UTF_8;

public class S3StorageTest
{
    private static final String TEST_S3_ENDPOINT = System.getenv("TEST_S3_ENDPOINT");
    private static final String TEST_S3_ACCESS_KEY_ID = System.getenv().getOrDefault("TEST_S3_ACCESS_KEY_ID", "test");
    private static final String TEST_S3_SECRET_ACCESS_KEY = System.getenv().getOrDefault("TEST_S3_SECRET_ACCESS_KEY", "test");

    private Storage storage;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TEST_S3_ENDPOINT, not(isEmptyOrNullString()));

        AWSCredentials credentials = new BasicAWSCredentials(TEST_S3_ACCESS_KEY_ID, TEST_S3_SECRET_ACCESS_KEY);
        AmazonS3Client s3 = new AmazonS3Client(credentials);
        s3.setEndpoint(TEST_S3_ENDPOINT);

        String bucket = UUID.randomUUID().toString();
        s3.createBucket(bucket);

        ConfigFactory cf = new ConfigFactory(objectMapper());
        Config config = cf.create()
            .set("endpoint", TEST_S3_ENDPOINT)
            .set("bucket", bucket)  // use unique bucket name
            .set("credentials.access-key-id", TEST_S3_ACCESS_KEY_ID)
            .set("credentials.secret-access-key", TEST_S3_SECRET_ACCESS_KEY)
            ;
        storage = new S3StorageFactory().newStorage(config);
    }

    @Test
    public void putReturnsMd5()
        throws Exception
    {
        String checksum1 = storage.put("key1", 10, contents("0123456789"));
        String checksum2 = storage.put("key2", 5, contents("01234"));
        assertThat(checksum1, is(md5hex("0123456789")));
        assertThat(checksum2, is(md5hex("01234")));
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
        storage.put("key/file/3", 2, contents("12"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("key", (chunk) -> all.addAll(chunk));

        assertThat(all.size(), is(3));
        assertThat(all.get(0).getKey(), is("key/file/1"));
        assertThat(all.get(0).getContentLength(), is(3L));
        assertThat(all.get(1).getKey(), is("key/file/2"));
        assertThat(all.get(1).getContentLength(), is(1L));
        assertThat(all.get(2).getKey(), is("key/file/3"));
        assertThat(all.get(2).getContentLength(), is(2L));
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
        assertThat(all.size(), is(2));
        assertThat(all.get(0).getKey(), is("test/file/1"));
        assertThat(all.get(1).getKey(), is("test/file/2"));
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
