package io.digdag.storage.gcs;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class GCSStorageFactory
        implements StorageFactory
{
    @Override
    public String getType()
    {
        return "gcs";
    }

    @Override
    public Storage newStorage(Config config)
    {
        com.google.cloud.storage.Storage storage;
        if (config.has("credentials.json")) {
            storage = getStorageFromJsonKey(config, "credentials.json.path");
        }
        else if (config.has("credentials.json.content")) {
            storage = getStorageFromJsonKey(config, "credentials.json.content");
        }
        else {
            storage = StorageOptions.getDefaultInstance().getService();
        }

        String bucket = config.get("bucket", String.class);
        return new GCSStorage(config, storage, bucket);
    }

    @VisibleForTesting
    Storage newStorage(com.google.cloud.storage.Storage storage, Config config)
    {
        String bucket = config.get("bucket", String.class);
        return new GCSStorage(config, storage, bucket);
    }

    private static com.google.cloud.storage.Storage getStorageFromJsonKey(Config config, String path)
            throws ConfigException
    {
        try {
            InputStream in;
            if (path == "credentials.json.path") {
                String credentialsPath = config.get("credentials.json.path", String.class);
                in = new FileInputStream(credentialsPath);
            }
            else if (path == "credentials.json.content") {
                String credentials = config.get("credentials.json.content", String.class);
                in = new ByteArrayInputStream(credentials.getBytes("utf-8"));
            }
            else {
                throw new ConfigException("credentials config must have credentials.json: or credentials.json.content:");
            }

            return StorageOptions.newBuilder()
                    .setCredentials(ServiceAccountCredentials.fromStream(in))
                    .build()
                    .getService();
        }
        catch (FileNotFoundException e) {
            throw new ConfigException("Credential File could not be found. Please check credentials.json");
        }
        catch (UnsupportedEncodingException e) {
            throw new ConfigException("Could not read credentials.json.content caused by use unsupported encoding type (utf-8)");
        }
        catch (IOException e) {
            throw new ConfigException("The credential cannot be created from the stream");
        }
    }
}

