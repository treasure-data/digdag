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
    private static final String PATH_KEY = "credentials.json.path";
    private static final String CONTENT_KEY = "credentials.json.content";

    @Override
    public String getType()
    {
        return "gcs";
    }

    @Override
    public Storage newStorage(Config config)
    {
        com.google.cloud.storage.Storage storage;
        if (config.has(PATH_KEY)) {
            storage = getStorageFromJsonKey(config, PATH_KEY);
        }
        else if (config.has(CONTENT_KEY)) {
            storage = getStorageFromJsonKey(config, CONTENT_KEY);
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

    private static com.google.cloud.storage.Storage getStorageFromJsonKey(Config config, String configKey)
            throws ConfigException
    {
        try {
            InputStream in;
            if (PATH_KEY.equals(configKey)) {
                String credentialsPath = config.get(PATH_KEY, String.class);
                in = new FileInputStream(credentialsPath);
            }
            else if (CONTENT_KEY.equals(configKey)) {
                String credentials = config.get(CONTENT_KEY, String.class);
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

