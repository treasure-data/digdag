package io.digdag.standards.operator.gcp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.core.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

class GcsClient
        extends BaseGcpClient<Storage>
{
    private static Logger logger = LoggerFactory.getLogger(GcsClient.class);

    GcsClient(GoogleCredential credential, Optional<ProxyConfig> proxyConfig)
    {
        super(credential, proxyConfig);
    }

    @Override
    protected Storage client(GoogleCredential credential, HttpTransport transport, JsonFactory jsonFactory)
    {
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Storage.Builder(transport, jsonFactory, credential)
                .setApplicationName("Digdag")
                .build();
    }

    Optional<StorageObject> stat(String bucket, String object)
            throws IOException
    {
        try {
            return Optional.of(client.objects()
                    .get(bucket, object)
                    .execute());
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return Optional.absent();
            }
            throw e;
        }
    }

    static class Factory
            extends BaseGcpClient.Factory
    {
        @Inject
        public Factory(@Environment Map<String, String> environment)
        {
            super(environment);
        }

        GcsClient create(GoogleCredential credential)
        {
            return new GcsClient(credential, proxyConfig);
        }
    }
}
