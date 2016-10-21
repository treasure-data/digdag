package io.digdag.standards.operator.bq;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.core.Environment;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.Proxies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;
import java.util.Map;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.nio.charset.StandardCharsets.UTF_8;

class BqClient
        implements AutoCloseable
{
    private static Logger logger = LoggerFactory.getLogger(BqClient.class);

    private final Bigquery client;
    private final NetHttpTransport transport;

    BqClient(GoogleCredential credential, Optional<ProxyConfig> proxyConfig)
    {
        this.transport = new NetHttpTransport.Builder()
                .setProxy(proxy(proxyConfig))
                .build();
        this.client = client(credential, transport);
    }

    @Override
    public void close()
    {
        try {
            transport.shutdown();
        }
        catch (IOException e) {
            logger.warn("Error shutting down BigQuery client", e);
        }
    }

    private static Bigquery client(GoogleCredential credential, HttpTransport transport)
    {
        JsonFactory jsonFactory = new JacksonFactory();

        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Digdag")
                .build();
    }

    private static Proxy proxy(Optional<ProxyConfig> proxyConfig)
    {
        if (!proxyConfig.isPresent()) {
            return Proxy.NO_PROXY;
        }

        ProxyConfig cfg = proxyConfig.get();
        InetSocketAddress address = new InetSocketAddress(cfg.getHost(), cfg.getPort());
        Proxy proxy = new Proxy(Proxy.Type.HTTP, address);

        // TODO: support authenticated proxying
        Optional<String> user = cfg.getUser();
        Optional<String> password = cfg.getPassword();
        if (user.isPresent() || password.isPresent()) {
            logger.warn("Authenticated proxy is not supported");
        }

        return proxy;
    }

    void createDataset(String projectId, Dataset dataset)
            throws IOException
    {
        try {
            client.datasets().insert(projectId, dataset)
                    .execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_CONFLICT) {
                logger.debug("Dataset already exists: {}:{}", dataset.getDatasetReference());
            }
            else {
                throw e;
            }
        }
    }

    void emptyDataset(String projectId, Dataset dataset)
            throws IOException
    {
        String datasetId = dataset.getDatasetReference().getDatasetId();
        deleteDataset(projectId, datasetId);
        createDataset(projectId, dataset);
    }

    private void deleteTables(String projectId, String datasetId)
            throws IOException
    {
        Bigquery.Tables.List list = client.tables().list(projectId, datasetId);
        TableList tableList;
        do {
            tableList = list.execute();
            List<TableList.Tables> tables = tableList.getTables();
            if (tables != null) {
                for (TableList.Tables table : tables) {
                    deleteTable(projectId, datasetId, table.getTableReference().getTableId());
                }
            }
        }
        while (tableList.getNextPageToken() != null);
    }

    void deleteTable(String projectId, String datasetId, String tableId)
            throws IOException
    {
        try {
            client.tables().delete(projectId, datasetId, tableId).execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                // Already deleted
                return;
            }
            throw e;
        }
    }

    boolean datasetExists(String projectId, String datasetId)
            throws IOException
    {
        try {
            client.datasets().get(projectId, datasetId).execute();
            return true;
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    void deleteDataset(String projectId, String datasetId)
            throws IOException
    {
        if (datasetExists(projectId, datasetId)) {
            deleteTables(projectId, datasetId);

            try {
                client.datasets().delete(projectId, datasetId).execute();
            }
            catch (GoogleJsonResponseException e) {
                if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                    // Already deleted
                    return;
                }
                throw e;
            }
        }
    }

    void createTable(String projectId, Table table)
            throws IOException
    {
        String datasetId = table.getTableReference().getDatasetId();
        try {
            client.tables().insert(projectId, datasetId, table)
                    .execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_CONFLICT) {
                logger.debug("Table already exists: {}:{}.{}", projectId, datasetId, table.getTableReference().getTableId());
            }
            else {
                throw e;
            }
        }
    }

    void emptyTable(String projectId, Table table)
            throws IOException
    {
        TableReference r = table.getTableReference();
        deleteTable(r.getProjectId(), r.getDatasetId(), r.getTableId());
        createTable(projectId, table);
    }

    void submitJob(String projectId, Job job)
            throws IOException
    {
        client.jobs()
                .insert(projectId, job)
                .execute();
    }

    public Job jobStatus(String projectId, String jobId)
            throws IOException
    {
        return client.jobs()
                .get(projectId, jobId)
                .execute();
    }

    static class Factory
    {
        private final Optional<ProxyConfig> proxyConfig;

        @Inject
        public Factory(@Environment Map<String, String> environment)
        {
            this.proxyConfig = Proxies.proxyConfigFromEnv("https", environment);
        }

        BqClient create(GoogleCredential credential)
        {
            return new BqClient(credential, proxyConfig);
        }
    }
}
