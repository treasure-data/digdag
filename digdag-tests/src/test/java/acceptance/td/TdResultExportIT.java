package acceptance.td;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.treasuredata.client.TDApiRequest;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.model.TDExportResultJobRequest;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static acceptance.td.Secrets.TD_API_KEY;
import static acceptance.td.Secrets.TD_API_ENDPOINT;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;

public class TdResultExportIT
{
    private TDClient client;
    private String database;
    private String table;
    private String savedQuery;
    private String jobId;
    private String connectionId;
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private OkHttpClient httpClient;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        assumeThat(TD_API_ENDPOINT, not(isEmptyOrNullString()));

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .setEndpoint(TD_API_ENDPOINT)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);
        String queryName = "test_" + UUID.randomUUID().toString().replace('-', '_');
        createQuery(queryName);
        TDSavedQueryStartRequest req = TDSavedQueryStartRequest.builder()
                .name(queryName)
                .scheduledTime(new Date())
                .build();
        this.jobId = client.startSavedQuery(req);
        this.table = "test_" + UUID.randomUUID().toString().replace('-', '_');
        client.createTableIfNotExists(database, table);
        this.httpClient = new OkHttpClient();
    }

    @Test
    public void testSubmitResultExportJob()
            throws IOException
    {
        String testConnectorName =  "test_" + UUID.randomUUID().toString().replace('-', '_');
        String json = "{\"description\":null,\"name\":\"" + testConnectorName + "\"," +
                "\"settings\":{\"api_key\":\"\",\"api_hostname\":\"\"}," +
                "\"shared\":false,\"type\":\"treasure_data\"}";

        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url("https://" + TD_API_ENDPOINT + "/v4/connections")
                .header("authorization", "TD1 " + TD_API_KEY)
                .post(body)
                .build();
        Response response = httpClient.newCall(request).execute();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(response.body().string());
        connectionId = jsonNode.get("id").asText();

        String resultConnectionSettings = "{\"user_database_name\":\"" + this.database + "\"," +
                "\"user_table_name\":\"" + this.table + "\"}";

        TDExportResultJobRequest req = TDExportResultJobRequest.builder()
                .jobId(this.jobId)
                .resultConnectionId(connectionId)
                .resultConnectionSettings(resultConnectionSettings)
                .build();
        client.submitResultExportJob(req);
    }

    @After
    public void deleteQuery()
    {
        if (client != null) {
            client.deleteSavedQuery(savedQuery);
        }
    }

    @After
    public void deleteConnection()
            throws IOException
    {
        if(connectionId != null){
            Request request = new Request.Builder()
                    .url("https://" + TD_API_ENDPOINT + "/v4/connections/" + connectionId)
                    .header("authorization", "TD1 " + TD_API_KEY)
                    .delete()
                    .build();
            httpClient.newCall(request).execute();
        }
    }

    @After
    public void deleteDatabase()
    {
        if (client != null && database != null) {
            client.deleteDatabase(database);
        }
    }

    private TDSavedQuery createQuery(String name)
    {
        return saveQuery(TDSavedQuery.newBuilder(
                name,
                TDJob.Type.PRESTO,
                database,
                "select 1",
                "Asia/Tokyo")
                .build());
    }

    private TDSavedQuery saveQuery(TDSaveQueryRequest request)
    {
        this.savedQuery = request.getName();
        return client.saveQuery(request);
    }
}
