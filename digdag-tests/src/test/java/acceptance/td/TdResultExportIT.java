package acceptance.td;

import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDExportResultJobRequest;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
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
    }

    @Test
    public void testSubmitResultExportJob()
    {
        String resultUrl = "td://@/" + this.database + "/" + this.table;
        TDExportResultJobRequest req = TDExportResultJobRequest.builder()
                .jobId(this.jobId)
                .resultOutput(resultUrl)
                .build();
        client.submitResultExportJob(req);
    }

    @After
    public void deleteQuery()
            throws Exception
    {
        if (client != null) {
            client.deleteSavedQuery(savedQuery);
        }
    }

    @After
    public void deleteDatabase()
            throws Exception
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
