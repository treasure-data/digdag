package acceptance.td;

import com.google.common.collect.ImmutableSet;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.model.TDTable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static acceptance.td.Secrets.TD_API_KEY;
import static com.google.common.collect.ObjectArrays.concat;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.main;

public class TdDdlIT
{
    private static final Logger logger = LoggerFactory.getLogger(TdDdlIT.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;

    private List<String> tempDatabases = new ArrayList<>();

    private String database = tempDatabaseName();

    private String dropDb1 = tempDatabaseName();
    private String dropDb2 = tempDatabaseName();
    private String createDb1 = tempDatabaseName();
    private String createDb2 = tempDatabaseName();
    private String emptyDb1 = tempDatabaseName();
    private String emptyDb2 = tempDatabaseName();

    private Path outfile;

    private HttpProxyServer proxyServer;

    private TemporaryDigdagServer server;
    private String noTdConf;

    private Map<String, String> env;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        noTdConf = folder.newFolder().toPath().resolve("non-existing-td.conf").toString();
        projectDir = folder.newFolder().toPath();
        config = folder.newFile().toPath();
        Files.write(config, asList("secrets.td.apikey = " + TD_API_KEY));
        outfile = projectDir.resolve("outfile");

        env = new HashMap<>();
        env.put("TD_CONFIG_PATH", noTdConf);

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .build();
        client.createDatabase(database);
    }

    private String tempDatabaseName()
    {
        String name = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        tempDatabases.add(name);
        return name;
    }

    @After
    public void deleteDatabases()
            throws Exception
    {
        if (client != null) {
            for (String db : tempDatabases) {
                try {
                    client.deleteDatabaseIfExists(db);
                }
                catch (TDClientException e) {
                    logger.warn("failed to delete temp db: {}", db, e);
                }
            }
        }
    }

    @After
    public void stopProxy()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
    }

    @After
    public void stopServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void testRetries()
            throws Exception
    {
        int failures = 7;

        Map<String, AtomicInteger> requests = new ConcurrentHashMap<>();

        proxyServer = DefaultHttpProxyServer
                .bootstrap()
                .withPort(0)
                .withFiltersSource(new HttpFiltersSourceAdapter()
                {
                    @Override
                    public int getMaximumRequestBufferSizeInBytes()
                    {
                        return 1024 * 1024;
                    }

                    @Override
                    public HttpFilters filterRequest(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext)
                    {
                        return new HttpFiltersAdapter(httpRequest)
                        {
                            @Override
                            public HttpResponse clientToProxyRequest(HttpObject httpObject)
                            {
                                assert httpObject instanceof FullHttpRequest;
                                FullHttpRequest fullHttpRequest = (FullHttpRequest) httpObject;
                                String key = fullHttpRequest.getMethod() + " " + fullHttpRequest.getUri();
                                int i = requests.computeIfAbsent(key, uri -> new AtomicInteger())
                                        .incrementAndGet();
                                HttpResponse response;
                                if (i < failures) {
                                    logger.info("Simulating 500 INTERNAL SERVER ERROR for request: {}", key);
                                    response = new DefaultFullHttpResponse(httpRequest.getProtocolVersion(), INTERNAL_SERVER_ERROR);
                                }
                                else {
                                    logger.info("Simulation 200 OK for request: {}", key);
                                    response = new DefaultFullHttpResponse(httpRequest.getProtocolVersion(), OK);
                                }
                                response.headers().set(CONNECTION, CLOSE);
                                return response;
                            }
                        };
                    }
                }).start();

        Files.write(config, asList(
                "params.td.use_ssl = false",
                "params.td.proxy.enabled = true",
                "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
        ), APPEND);

        runDdlWorkflow();

        String dropDatabases[] = {dropDb1, dropDb2};
        String createDatabases[] = {createDb1, createDb2};
        String emptyDatabases[] = {emptyDb1, emptyDb2};

        String dropTables[] = {"drop_table_1", "drop_table_2"};
        String createTables[] = {"create_table_1", "create_table_2"};
        String emptyTables[] = {"empty_table_1", "empty_table_2"};

        String endpoint = "http://api.treasuredata.com/";

        for (String table : concat(dropTables, emptyTables, String.class)) {
            String key = "POST " + endpoint + "v3/table/delete/" + database + "/" + table;
            assertThat(requests.get(key).get(), is(failures));
        }
        for (String table : concat(createTables, emptyTables, String.class)) {
            String key = "POST " + endpoint + "v3/table/create/" + database + "/" + table + "/log";
            assertThat(requests.get(key).get(), is(failures));
        }

        for (String db : concat(dropDatabases, emptyDatabases, String.class)) {
            String key = "POST " + endpoint + "v3/database/delete/" + db;
            assertThat(requests.get(key).get(), is(failures));
        }
        for (String db : concat(createDatabases, emptyDatabases, String.class)) {
            String key = "POST " + endpoint + "v3/database/create/" + db;
            assertThat(requests.get(key).get(), is(failures));
        }
    }

    @Test
    public void testDdl()
            throws Exception
    {
        runDdlWorkflow();

        Set<String> databases = ImmutableSet.copyOf(client.listDatabaseNames());
        assertThat(databases, hasItem(createDb1));
        assertThat(databases, hasItem(createDb2));
        assertThat(databases, hasItem(emptyDb1));
        assertThat(databases, hasItem(emptyDb2));
        assertThat(databases, not(hasItem(dropDb1)));
        assertThat(databases, not(hasItem(dropDb2)));

        List<String> tables = client.listTables(database).stream().map(TDTable::getName).collect(toList());
        assertThat(tables, containsInAnyOrder("create_table_1", "create_table_2", "empty_table_1", "empty_table_2"));
    }

    private void runDdlWorkflow()
            throws IOException
    {
        addWorkflow(projectDir, "acceptance/td/td_ddl/td_ddl.dig");
        CommandStatus runStatus = runWorkflow("td_ddl",
                "database=" + database,
                "drop_db_1=" + dropDb1,
                "drop_db_2=" + dropDb2,
                "create_db_1=" + createDb1,
                "create_db_2=" + createDb2,
                "empty_db_1=" + emptyDb1,
                "empty_db_2=" + emptyDb2);
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(Files.exists(outfile), is(true));
    }

    private CommandStatus runWorkflow(String workflow, String... params)
    {
        List<String> args = new ArrayList<>();
        args.addAll(asList("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "outfile=" + outfile));

        for (String param : params) {
            args.add("-p");
            args.add(param);
        }

        args.add(workflow);

        return main(env, args);
    }
}
