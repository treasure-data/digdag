package acceptance;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.RestWorkflowDefinitionCollection;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class ApiWorkflowsIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private ObjectMapper objectMapper;

    @Before
    public void setUp()
            throws Exception
    {
        objectMapper = new ObjectMapper()
                .registerModule(new GuavaModule())
                .registerModule(new JacksonTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // To deserialize Config class, need ObjectMapper.
        objectMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, objectMapper));

        projectDir = folder.getRoot().toPath().resolve("foobar");
        Files.createDirectory(projectDir);
        config = folder.newFile().toPath();

        copyResource("acceptance/basic.dig", projectDir.resolve("wf1abc.dig"));
        copyResource("acceptance/basic.dig", projectDir.resolve("wf2def.dig"));

        // Push first project. (named: prj1foo)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "prj1foo",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // Push second project. (named: prj2bar)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "prj2bar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }
    }

    @Test
    public void showWorkflows()
            throws Exception
    {
        { // No params
            RestWorkflowDefinitionCollection restWorkflows = callGetWorkflows("");
            assertThat("Num of workflows", restWorkflows.getWorkflows().size(), is(4));
            String ids = restWorkflows.getWorkflows().stream().map((n) -> {
                return n.getId().get();
            }).collect(Collectors.joining(","));
            assertThat("List of workflow ids", ids, is("1,2,3,4"));
        }
        { // order=desc count=2
            RestWorkflowDefinitionCollection restWorkflows = callGetWorkflows("?order=desc&count=2");
            assertThat("Num of workflows", restWorkflows.getWorkflows().size(), is(2));
            String ids = restWorkflows.getWorkflows().stream().map((n) -> {
                return n.getId().get();
            }).collect(Collectors.joining(","));
            assertThat("List of workflow ids", ids, is("4,3"));
        }
        { // order=desc last_id=3
            RestWorkflowDefinitionCollection restWorkflows = callGetWorkflows("?order=desc&last_id=3");
            assertThat("Num of workflows", restWorkflows.getWorkflows().size(), is(2));
            String ids = restWorkflows.getWorkflows().stream().map((n) -> {
                return n.getId().get();
            }).collect(Collectors.joining(","));
            assertThat("List of workflow ids", ids, is("2,1"));
        }
        { // order=desc  count=2 name_pattern=f1a
            RestWorkflowDefinitionCollection restWorkflows = callGetWorkflows("?order=desc&count=2&name_pattern=f1a");
            assertThat("Num of workflows", restWorkflows.getWorkflows().size(), is(2));
            String ids = restWorkflows.getWorkflows().stream().map((n) -> {
                return n.getId().toString() + "-" + n.getName();
            }).collect(Collectors.joining(","));
            assertThat("List of workflows", ids, is("3-wf1abc,1-wf1abc"));
        }

        { // order=desc  count=2 name_pattern=f1a search_project_name=true
            RestWorkflowDefinitionCollection restWorkflows = callGetWorkflows("?order=desc&count=2&name_pattern=prj2&search_project_name=true");
            assertThat("Num of workflows", restWorkflows.getWorkflows().size(), is(2));
            String ids = restWorkflows.getWorkflows().stream().map((n) -> {
                return n.getId().toString() + "-" + n.getProject().getName() + "-" + n.getName();
            }).collect(Collectors.joining(","));
            assertThat("List of workflows", ids, is("4-prj2bar-wf2def,3-prj2bar-wf1abc"));
        }
    }

    private RestWorkflowDefinitionCollection callGetWorkflows(String queryParams)
            throws IOException
    {
        OkHttpClient client = new OkHttpClient();

        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/workflows" + queryParams)
                .build()).execute();
        assertThat("Response failed.", response.isSuccessful(), is(true));
        JsonNode json = objectMapper.readTree(response.body().string());
        RestWorkflowDefinitionCollection restWorkflows = objectMapper.treeToValue(json, RestWorkflowDefinitionCollection.class);
        return restWorkflows;
    }
}
