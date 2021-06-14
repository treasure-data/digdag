package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqDdlOperatorFactoryTest
{
    private static final String PROJECT_ID = "test-project";
    private static final ObjectMapper OBJECT_MAPPER = DigdagClient.objectMapper();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock TemplateEngine templateEngine;
    @Mock TaskRequest taskRequest;
    @Mock SecretProvider secrets;

    @Mock BqClient bqClient;
    @Mock BqClient.Factory bqClientFactory;

    @Mock GoogleCredential googleCredential;
    @Mock GcpCredential gcpCredential;
    @Mock GcpCredentialProvider gcpCredentialProvider;

    @Captor ArgumentCaptor<Dataset> createDatasetCaptor;
    @Captor ArgumentCaptor<Dataset> emptyDatasetCaptor;
    @Captor ArgumentCaptor<String> projectIdCaptor;
    @Captor ArgumentCaptor<String> datasetIdCaptor;
    @Captor ArgumentCaptor<Table> createTableCaptor;
    @Captor ArgumentCaptor<Table> emptyTableCaptor;

    private Path projectPath;
    private OperatorContext operatorContext;
    private BqDdlOperatorFactory factory;

    @Before
    public void setUp()
            throws Exception
    {
        when(gcpCredential.credential()).thenReturn(googleCredential);
        when(gcpCredential.projectId()).thenReturn(Optional.of(PROJECT_ID));
        when(gcpCredentialProvider.credential(secrets)).thenReturn(gcpCredential);

        when(secrets.getSecretOptional("gcp.project")).thenReturn(Optional.of(PROJECT_ID));

        when(bqClientFactory.create(googleCredential)).thenReturn(bqClient);
        when(taskRequest.getLastStateParams()).thenReturn(newConfig());

        projectPath = temporaryFolder.newFolder().toPath();
        operatorContext = newContext(projectPath, taskRequest, secrets);
        factory = new BqDdlOperatorFactory(OBJECT_MAPPER, bqClientFactory, gcpCredentialProvider);
    }

    @Test
    public void testEmptyBqDdl()
    {
        Config config = newConfig();
        config.set("_command", "");
        when(taskRequest.getConfig()).thenReturn(config);
        Operator operator = factory.newOperator(operatorContext);
        operator.run();
        verify(bqClient).close();
        verifyNoMoreInteractions(bqClient);
    }

    @Test
    public void testEmptyBqDdl2()
    {
        Config config = newConfig();
        config.set("_command", "");
        config.set("create_datasets", ImmutableList.of());
        config.set("empty_datasets", ImmutableList.of());
        config.set("delete_datasets", ImmutableList.of());
        config.set("create_tables", ImmutableList.of());
        config.set("empty_tables", ImmutableList.of());
        config.set("delete_tables", ImmutableList.of());
        when(taskRequest.getConfig()).thenReturn(config);
        Operator operator = factory.newOperator(operatorContext);
        operator.run();
        verify(bqClient).close();
        verifyNoMoreInteractions(bqClient);
    }

    @Test
    public void testBqDdl()
            throws Exception
    {
        Config config = newConfig();

        config.set("_command", "");

        config.set("dataset", "the_default_dataset");

        config.set("create_datasets", ImmutableList.of(
                "create_dataset_1",
                "project_2:create_dataset_2",
                ImmutableMap.of("id", "create_dataset_3"),
                ImmutableMap.of(
                        "project", "project_4",
                        "id", "create_dataset_4",
                        "friendly_name", "create dataset 4",
                        "default_table_expiration", "1d",
                        "labels", ImmutableMap.of(
                                "l1", "v1",
                                "l2", "v2"
                        )
                )));

        config.set("empty_datasets", ImmutableList.of(
                "empty_dataset_1",
                "project_2:empty_dataset_2",
                ImmutableMap.of("id", "empty_dataset_3"),
                ImmutableMap.of(
                        "project", "project_4",
                        "id", "empty_dataset_4",
                        "friendly_name", "empty dataset 4",
                        "default_table_expiration", "1d",
                        "labels", ImmutableMap.of(
                                "l1", "v1",
                                "l2", "v2"
                        )
                )));

        config.set("delete_datasets", ImmutableList.of(
                "delete_dataset_1",
                "project_2:delete_dataset_2"));

        config.set("create_tables", ImmutableList.of(
                "create_table_1",
                "dataset_2.create_table_2",
                "project_3:create_table_3",
                "project_4:dataset_4.create_table_4",
                ImmutableMap.of("id", "create_table_5"),
                ImmutableMap.of(
                        "id", "create_table_6",
                        "friendly_name", "create table 6",
                        "expiration_time", "2017-01-02T01:02:03Z",
                        "schema", ImmutableMap.of(
                                "fields", ImmutableList.of(
                                        ImmutableMap.of(
                                                "name", "f1",
                                                "type", "STRING"
                                        ),
                                        ImmutableMap.of(
                                                "name", "f2",
                                                "type", "STRING"
                                        )
                                )
                        )
                )
        ));

        config.set("empty_tables", ImmutableList.of(
                "empty_table_1",
                "dataset_2.empty_table_2",
                "project_3:empty_table_3",
                "project_4:dataset_4.empty_table_4",
                ImmutableMap.of("id", "empty_table_5"),
                ImmutableMap.<String, Object>builder()
                        .put("project", "project_6")
                        .put("dataset", "dataset_6")
                        .put("id", "empty_table_6")
                        .put("friendly_name", "empty table 6")
                        .put("expiration_time", "2017-01-02T01:02:03Z")
                        .put("schema", ImmutableMap.of(
                                "fields", ImmutableList.of(
                                        ImmutableMap.of(
                                                "name", "f1",
                                                "type", "STRING"
                                        ),
                                        ImmutableMap.of(
                                                "name", "f2",
                                                "type", "STRING"
                                        )
                                )
                        )).build()
                )
        );

        config.set("delete_tables", ImmutableList.of(
                "delete_table_1",
                "dataset_2.delete_table_2",
                "project_3:delete_table_3",
                "project_4:dataset_4.delete_table_4"
        ));

        when(taskRequest.getConfig()).thenReturn(config);

        Operator operator = factory.newOperator(operatorContext);

        operator.run();

        InOrder inOrder = Mockito.inOrder(bqClient);

        inOrder.verify(bqClient).deleteDataset(PROJECT_ID, "delete_dataset_1");
        inOrder.verify(bqClient).deleteDataset("project_2", "delete_dataset_2");
        inOrder.verify(bqClient, times(4)).emptyDataset(eq(PROJECT_ID), emptyDatasetCaptor.capture());
        inOrder.verify(bqClient, times(4)).createDataset(eq(PROJECT_ID), createDatasetCaptor.capture());

        inOrder.verify(bqClient).deleteTable(PROJECT_ID, "the_default_dataset", "delete_table_1");
        inOrder.verify(bqClient).deleteTable(PROJECT_ID, "dataset_2", "delete_table_2");
        inOrder.verify(bqClient).deleteTable("project_3", "the_default_dataset", "delete_table_3");
        inOrder.verify(bqClient).deleteTable("project_4", "dataset_4", "delete_table_4");
        inOrder.verify(bqClient, times(6)).emptyTable(eq(PROJECT_ID), emptyTableCaptor.capture());
        inOrder.verify(bqClient, times(6)).createTable(eq(PROJECT_ID), createTableCaptor.capture());

        // Datasets

        List<Dataset> emptyDatasets = emptyDatasetCaptor.getAllValues();

        assertThat(emptyDatasets.size(), is(4));

        assertThat(emptyDatasets.get(0), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("empty_dataset_1"))));

        assertThat(emptyDatasets.get(1), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId("project_2")
                        .setDatasetId("empty_dataset_2"))));

        assertThat(emptyDatasets.get(2), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("empty_dataset_3"))));

        assertThat(emptyDatasets.get(3), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId("project_4")
                        .setDatasetId("empty_dataset_4"))
                .setFriendlyName("empty dataset 4")
                .setDefaultTableExpirationMs(DAYS.toMillis(1))
                .setLabels(ImmutableMap.of(
                        "l1", "v1",
                        "l2", "v2"
                ))));

        List<Dataset> createDatasets = createDatasetCaptor.getAllValues();

        assertThat(createDatasets.size(), is(4));

        assertThat(createDatasets.get(0), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("create_dataset_1"))));

        assertThat(createDatasets.get(1), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId("project_2")
                        .setDatasetId("create_dataset_2"))));

        assertThat(createDatasets.get(2), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("create_dataset_3"))));

        assertThat(createDatasets.get(3), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId("project_4")
                        .setDatasetId("create_dataset_4"))
                .setFriendlyName("create dataset 4")
                .setDefaultTableExpirationMs(DAYS.toMillis(1))
                .setLabels(ImmutableMap.of(
                        "l1", "v1",
                        "l2", "v2"
                ))));

        // Tables

        List<Table> emptyTables = emptyTableCaptor.getAllValues();

        assertThat(emptyTables.size(), is(6));

        assertThat(emptyTables.get(0), is(new Table()
                .setTableReference(new TableReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("the_default_dataset")
                        .setTableId("empty_table_1"))));

        assertThat(emptyTables.get(1), is(new Table()
                .setTableReference(new TableReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("dataset_2")
                        .setTableId("empty_table_2"))));

        assertThat(emptyTables.get(2), is(new Table()
                .setTableReference(new TableReference()
                        .setProjectId("project_3")
                        .setDatasetId("the_default_dataset")
                        .setTableId("empty_table_3"))));

        assertThat(emptyTables.get(3), is(new Table()
                .setTableReference(new TableReference()
                        .setProjectId("project_4")
                        .setDatasetId("dataset_4")
                        .setTableId("empty_table_4"))));

        assertThat(emptyTables.get(4), is(new Table()
                .setTableReference(new TableReference()
                        .setProjectId(PROJECT_ID)
                        .setDatasetId("the_default_dataset")
                        .setTableId("empty_table_5"))));

        assertThat(emptyTables.get(5), is(new Table()
                .setTableReference(new TableReference()
                        .setProjectId("project_6")
                        .setDatasetId("dataset_6")
                        .setTableId("empty_table_6"))
                .setFriendlyName("empty table 6")
                .setExpirationTime(Instant.parse("2017-01-02T01:02:03Z").toEpochMilli())
                .setSchema(new TableSchema()
                        .setFields(ImmutableList.of(
                                new TableFieldSchema()
                                        .setName("f1")
                                        .setType("STRING"),
                                new TableFieldSchema()
                                        .setName("f2")
                                        .setType("STRING")
                        )))));
    }
}
