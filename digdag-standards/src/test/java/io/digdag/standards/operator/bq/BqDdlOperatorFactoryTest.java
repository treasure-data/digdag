package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqDdlOperatorFactoryTest
{
    private static final String PROJECT_ID = "test-project";
    private static final ObjectMapper OBJECT_MAPPER = DigdagClient.objectMapper();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock TemplateEngine templateEngine;
    @Mock TaskRequest taskRequest;
    @Mock TaskExecutionContext taskExecutionContext;
    @Mock SecretProvider secretProvider;

    @Mock BqJobRunner bqJobRunner;
    @Mock BqJobRunner.Factory bqJobRunnerFactory;

    @Captor ArgumentCaptor<Dataset> datasetCaptor;

    @Captor ArgumentCaptor<String> projectIdCaptor;
    @Captor ArgumentCaptor<String> datasetIdCaptor;

    private BqDdlOperatorFactory factory;
    private Path projectPath;

    @Before
    public void setUp()
            throws Exception
    {
        when(bqJobRunner.projectId()).thenReturn(PROJECT_ID);
        when(bqJobRunnerFactory.create(taskRequest, taskExecutionContext)).thenReturn(bqJobRunner);
        when(taskRequest.getLastStateParams()).thenReturn(newConfig());

        projectPath = temporaryFolder.newFolder().toPath();
        factory = new BqDdlOperatorFactory(OBJECT_MAPPER, bqJobRunnerFactory);
    }

    @Test
    public void testDatasets()
            throws Exception
    {
        Config config = newConfig();

        config.set("_command", "");

        config.set("create_datasets", ImmutableList.of(
                "create-foo",
                "bar-project:create-bar",
                ImmutableMap.of(
                        "id", "create-baz",
                        "friendly_name", "a baz table",
                        "default_table_expiration_ms", Long.toString(TimeUnit.DAYS.toMillis(1)),
                        "labels", ImmutableMap.of(
                                "l1", "v1",
                                "l2", "v2"
                        )
                )));

        config.set("empty_datasets", ImmutableList.of(
                "empty-foo",
                "bar-project:empty-bar",
                ImmutableMap.of(
                        "id", "empty-baz",
                        "friendly_name", "a baz table",
                        "default_table_expiration_ms", Long.toString(TimeUnit.DAYS.toMillis(1)),
                        "labels", ImmutableMap.of(
                                "l1", "v1",
                                "l2", "v2"
                        )
                )));

        config.set("delete_datasets", ImmutableList.of(
                "delete-foo",
                "bar-project:delete-bar"));

        when(taskRequest.getConfig()).thenReturn(config);

        Operator operator = factory.newOperator(projectPath, taskRequest);

        operator.run(taskExecutionContext);

        InOrder inOrder = Mockito.inOrder(bqJobRunner);

        inOrder.verify(bqJobRunner).deleteDataset(PROJECT_ID, "delete-foo");
        inOrder.verify(bqJobRunner).deleteDataset("bar-project", "delete-bar");
        inOrder.verify(bqJobRunner, times(3)).emptyDataset(datasetCaptor.capture());
        inOrder.verify(bqJobRunner, times(3)).createDataset(datasetCaptor.capture());

        // Empty + Create

        List<Dataset> datasets = datasetCaptor.getAllValues();

        assertThat(datasets.size(), is(6));

        assertThat(datasets.get(0), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setDatasetId("empty-foo"))));

        assertThat(datasets.get(1), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId("bar-project")
                        .setDatasetId("empty-bar"))));

        assertThat(datasets.get(2), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setDatasetId("empty-baz"))
                .setFriendlyName("a baz table")
                .setDefaultTableExpirationMs(TimeUnit.DAYS.toMillis(1))
                .setLabels(ImmutableMap.of(
                        "l1", "v1",
                        "l2", "v2"
                ))));

        assertThat(datasets.get(3), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setDatasetId("create-foo"))));

        assertThat(datasets.get(4), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setProjectId("bar-project")
                        .setDatasetId("create-bar"))));

        assertThat(datasets.get(5), is(new Dataset()
                .setDatasetReference(new DatasetReference()
                        .setDatasetId("create-baz"))
                .setFriendlyName("a baz table")
                .setDefaultTableExpirationMs(TimeUnit.DAYS.toMillis(1))
                .setLabels(ImmutableMap.of(
                        "l1", "v1",
                        "l2", "v2"
                ))));
    }
}