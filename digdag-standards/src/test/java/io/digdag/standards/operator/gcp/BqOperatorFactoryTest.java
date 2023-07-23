package io.digdag.standards.operator.gcp;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.RangePartitioning.Range;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.digdag.client.config.Config;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TemplateEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static io.digdag.client.config.ConfigUtils.configFactory;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class BqOperatorFactoryTest {
        private static final String PROJECT_ID = "test-project";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock
    TemplateEngine templateEngine;
    @Mock
    SecretProvider secrets;

    @Mock
    BqClient bqClient;
    @Mock
    BqClient.Factory bqClientFactory;

    @Mock
    GoogleCredential googleCredential;
    @Mock
    GcpCredential gcpCredential;
    @Mock
    GcpCredentialProvider gcpCredentialProvider;

    private Path projectPath;
    private OperatorContext operatorContext;
    private BqOperatorFactory factory;

    private static final String dummyQuery = "SELECT 1";
    private static final String dummyDataset = "some_dataset";
    private static final String dummyTable = "some_table";

    private void doTestBq(Config config) throws IOException {
        Answer<Void> answer = new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                JobConfigurationQuery jobConf = invocation.getArgumentAt(1, Job.class).getConfiguration().getQuery();

                assertThat(jobConf.getQuery(), is(config.get("query", String.class)));
                // bq operator sets default value of use_legacy_sql at false
                assertThat(jobConf.getUseLegacySql(), is(config.get("use_legacy_sql", Boolean.class, false)));
                assertThat(jobConf.getAllowLargeResults(), is(config.get("allow_large_results", Boolean.class, null)));
                assertThat(jobConf.getUseQueryCache(), is(config.get("use_query_cache", Boolean.class, null)));
                assertThat(jobConf.getCreateDisposition(), is(config.get("create_disposition", String.class, null)));
                assertThat(jobConf.getWriteDisposition(), is(config.get("write_disposition", String.class, null)));
                assertThat(jobConf.getFlattenResults(), is(config.get("flatten_results", Boolean.class, null)));
                assertThat(jobConf.getMaximumBillingTier(),
                        is(config.get("maximum_billing_tier", Integer.class, null)));
                assertThat(jobConf.getPriority(), is(config.get("priority", String.class, null)));

                String destinaionTable = jobConf.getDestinationTable().getDatasetId() + "."
                        + jobConf.getDestinationTable().getTableId();
                assertThat(destinaionTable, is(config.get("destination_table", String.class)));

                Config clustering = config.getNestedOrGetEmpty("clustering");
                if (!clustering.isEmpty())
                    assertThat(jobConf.getClustering().getFields(),
                            is(contains(clustering.getList("fields", String.class).toArray())));

                Config encryptionConfiguration = config.getNestedOrGetEmpty("encryption_configuration");
                if (!encryptionConfiguration.isEmpty())
                    assertThat(jobConf.getDestinationEncryptionConfiguration().getKmsKeyName(),
                            is(encryptionConfiguration.get("kmsKeyName", String.class)));

                assertThat(jobConf.getMaximumBytesBilled(), is(config.get("maximum_bytes_billed", Long.class, null)));

                List<String> schema_update_options = config.getListOrEmpty("schema_update_options", String.class);
                if (!schema_update_options.isEmpty())
                    assertThat(jobConf.getSchemaUpdateOptions(), is(contains(schema_update_options.toArray())));

                Config rangePartitioning = config.getNestedOrGetEmpty("range_partitioning");
                if (!rangePartitioning.isEmpty()) {
                    assertThat(jobConf.getRangePartitioning().getField(),
                            is(rangePartitioning.get("field", String.class)));
                    Range range = jobConf.getRangePartitioning().getRange();
                    Config rangeConfig = rangePartitioning.getNested("range");
                    assertThat(range.getStart(),
                            is(rangeConfig.get("start", Long.class)));
                    assertThat(range.getInterval(),
                            is(rangeConfig.get("interval", Long.class)));
                    assertThat(range.getEnd(),
                            is(rangeConfig.get("end", Long.class)));
                }

                Config timePartitioningConfig = config.getNestedOrGetEmpty("time_partitioning");
                if (!timePartitioningConfig.isEmpty()) {
                    TimePartitioning timePartitioning = jobConf.getTimePartitioning();
                    assertThat(timePartitioning.getType(),
                            is(timePartitioningConfig.get("type", String.class)));
                    assertThat(timePartitioning.getField(),
                            is(timePartitioningConfig.get("field", String.class, null)));
                    assertThat(timePartitioning.getRequirePartitionFilter(),
                            is(timePartitioningConfig.get("requirePartitionFilter", Boolean.class, null)));
                    assertThat(timePartitioning.getExpirationMs(),
                            is(timePartitioningConfig.get("expirationMs", Long.class, null)));
                }

                return null;
            }
        };
        doAnswer(answer).when(bqClient).submitJob(anyString(), any());

        projectPath = temporaryFolder.newFolder().toPath();
        ImmutableTaskRequest taskRequest = ImmutableTaskRequest.builder()
                        .siteId(0)
                        .projectId(0)
                        .projectName("dummy_project")
                        .workflowName("dummy_workflow")
                        .taskId(0)
                        .attemptId(0)
                        .sessionId(0)
                        .taskName("dummy_task")
                        .lockId("dummy_lockId")
                        .timeZone(ZoneId.of("UTC"))
                        .sessionUuid(UUID.randomUUID())
                        .sessionTime(Instant.now())
                        .createdAt(Instant.now())
                        .isCancelRequested(false)
                        .localConfig(newConfig())
                        .config(config)
                        .lastStateParams(newConfig())
                        .build();
        operatorContext = newContext(projectPath, taskRequest, secrets);
        Operator operator = factory.newOperator(operatorContext);

        // first call of operator.run() set BigQuery job id in state param
        // and throw TaskExecutionException
        try {
            operator.run();
        } catch (TaskExecutionException e) {
            taskRequest = taskRequest.withLastStateParams(e.getStateParams(configFactory).get());
        }

        Job job = new Job();
        job.setStatus(new JobStatus().setState("DONE"));
        when(bqClient.jobStatus(anyString(), anyString(), any())).thenReturn(job);

        // second call of operator.run() check BigQUery job status
        operatorContext = newContext(projectPath, taskRequest, secrets);
        operator = factory.newOperator(operatorContext);
        operator.run();
    }

@Before
    public void setUp() throws Exception
    {
        when(gcpCredential.credential()).thenReturn(googleCredential);
        when(gcpCredential.projectId()).thenReturn(Optional.of(PROJECT_ID));
        when(gcpCredentialProvider.credential(secrets)).thenReturn(gcpCredential);
        when(secrets.getSecretOptional("gcp.project")).thenReturn(Optional.of(PROJECT_ID));
        when(bqClientFactory.create(googleCredential)).thenReturn(bqClient);

        factory = new BqOperatorFactory(templateEngine, bqClientFactory, gcpCredentialProvider);
    }

    @Test
    public void testBqTimePartitioningDay() throws IOException {
            Config config = newConfig();
            config.set("query", dummyQuery);
            config.set("destination_table", dummyDataset + "." + dummyTable + "$20230723");
            config.set("time_partitioning", ImmutableMap.of(
                            "type", "DAY",
                            "field", "date",
                            "requirePartitionFilter", true,
                            "expirationMs", 3600000L));
            config.set("clustering", ImmutableMap.of(
                            "fields", ImmutableList.of("field1", "field2")));
            config.set("schema_update_options", ImmutableList.of("ALLOW_FIELD_ADDITION"));
            config.set("maximum_bytes_billed", 1024L);
            config.set("priority", "BATCH");
            config.set("encryption_configuration", ImmutableMap.of(
                            "kmsKeyName", "dummy_key"));

            doTestBq(config);
    }

    @Test
    public void testBqTimePartitioningYear() throws IOException {
            Config config = newConfig();
            config.set("query", dummyQuery);
            config.set("use_legacy_sql", true);
            config.set("destination_table", dummyDataset + "." + dummyTable + "$2023");
            config.set("time_partitioning", ImmutableMap.of(
                            "type", "YEAR",
                            "field", "date",
                            "requirePartitionFilter", true,
                            "expirationMs", 3600000L));
            config.set("clustering", ImmutableMap.of(
                            "fields", ImmutableList.of("field1", "field2")));
            config.set("schema_update_options", ImmutableList.of("ALLOW_FIELD_ADDITION"));
            config.set("maximum_bytes_billed", 1024L);
            config.set("priority", "BATCH");

            doTestBq(config);
    }

    @Test
    public void testBqTimePartitioningHour() throws IOException {
            Config config = newConfig();
            config.set("query", dummyQuery);
            config.set("use_legacy_sql", true);
            config.set("destination_table", dummyDataset + "." + dummyTable + "$2023072301");
            config.set("time_partitioning", ImmutableMap.of(
                            "type", "HOUR",
                            "field", "date",
                            "requirePartitionFilter", true,
                            "expirationMs", 3600000L));
            config.set("clustering", ImmutableMap.of(
                            "fields", ImmutableList.of("field1", "field2")));
            config.set("schema_update_options", ImmutableList.of("ALLOW_FIELD_ADDITION"));
            config.set("maximum_bytes_billed", 1024L);
            config.set("priority", "BATCH");

            doTestBq(config);
    }

    @Test
    public void testBqRangePartitioning() throws IOException {
            Config config = newConfig();
            config.set("query", dummyQuery);
            config.set("destination_table", dummyDataset + "." + dummyTable);
            config.set("range_partitioning", ImmutableMap.of(
                            "field", "id",
                            "range", ImmutableMap.of(
                                            "start", 0L,
                                            "interval", 10L,
                                            "end", 100L)));
            config.set("clustering", ImmutableMap.of(
                            "fields", ImmutableList.of("field1", "field2")));
            config.set("schema_update_options", ImmutableList.of("ALLOW_FIELD_ADDITION"));
            config.set("maximum_bytes_billed", 1024L);
            config.set("priority", "BATCH");

            doTestBq(config);
    }

}