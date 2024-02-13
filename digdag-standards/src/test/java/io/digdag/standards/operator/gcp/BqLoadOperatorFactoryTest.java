package io.digdag.standards.operator.gcp;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.api.services.bigquery.model.RangePartitioning.Range;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TemplateEngine;

import static io.digdag.client.config.ConfigUtils.configFactory;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class BqLoadOperatorFactoryTest {
    private static final String PROJECT_ID = "test-project";
    private static final ObjectMapper OBJECT_MAPPER = DigdagClient.objectMapper();

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
    private BqLoadOperatorFactory factory;

    private static final String dummyPath = "gs://bucket/path";
    private static final String dummyDataset = "some_dataset";
    private static final String dummyTable = "some_table";

    private void doTestBqLoad(Config config) throws IOException {
        Answer<Void> answer = new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                JobConfigurationLoad jobConf = invocation.getArgumentAt(1, Job.class).getConfiguration().getLoad();

                assertThat(jobConf.getSourceUris(), is(contains(sourceUris(config).toArray())));

                String destinaionTable = jobConf.getDestinationTable().getDatasetId() + "."
                        + jobConf.getDestinationTable().getTableId();
                assertThat(destinaionTable, is(config.get("destination_table", String.class)));
                assertThat(jobConf.getSourceFormat(), is(config.get("source_format", String.class, null)));
                assertThat(jobConf.getAutodetect(), is(config.get("autodetect", Boolean.class, null)));

                Config clustering = config.getNestedOrGetEmpty("clustering");
                if (!clustering.isEmpty())
                    assertThat(jobConf.getClustering().getFields(),
                            is(contains(clustering.getList("fields", String.class).toArray())));

                if (config.has("decimal_target_types"))
                    assertThat(jobConf.getDecimalTargetTypes(),
                            is(contains(config.getList("decimal_target_types", String.class).toArray())));

                Config encryptionConfiguration = config.getNestedOrGetEmpty("encryption_configuration");
                if (!encryptionConfiguration.isEmpty())
                    assertThat(jobConf.getDestinationEncryptionConfiguration().getKmsKeyName(),
                            is(encryptionConfiguration.get("kmsKeyName", String.class)));

                Config hivePartitioningOptions = config.getNestedOrGetEmpty("hive_partitioning_options");
                if (!hivePartitioningOptions.isEmpty()) {
                    assertThat(jobConf.getHivePartitioningOptions().getMode(),
                            is(hivePartitioningOptions.get("mode", String.class)));
                    assertThat(jobConf.getHivePartitioningOptions().getSourceUriPrefix(),
                            is(hivePartitioningOptions.get("sourceUriPrefix", String.class)));
                }

                assertThat(jobConf.getJsonExtension(), is(config.get("json_extension", String.class, null)));
                assertThat(jobConf.getNullMarker(), is(config.get("null_marker", String.class, null)));

                Config parquetOptions = config.getNestedOrGetEmpty("parquet_options");
                if (!parquetOptions.isEmpty()) {
                    assertThat(jobConf.getParquetOptions().getEnableListInference(),
                            is(parquetOptions.get("enableListInference", Boolean.class, null)));
                    assertThat(jobConf.getParquetOptions().getEnumAsString(),
                            is(parquetOptions.get("enumAsString", Boolean.class, null)));
                }

                assertThat(jobConf.getUseAvroLogicalTypes(),
                        is(config.get("use_avro_logical_types", Boolean.class, null)));

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

    private List<String> sourceUris(Config params) {
        try {
            return params.parseList("_command", String.class);
        } catch (ConfigException ignore) {
            return ImmutableList.of(params.get("_command", String.class));
        }
    }

    @Before
    public void setUp() throws Exception
    {
        when(gcpCredential.credential()).thenReturn(googleCredential);
        when(gcpCredential.projectId()).thenReturn(Optional.of(PROJECT_ID));
        when(gcpCredentialProvider.credential(secrets)).thenReturn(gcpCredential);
        when(secrets.getSecretOptional("gcp.project")).thenReturn(Optional.of(PROJECT_ID));
        when(bqClientFactory.create(googleCredential)).thenReturn(bqClient);

        factory = new BqLoadOperatorFactory(OBJECT_MAPPER, templateEngine, bqClientFactory, gcpCredentialProvider);
    }

    @Test
    public void testBqLoadJsonTimePartitioningDay() throws IOException {
        Config config = newConfig();
        config.set("_command", dummyPath);
        config.set("destination_table", dummyDataset + "." + dummyTable);
        config.set("source_format", "NEWLINE_DELIMITED_JSON");
        config.set("autodetect", true);
        config.set("clustering", ImmutableMap.of(
                "fields", ImmutableList.of("field1", "field2")));
        config.set("decimal_target_types", ImmutableList.of("BIGNUMERIC", "STRING"));
        config.set("encryption_configuration", ImmutableMap.of(
                "kmsKeyName", "dummy_key"));
        config.set("json_extension", "GEOJSON");
        config.set("time_partitioning", ImmutableMap.of(
                "type", "DAY",
                "field", "date",
                "requirePartitionFilter", true,
                "expirationMs", 3600000L));

        doTestBqLoad(config);
    }

    @Test
    public void testBqLoadCsvTimePartitioningYear() throws IOException {
        Config config = newConfig();
        config.set("_command", dummyPath);
        config.set("destination_table", dummyDataset + "." + dummyTable);
        config.set("source_format", "CSV");
        config.set("autodetect", true);
        config.set("null_marker", "NULL");
        config.set("time_partitioning", ImmutableMap.of(
                "type", "YEAR",
                "field", "date",
                "requirePartitionFilter", true,
                "expirationMs", 3600000L));

        doTestBqLoad(config);
    }

    @Test
    public void testBqLoadJsonHivePartition() throws IOException {
        Config config = newConfig();
        config.set("_command", dummyPath);
        config.set("destination_table", dummyDataset + "." + dummyTable);
        config.set("source_format", "NEWLINE_DELIMITED_JSON");
        config.set("hive_partitioning_options", ImmutableMap.of(
                "mode", "AUTO",
                "sourceUriPrefix", dummyPath));

        doTestBqLoad(config);
    }

    @Test
    public void testBqLoadParquetRangePartition() throws IOException {
        Config config = newConfig();
        config.set("_command", dummyPath);
        config.set("destination_table", dummyDataset + "." + dummyTable);
        config.set("source_format", "PARQUET");
        config.set("parquet_options", ImmutableMap.of(
                "enableListInference", true,
                "enumAsString", true));
        config.set("range_partitioning", ImmutableMap.of(
                "field", "id",
                "range", ImmutableMap.of(
                        "start", 0L,
                        "interval", 10L,
                        "end", 100L)));

        doTestBqLoad(config);
    }

    @Test
    public void testBqLoadAvro() throws IOException {
        Config config = newConfig();
        config.set("_command", dummyPath);
        config.set("destination_table", dummyDataset + "." + dummyTable);
        config.set("source_format", "AVRO");
        config.set("use_avro_logical_types", true);

        doTestBqLoad(config);
    }
}
