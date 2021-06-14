package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.operator.jdbc.JdbcOpTestHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;

import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedshiftLoadOperatorFactoryTest
{
    private JdbcOpTestHelper testHelper = new JdbcOpTestHelper();
    private RedshiftLoadOperatorFactory operatorFactory;

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Before
    public void setUp()
    {
        operatorFactory = newOperatorFactory(RedshiftLoadOperatorFactory.class);
    }

    @Test
    public void getType()
    {
        assertThat(operatorFactory.getType(), is("redshift_load"));
    }

    private String getCopyConfig(Map<String, Object> configInput)
            throws IOException
    {
        return getCopyConfig(configInput, false);
    }

    private String getCopyConfig(Map<String, Object> configInput, boolean maskConfig)
            throws IOException
    {
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        OperatorContext operatorContext = mock(OperatorContext.class);
        when(operatorContext.getProjectPath()).thenReturn(testHelper.projectPath());
        when(operatorContext.getTaskRequest()).thenReturn(taskRequest);
        RedshiftLoadOperatorFactory.RedshiftLoadOperator operator = (RedshiftLoadOperatorFactory.RedshiftLoadOperator) operatorFactory.newOperator(operatorContext);
        assertThat(operator, is(instanceOf(RedshiftLoadOperatorFactory.RedshiftLoadOperator.class)));

        AWSSessionCredentials credentials = mock(AWSSessionCredentials.class);
        when(credentials.getAWSAccessKeyId()).thenReturn("my-access-key-id");
        when(credentials.getAWSSecretKey()).thenReturn("my-secret-access-key");

        RedshiftConnection.CopyConfig copyConfig = operator.createCopyConfig(testHelper.createConfig(configInput), credentials);

        Connection connection = mock(Connection.class);

        RedshiftConnection redshiftConnection = new RedshiftConnection(connection);

        return redshiftConnection.buildCopyStatement(copyConfig, maskConfig);
    }

    @Test
    public void newOperator()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "csv", ""
        );
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        OperatorContext operatorContext = mock(OperatorContext.class);
        when(operatorContext.getProjectPath()).thenReturn(testHelper.projectPath());
        when(operatorContext.getTaskRequest()).thenReturn(taskRequest);
        RedshiftLoadOperatorFactory.RedshiftLoadOperator operator = (RedshiftLoadOperatorFactory.RedshiftLoadOperator) operatorFactory.newOperator(operatorContext);
        assertThat(operator, is(instanceOf(RedshiftLoadOperatorFactory.RedshiftLoadOperator.class)));
    }

    @Test
    public void createCopyConfigWithSimpleCsvOption()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "csv", ""
        );
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_table\" FROM 's3://my-bucket/my-path'\n" +
                "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                "CSV\n"));
    }

    @Test
    public void createCopyConfigWithSimpleCsvOptionWithQuotesInParams()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_\"table",
                "from", "s3://my-'bucket/my-''path",
                "csv", "'"
        );
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_\"\"table\" FROM 's3://my-''bucket/my-''''path'\n" +
                "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                "CSV QUOTE ''''\n"));
    }

    @Test
    public void createCopyConfigWithSimpleCsvOptionWithQuotesInParamsWithMaskingCredentials()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_\"table",
                "from", "s3://my-'bucket/my-''path",
                "csv", "'"
        );
        String sql = getCopyConfig(configInput, true);
        assertThat(sql,
                is("COPY \"my_\"\"table\" FROM 's3://my-''bucket/my-''''path'\n" +
                "CREDENTIALS 'aws_access_key_id=********;aws_secret_access_key=********'\n" +
                "CSV QUOTE ''''\n"));
    }

    @Test
    public void createCopyConfigWithComplicatedCsvOption()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.<String, Object>builder()
                .put("table", "my_table")
                .put("column_list", "name, age, email")
                .put("from", "s3://my-bucket/my-path")
                .put("readratio", 123)
                .put("manifest", true)
                .put("encrypted", true)
                .put("region", "us-east-1")
                .put("csv", "`")
                .put("delimiter", "$")
                .put("gzip", true)
                .put("acceptanydate", true)
                .put("acceptinvchars", "&")
                .put("blanksasnull", true)
                .put("dateformat", "yyyy-MM-dd")
                .put("emptyasnull", true)
                .put("encoding", "UTF16")
                .put("escape", false)
                .put("explicit_ids", true)
                .put("fillrecord", true)
                .put("ignoreblanklines", true)
                .put("ignoreheader", 42)
                .put("null_as", "nULl")
                .put("removequotes", false)
                .put("roundec", true)
                .put("timeformat", "YYYY-MM-DD HH:MI:SS")
                .put("trimblanks", true)
                .put("truncatecolumns", true)
                .put("comprows", 12)
                .put("compupdate", "ON")
                .put("maxerror", 34)
                .put("noload", true)
                .put("statupdate", "on")
                .build();
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_table\" FROM 's3://my-bucket/my-path'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "READRATIO 123\n" +
                        "MANIFEST\n" +
                        "ENCRYPTED\n" +
                        "REGION 'us-east-1'\n" +
                        "CSV QUOTE '`'\n" +
                        "DELIMITER '$'\n" +
                        "GZIP\n" +
                        "ACCEPTANYDATE\n" +
                        "ACCEPTINVCHARS '&'\n" +
                        "BLANKSASNULL\n" +
                        "DATEFORMAT 'yyyy-MM-dd'\n" +
                        "EMPTYASNULL\n" +
                        "ENCODING UTF16\n" +
                        "EXPLICIT_IDS\n" +
                        "FILLRECORD\n" +
                        "IGNOREBLANKLINES\n" +
                        "IGNOREHEADER 42\n" +
                        "NULL AS 'nULl'\n" +
                        "ROUNDEC\n" +
                        "TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'\n" +
                        "TRIMBLANKS\n" +
                        "TRUNCATECOLUMNS\n" +
                        "COMPROWS 12\n" +
                        "COMPUPDATE ON\n" +
                        "MAXERROR 34\n" +
                        "NOLOAD\n" +
                        "STATUPDATE on\n"));
    }

    @Test
    public void createCopyConfigWithComplicatedDelimiterOption()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.<String, Object>builder()
                .put("table", "my_table")
                .put("column_list", "name, age, email")
                .put("from", "s3://my-bucket/my-path")
                .put("readratio", 123)
                .put("manifest", false)
                .put("encrypted", false)
                .put("region", "us-east-1")
                .put("delimiter", "$")
                .put("bzip2", true)
                .put("acceptanydate", false)
                .put("acceptinvchars", "&")
                .put("blanksasnull", false)
                .put("dateformat", "yyyy-MM-dd")
                .put("emptyasnull", false)
                .put("encoding", "UTF16")
                .put("escape", true)
                .put("explicit_ids", false)
                .put("fillrecord", false)
                .put("ignoreblanklines", false)
                .put("ignoreheader", 42)
                .put("null_as", "nULl")
                .put("removequotes", true)
                .put("roundec", false)
                .put("timeformat", "YYYY-MM-DD HH:MI:SS")
                .put("trimblanks", false)
                .put("truncatecolumns", false)
                .put("comprows", 12)
                .put("compupdate", "ON")
                .put("maxerror", 34)
                .put("noload", false)
                .put("statupdate", "OFF")
                .build();
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_table\" FROM 's3://my-bucket/my-path'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "READRATIO 123\n" +
                        "REGION 'us-east-1'\n" +
                        "DELIMITER '$'\n" +
                        "BZIP2\n" +
                        "ACCEPTINVCHARS '&'\n" +
                        "DATEFORMAT 'yyyy-MM-dd'\n" +
                        "ENCODING UTF16\n" +
                        "ESCAPE\n" +
                        "IGNOREHEADER 42\n" +
                        "NULL AS 'nULl'\n" +
                        "REMOVEQUOTES\n" +
                        "TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'\n" +
                        "COMPROWS 12\n" +
                        "COMPUPDATE ON\n" +
                        "MAXERROR 34\n" +
                        "STATUPDATE OFF\n"));
    }

    @Test
    public void createCopyConfigWithSimpleFixedwidthOption()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "fixedwidth", "col1:11,col2:222,col3:333,col4:4444",
                "lzop", true
        );
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_table\" FROM 's3://my-bucket/my-path'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "FIXEDWIDTH 'col1:11,col2:222,col3:333,col4:4444'\n" +
                        "LZOP\n"
                ));
    }

    @Test
    public void createCopyConfigWithSimpleJsonOptionWithJsonfilepath()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "json", "s3://source-bucket/jsonpaths_file"
        );
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_table\" FROM 's3://my-bucket/my-path'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "FORMAT AS JSON 's3://source-bucket/jsonpaths_file'\n"));
    }

    @Test
    public void createCopyConfigWithSimpleAvroOption()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "avro", "auto"
        );
        String sql = getCopyConfig(configInput);
        assertThat(sql,
                is("COPY \"my_table\" FROM 's3://my-bucket/my-path'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "FORMAT AS AVRO 'auto'\n"));
    }

    @Test
    public void invalidStatupdate()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "csv", "",
                "statupdate", "YES"
        );

        this.thrown.expect(ConfigException.class);
        getCopyConfig(configInput);
        assertTrue(false);
    }

    @Test
    public void invalidCompupdate()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "from", "s3://my-bucket/my-path",
                "csv", "",
                "compupdate", "YES"
        );

        this.thrown.expect(ConfigException.class);
        getCopyConfig(configInput);
        assertTrue(false);
    }

    @Test
    public void noTableName()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "from", "s3://my-bucket/my-path",
                "csv", ""
        );

        this.thrown.expect(ConfigException.class);
        getCopyConfig(configInput);
        assertTrue(false);
    }

    @Test
    public void noFrom()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table", "my_table",
                "csv", ""
        );

        this.thrown.expect(ConfigException.class);
        getCopyConfig(configInput);
        assertTrue(false);
    }

    @Test
    public void wrongCombinationWithCsv()
            throws IOException
    {
        ImmutableMap<String, Object> kvs = ImmutableMap.<String, Object>builder()
                .put("removequotes", true)
                .put("escape", true)
                .build();
        for (Map.Entry<String, Object> kv : kvs.entrySet()) {
            Map<String, Object> configInput = ImmutableMap.of(
                    "table", "my_table",
                    "from", "s3://my-bucket/my-path",
                    "csv", "",
                    kv.getKey(), kv.getValue()
            );

            this.thrown.expect(ConfigException.class);
            getCopyConfig(configInput);
            assertTrue(kv.toString(), false);
        }
    }

    @Test
    public void wrongCombinationWithDelimiter()
            throws IOException
    {
        ImmutableMap<String, Object> kvs = ImmutableMap.<String, Object>builder()
                .put("csv", "")
                .put("delimiter", "'")
                .put("json", "auto")
                .put("avro", "auto")
                .build();
        for (Map.Entry<String, Object> kv : kvs.entrySet()) {
            Map<String, Object> configInput = ImmutableMap.of(
                    "table", "my_table",
                    "from", "s3://my-bucket/my-path",
                    "fixedwidth", "col0:42",
                    kv.getKey(), kv.getValue()
            );

            this.thrown.expect(ConfigException.class);
            getCopyConfig(configInput);
            assertTrue(kv.toString(), false);
        }
    }

    @Test
    public void wrongCombinationWithJson()
            throws IOException
    {
        ImmutableMap<String, Object> kvs = ImmutableMap.<String, Object>builder()
                .put("csv", "")
                .put("delimiter", "'")
                .put("avro", "auto")
                .put("escape", true)
                .put("filerecord", true)
                .put("null_as", "X")
                .put("readratio", 150)
                .put("removequotes", true)
                .build();
        for (Map.Entry<String, Object> kv : kvs.entrySet()) {
            Map<String, Object> configInput = ImmutableMap.of(
                    "table", "my_table",
                    "from", "s3://my-bucket/my-path",
                    "json", "auto",
                    kv.getKey(), kv.getValue()
            );

            this.thrown.expect(ConfigException.class);
            getCopyConfig(configInput);
            assertTrue(kv.toString(), false);
        }
    }

    @Test
    public void wrongCombinationWithAvro()
            throws IOException
    {
        ImmutableMap<String, Object> kvs = ImmutableMap.<String, Object>builder()
                .put("csv", "")
                .put("delimiter", "'")
                .put("json", "auto")
                .build();
        for (Map.Entry<String, Object> kv : kvs.entrySet()) {
            Map<String, Object> configInput = ImmutableMap.of(
                    "table", "my_table",
                    "from", "s3://my-bucket/my-path",
                    "avro", "auto",
                    kv.getKey(), kv.getValue()
            );

            this.thrown.expect(ConfigException.class);
            getCopyConfig(configInput);
            assertTrue(kv.toString(), false);
        }
    }
}
