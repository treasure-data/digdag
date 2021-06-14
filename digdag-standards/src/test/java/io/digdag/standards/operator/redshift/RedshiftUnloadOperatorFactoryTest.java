package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
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
import java.util.UUID;

import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedshiftUnloadOperatorFactoryTest
{
    private JdbcOpTestHelper testHelper = new JdbcOpTestHelper();
    private RedshiftUnloadOperatorFactory operatorFactory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp()
    {
        operatorFactory = newOperatorFactory(RedshiftUnloadOperatorFactory.class);
    }

    @Test
    public void getType()
    {
        assertThat(operatorFactory.getType(), is("redshift_unload"));
    }

    private String getUnloadConfig(Map<String, Object> configInput, String queryId)
            throws IOException
    {
        return getUnloadConfig(configInput, queryId, false);
    }

    private String getUnloadConfig(Map<String, Object> configInput, String queryId, boolean maskConfig)
            throws IOException
    {
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        OperatorContext operatorContext = mock(OperatorContext.class);
        when(operatorContext.getProjectPath()).thenReturn(testHelper.projectPath());
        when(operatorContext.getTaskRequest()).thenReturn(taskRequest);
        RedshiftUnloadOperatorFactory.RedshiftUnloadOperator operator = (RedshiftUnloadOperatorFactory.RedshiftUnloadOperator) operatorFactory.newOperator(operatorContext);
        assertThat(operator, is(instanceOf(RedshiftUnloadOperatorFactory.RedshiftUnloadOperator.class)));

        AWSSessionCredentials credentials = mock(AWSSessionCredentials.class);
        when(credentials.getAWSAccessKeyId()).thenReturn("my-access-key-id");
        when(credentials.getAWSSecretKey()).thenReturn("my-secret-access-key");

        RedshiftConnection.UnloadConfig unloadConfig = operator.createUnloadConfig(testHelper.createConfig(configInput), credentials, queryId);

        Connection connection = mock(Connection.class);

        RedshiftConnection redshiftConnection = new RedshiftConnection(connection);

        return redshiftConnection.buildUnloadStatement(unloadConfig, maskConfig);
    }

    @Test
    public void newOperator()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "query", "select * from users",
                "to", "s3://my-bucket/my-path"
        );
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        OperatorContext operatorContext = mock(OperatorContext.class);
        when(operatorContext.getProjectPath()).thenReturn(testHelper.projectPath());
        when(operatorContext.getTaskRequest()).thenReturn(taskRequest);
        RedshiftUnloadOperatorFactory.RedshiftUnloadOperator operator = (RedshiftUnloadOperatorFactory.RedshiftUnloadOperator) operatorFactory.newOperator(operatorContext);
        assertThat(operator, is(instanceOf(RedshiftUnloadOperatorFactory.RedshiftUnloadOperator.class)));
    }

    @Test
    public void createUnloadConfigWithSimpleOption()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "query", "select * from users",
                "to", "s3://my-bucket/my-path"
        );
        String queryId = UUID.randomUUID().toString();
        String sql = getUnloadConfig(configInput, queryId);
        assertThat(sql,
                is("UNLOAD ('select * from users') TO 's3://my-bucket/my-path/" + queryId + "_'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n"));
    }

    @Test
    public void createUnloadConfigWithSimpleOptionWithMaskingCredentials()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "query", "select * from users",
                "to", "s3://my-bucket/my-path"
        );
        String queryId = UUID.randomUUID().toString();
        String sql = getUnloadConfig(configInput, queryId, true);
        assertThat(sql,
                is("UNLOAD ('select * from users') TO 's3://my-bucket/my-path/" + queryId + "_'\n" +
                        "CREDENTIALS 'aws_access_key_id=********;aws_secret_access_key=********'\n"));
    }

    @Test
    public void createUnloadConfigWithFixedWidth()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "query", "select * from users",
                "to", "s3://my-bucket/my-path",
                "fixedwidth", "col1:11,col2:222,col3:333,col4:4444",
                "gzip", true
        );
        String queryId = UUID.randomUUID().toString();
        String sql = getUnloadConfig(configInput, queryId);
        assertThat(sql,
                is("UNLOAD ('select * from users') TO 's3://my-bucket/my-path/" + queryId + "_'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "FIXEDWIDTH 'col1:11,col2:222,col3:333,col4:4444'\n" +
                        "GZIP\n"
                ));
    }

    @Test
    public void createUnloadConfigWithParallelOn()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "query", "select * from users",
                "to", "s3://my-bucket/my-path",
                "fixedwidth", "col1:11,col2:222,col3:333,col4:4444",
                "gzip", true,
                "parallel", "ON"
        );
        String queryId = UUID.randomUUID().toString();
        String sql = getUnloadConfig(configInput, queryId);
        assertThat(sql,
                is("UNLOAD ('select * from users') TO 's3://my-bucket/my-path/" + queryId + "_'\n" +
                        "CREDENTIALS 'aws_access_key_id=my-access-key-id;aws_secret_access_key=my-secret-access-key'\n" +
                        "FIXEDWIDTH 'col1:11,col2:222,col3:333,col4:4444'\n" +
                        "GZIP\n" +
                        "PARALLEL ON\n"
                ));
    }
}