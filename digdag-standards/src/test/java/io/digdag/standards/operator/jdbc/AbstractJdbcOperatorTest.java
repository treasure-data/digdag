package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import org.immutables.value.Value;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbstractJdbcOperatorTest
{
    @Mock TaskExecutionContext taskExecutionContext;

    @Before
    public void setUp()
            throws Exception
    {
        when(taskExecutionContext.secrets()).thenReturn(key -> Optional.absent());

    }

    private final JdbcOpTestHelper testHelper = new JdbcOpTestHelper();

    @Value.Immutable
    public abstract static class TestConnectionConfig
            extends AbstractJdbcConnectionConfig
    {
    }

    public static class TestConnection
            extends AbstractJdbcConnection
    {
        public TestConnection(Connection connection)
        {
            super(connection);
        }

        @Override
        public void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler)
                throws NotReadOnlyException
        {
        }

        @Override
        public TransactionHelper getStrictTransactionHelper(String statusTableName, Duration cleanupDuration)
        {
            return null;
        }
    }

    public static class TestOperator
        extends AbstractJdbcOperator<TestConnectionConfig>
    {
        public TestOperator(Path workspacePath, TaskRequest request, TemplateEngine templateEngine)
        {
            super(workspacePath, request, templateEngine);
        }

        @Override
        protected TestConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return null;
        }

        @Override
        protected JdbcConnection connect(TestConnectionConfig connectionConfig)
        {
            return Mockito.mock(TestConnection.class);
        }
    }

    private TestOperator getJdbcOperator(Map<String, Object> configInput, Optional<Map<String, Object>> lastState)
            throws IOException
    {
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, lastState);
        TemplateEngine templateEngine = testHelper.injector().getInstance(TemplateEngine.class);
        return Mockito.spy(new TestOperator(testHelper.workpath(), taskRequest, templateEngine));
    }

    private void runTaskReadOnly(Map<String, Object> configInput, String sql)
            throws IOException, NotReadOnlyException
    {
        TestOperator operator = getJdbcOperator(configInput, Optional.absent());

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);

        operator.runTask(taskExecutionContext);
        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).executeReadOnlyQuery(eq(sql), anyObject());
    }

    private UUID runTaskWithoutQueryId(Map<String, Object> configInput)
            throws IOException
    {
        TestOperator operator = getJdbcOperator(configInput, Optional.absent());

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);

        UUID queryId = null;
        try {
            operator.runTask(taskExecutionContext);
            assertTrue(false);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval(), is(Optional.of(0)));
            Config config = e.getStateParams(testHelper.getConfigFactory()).get();
            queryId = config.get("queryId", UUID.class);
        }
        return queryId;
    }

    private void runTaskWithQueryId(TestOperator operator)
    {
        TaskResult taskResult = operator.runTask(taskExecutionContext);
        assertThat(taskResult, is(notNullValue()));
        assertThat(taskResult.getStoreParams().has("pollInterval"), is(false));
    }

    @Test
    public void selectAndDownload()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "download_file", "result.csv",
                "query", sql
        );

        runTaskReadOnly(configInput, sql);
    }

    @Test
    public void createTable()
            throws IOException, LockConflictException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "create_table", "desttbl",
                "query", sql
        );

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildCreateTableStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare();
        verify(txHelper).lockedTransaction(eq(queryId), anyObject());
        verify(txHelper).cleanup();
    }

    @Test
    public void createTableWithSchema()
            throws IOException, LockConflictException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.<String, Object>builder().
                put("host", "foobar.com").
                put("user", "testuser").
                put("database", "testdb").
                put("schema", "myschema").
                put("create_table", "desttbl").
                put("query", sql).build();

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildCreateTableStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare();
        verify(txHelper).lockedTransaction(eq(queryId), anyObject());
        verify(txHelper).cleanup();
    }

    @Test
    public void createTableWithoutStrictTx()
            throws IOException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.<String, Object>builder().
                put("host", "foobar.com").
                put("user", "testuser").
                put("database", "testdb").
                put("strict_transaction", "false").
                put("create_table", "desttbl").
                put("query", sql).build();

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildCreateTableStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
    }

    @Test
    public void insertInto()
            throws IOException, LockConflictException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "insert_into", "desttbl",
                "query", sql
        );

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildInsertStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare();
        verify(txHelper).lockedTransaction(eq(queryId), anyObject());
        verify(txHelper).cleanup();
    }

    @Test
    public void insertIntoWithCustomStatusTableAndDuration()
            throws IOException, LockConflictException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.<String, Object>builder().
                put("host", "foobar.com").
                put("user", "testuser").
                put("database", "testdb").
                put("insert_into", "desttbl").
                put("status_table", "___my_status_table").
                put("status_table_cleanup", "48h").
                put("query", sql).build();

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        TestConnection connection = Mockito.mock(TestConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq("___my_status_table"), eq(Duration.ofHours(48)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildInsertStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare();
        verify(txHelper).lockedTransaction(eq(queryId), anyObject());
        verify(txHelper).cleanup();
    }

    @Test
    public void createTableWithLockConflict()
            throws IOException, LockConflictException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "create_table", "desttbl",
                "query", sql
        );

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        {
            TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

            TestConnection connection = Mockito.mock(TestConnection.class);
            when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
            TransactionHelper txHelper = mock(TransactionHelper.class);
            when(connection.getStrictTransactionHelper(eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);
            when(txHelper.lockedTransaction(eq(queryId), anyObject())).thenThrow(new LockConflictException("foo bar"));

            try {
                operator.runTask(taskExecutionContext);
                assertTrue(false);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(1)));
                assertThat(e.getStateParams(testHelper.getConfigFactory()).get().get("pollInterval", Integer.class), is(2));
            }
        }

        {
            TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId, "pollInterval", 2)));

            TestConnection connection = Mockito.mock(TestConnection.class);
            when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
            TransactionHelper txHelper = mock(TransactionHelper.class);
            when(connection.getStrictTransactionHelper(eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);
            when(txHelper.lockedTransaction(eq(queryId), anyObject())).thenThrow(new LockConflictException("foo bar"));

            try {
                operator.runTask(taskExecutionContext);
                assertTrue(false);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(2)));
                assertThat(e.getStateParams(testHelper.getConfigFactory()).get().get("pollInterval", Integer.class), is(4));
            }
        }

        {
            TestOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId, "pollInterval", 1024)));

            TestConnection connection = Mockito.mock(TestConnection.class);
            when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
            TransactionHelper txHelper = mock(TransactionHelper.class);
            when(connection.getStrictTransactionHelper(eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);
            when(txHelper.lockedTransaction(eq(queryId), anyObject())).thenThrow(new LockConflictException("foo bar"));

            try {
                operator.runTask(taskExecutionContext);
                assertTrue(false);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(1024)));
                assertThat(e.getStateParams(testHelper.getConfigFactory()).get().get("pollInterval", Integer.class), is(1200));
            }
        }
    }
}