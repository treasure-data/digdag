package io.digdag.standards.operator.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import org.immutables.value.Value;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbstractJdbcJobOperatorTest
{
    private final JdbcOpTestHelper testHelper = new JdbcOpTestHelper();

    @Value.Immutable
    public abstract static class TestConnectionConfig
            extends AbstractJdbcConnectionConfig
    {
    }

    public static class TestJobOperator
        extends AbstractJdbcJobOperator<TestConnectionConfig>
    {
        public TestJobOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine)
        {
            super(systemConfig, context, templateEngine);
        }

        @Override
        protected TestConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return null;
        }

        @Override
        protected JdbcConnection connect(TestConnectionConfig connectionConfig)
        {
            return Mockito.mock(JdbcConnection.class);
        }

        @Override
        protected String type()
        {
            return "testop";
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig()
        {
            return context.getSecrets().getSecrets("test");
        }
    }

    private TestJobOperator getJdbcOperator(
            Map<String, Object> configInput,
            Optional<Map<String, Object>> lastState)
            throws IOException
    {
        return getJdbcOperator(Optional.absent(), configInput, lastState);
    }

    private TestJobOperator getJdbcOperator(
            Optional<Config> systemConfig,
            Map<String, Object> configInput,
            Optional<Map<String, Object>> lastState)
            throws IOException
    {
        final TaskRequest taskRequest = testHelper.createTaskRequest(configInput, lastState);
        TemplateEngine templateEngine = testHelper.injector().getInstance(TemplateEngine.class);
        return Mockito.spy(new TestJobOperator(systemConfig.or(new ConfigFactory(DigdagClient.objectMapper()).create()),
                new OperatorContext() {
            @Override
            public Path getProjectPath()
            {
                try {
                    return testHelper.projectPath();
                }
                catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public TaskRequest getTaskRequest()
            {
                return taskRequest;
            }

            @Override
            public PrivilegedVariables getPrivilegedVariables()
            {
                return null;
            }

            @Override
            public SecretProvider getSecrets()
            {
                return (key) -> Optional.absent();
            }
        }, templateEngine));
    }

    private TaskResult runTaskReadOnly(Map<String, Object> configInput, String sql)
            throws IOException, NotReadOnlyException
    {
        return runTaskReadOnly(Optional.absent(), configInput, sql);
    }

    private TaskResult runTaskReadOnly(Optional<Config> systemConfig, Map<String, Object> configInput, String sql)
            throws IOException, NotReadOnlyException
    {
        TestJobOperator operator = getJdbcOperator(systemConfig, configInput, Optional.absent());

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);

        doAnswer(invocationOnMock -> {
            JdbcResultSet jdbcResultSet = mock(JdbcResultSet.class);

            when(jdbcResultSet.getColumnNames())
                    .thenReturn(ImmutableList.of("int", "str", "float"));

            when(jdbcResultSet.next())
                    .thenReturn(ImmutableList.of(42, "foo", 3.14f))
                    .thenReturn(ImmutableList.of(12345, "bar", 0.12f))
                    .thenReturn(null);

            invocationOnMock.getArgumentAt(1, Consumer.class).accept(jdbcResultSet);

            return null;
        }).
        when(connection).executeReadOnlyQuery(anyString(), any(Consumer.class));

        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);

        TaskResult taskResult = operator.runTask();
        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).executeReadOnlyQuery(eq(sql), anyObject());

        return taskResult;
    }

    private UUID runTaskWithoutQueryId(Map<String, Object> configInput)
            throws IOException
    {
        TestJobOperator operator = getJdbcOperator(configInput, Optional.absent());

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);

        UUID queryId = null;
        try {
            operator.runTask();
            assertTrue(false);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval(), is(Optional.of(0)));
            Config config = e.getStateParams(testHelper.getConfigFactory()).get();
            queryId = config.get("queryId", UUID.class);
        }
        return queryId;
    }

    private void runTaskWithQueryId(TestJobOperator operator)
    {
        TaskResult taskResult = operator.runTask();
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
    public void selectAndStoreAllResults()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", "all",
                "query", sql
        );

        TaskResult taskResult = runTaskReadOnly(configInput, sql);
        JsonNode lastResult = taskResult.getStoreParams().getNestedOrGetEmpty("testop").get("last_results", JsonNode.class);

        assertThat(lastResult.size(), is(2));

        JsonNode first = lastResult.get(0);
        assertThat(first.get("int").asInt(), is(42));
        assertThat(first.get("str").asText(), is("foo"));
        assertThat(first.get("float").floatValue(), is(3.14f));

        JsonNode second = lastResult.get(1);
        assertThat(second.get("int").asInt(), is(12345));
        assertThat(second.get("str").asText(), is("bar"));
        assertThat(second.get("float").floatValue(), is(0.12f));
    }

    @Test
    public void selectAndStoreFirstResults()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", "first",
                "query", sql
        );

        TaskResult taskResult = runTaskReadOnly(configInput, sql);
        JsonNode first = taskResult.getStoreParams().getNestedOrGetEmpty("testop").get("last_results", JsonNode.class);

        assertThat(first.size(), is(3));
        assertThat(first.get("int").asInt(), is(42));
        assertThat(first.get("str").asText(), is("foo"));
        assertThat(first.get("float").floatValue(), is(3.14f));
    }

    @Test
    public void selectAndStoreFirstResultsConfiguredByBoolean()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", true,  // true == "first"
                "query", sql
        );

        TaskResult taskResult = runTaskReadOnly(configInput, sql);
        JsonNode first = taskResult.getStoreParams().getNestedOrGetEmpty("testop").get("last_results", JsonNode.class);

        assertThat(first.size(), is(3));
        assertThat(first.get("int").asInt(), is(42));
        assertThat(first.get("str").asText(), is("foo"));
        assertThat(first.get("float").floatValue(), is(3.14f));
    }

    @Test(expected = TaskExecutionException.class)
    public void selectAndStoreAllResultsWithExceedingMaxRows()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", "all",
                "query", sql
        );
        Config systemConfig = new ConfigFactory(DigdagClient.objectMapper()).create();
        systemConfig.set("config.jdbc.max_store_last_results_rows", 1);
        runTaskReadOnly(Optional.of(systemConfig), configInput, sql);
    }

    @Test(expected = TaskExecutionException.class)
    public void selectAndStoreAllResultsWithExceedingMaxColumns()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", "all",
                "query", sql
        );
        Config systemConfig = new ConfigFactory(DigdagClient.objectMapper()).create();
        systemConfig.set("config.testop.max_store_last_results_columns", 2);
        runTaskReadOnly(Optional.of(systemConfig), configInput, sql);
    }

    @Test(expected = TaskExecutionException.class)
    public void selectAndStoreFirstResultsWithExceedingMaxColumns()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", "first",
                "query", sql
        );
        Config systemConfig = new ConfigFactory(DigdagClient.objectMapper()).create();
        systemConfig.set("config.testop.max_store_last_results_columns", 2);
        runTaskReadOnly(Optional.of(systemConfig), configInput, sql);
    }

    @Test(expected = TaskExecutionException.class)
    public void selectAndStoreAllResultsWithExceedingMaxValueSize()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = ImmutableMap.of(
                "host", "foobar.com",
                "user", "testuser",
                "database", "testdb",
                "store_last_results", "all",
                "query", sql
        );
        Config systemConfig = new ConfigFactory(DigdagClient.objectMapper()).create();
        systemConfig.set("config.testop.max_store_last_results_value_size", 2);
        runTaskReadOnly(Optional.of(systemConfig), configInput, sql);
    }

    @Test(expected = ConfigException.class)
    public void selectAndStoreLastResultsWithConflictOption()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = new ImmutableMap.Builder<String, Object>()
                .put("host", "foobar.com")
                .put("user", "testuser")
                .put("database", "testdb")
                .put("store_last_results", "all")
                .put("download_file", "result.csv")
                .put("query", sql)
                .build();

        runTaskReadOnly(configInput, sql);
    }

    @Test
    public void selectAndFalseStoreLastResultsWithoutConflicts()
            throws IOException, NotReadOnlyException
    {
        String sql = "SELECT * FROM users";
        Map<String, Object> configInput = new ImmutableMap.Builder<String, Object>()
                .put("host", "foobar.com")
                .put("user", "testuser")
                .put("database", "testdb")
                .put("store_last_results", false)
                .put("download_file", "result.csv")
                .put("query", sql)
                .build();

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
        TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq(null), eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildCreateTableStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare(queryId);
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
        TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq(null), eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildCreateTableStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare(queryId);
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
        TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
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
        TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq(null), eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildInsertStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare(queryId);
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
                put("status_table_schema", "writable_schema").
                put("status_table", "___my_status_table").
                put("status_table_cleanup", "48h").
                put("query", sql).build();

        // First, just generates a query ID
        UUID queryId = runTaskWithoutQueryId(configInput);

        // Next, executes the query and updates statuses
        TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

        JdbcConnection connection = Mockito.mock(JdbcConnection.class);
        when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
        TransactionHelper txHelper = mock(TransactionHelper.class);
        when(connection.getStrictTransactionHelper(eq("writable_schema"), eq("___my_status_table"), eq(Duration.ofDays(2)))).thenReturn(txHelper);

        runTaskWithQueryId(operator);

        verify(operator).connect(any(TestConnectionConfig.class));
        verify(connection).validateStatement(eq(sql));
        verify(connection).buildInsertStatement(eq(sql), eq(ImmutableTableReference.builder().name("desttbl").build()));
        verify(txHelper).prepare(queryId);
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
            TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId)));

            JdbcConnection connection = Mockito.mock(JdbcConnection.class);
            when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
            TransactionHelper txHelper = mock(TransactionHelper.class);
            when(connection.getStrictTransactionHelper(eq(null), eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);
            when(txHelper.lockedTransaction(eq(queryId), anyObject())).thenThrow(new LockConflictException("foo bar"));

            try {
                operator.runTask();
                assertTrue(false);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(1)));
                assertThat(e.getStateParams(testHelper.getConfigFactory()).get().get("pollInterval", Integer.class), is(2));
            }
        }

        {
            TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId, "pollInterval", 2)));

            JdbcConnection connection = Mockito.mock(JdbcConnection.class);
            when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
            TransactionHelper txHelper = mock(TransactionHelper.class);
            when(connection.getStrictTransactionHelper(eq(null), eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);
            when(txHelper.lockedTransaction(eq(queryId), anyObject())).thenThrow(new LockConflictException("foo bar"));

            try {
                operator.runTask();
                assertTrue(false);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(2)));
                assertThat(e.getStateParams(testHelper.getConfigFactory()).get().get("pollInterval", Integer.class), is(4));
            }
        }

        {
            TestJobOperator operator = getJdbcOperator(configInput, Optional.of(ImmutableMap.of("queryId", queryId, "pollInterval", 1024)));

            JdbcConnection connection = Mockito.mock(JdbcConnection.class);
            when(operator.connect(any(TestConnectionConfig.class))).thenReturn(connection);
            TransactionHelper txHelper = mock(TransactionHelper.class);
            when(connection.getStrictTransactionHelper(eq(null), eq("__digdag_status"), eq(Duration.ofHours(24)))).thenReturn(txHelper);
            when(txHelper.lockedTransaction(eq(queryId), anyObject())).thenThrow(new LockConflictException("foo bar"));

            try {
                operator.runTask();
                assertTrue(false);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(1024)));
                assertThat(e.getStateParams(testHelper.getConfigFactory()).get().get("pollInterval", Integer.class), is(1200));
            }
        }
    }
}
