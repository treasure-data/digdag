package io.digdag.cli.profile;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigModule;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.ImmutableArchivedTask;
import io.digdag.core.session.ImmutableSession;
import io.digdag.core.session.ImmutableStoredSessionAttemptWithSession;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.core.session.TaskType;
import io.digdag.core.workflow.TaskConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.digdag.client.DigdagClient.objectMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class TasksAnalyzerTest
{
    private static Instant TIME_FROM = Instant.now().minusSeconds(3600);
    private static Instant TIME_TO = Instant.now();

    private static final Config EMPTY_CONFIG = new ConfigFactory(objectMapper()).create();

    private static final int FETCH_ATTEMPTS = 4;
    private static final int PARTITION_SIZE = 2;

    private static final int SITE_1 = 1;
    private static final int SITE_2 = 2;
    private static final int SITE_3 = 3;

    private static final int PROJECT_S1_P10 = 10;
    private static final int PROJECT_S1_P11 = 11;
    private static final int PROJECT_S2_P20 = 20;
    private static final int PROJECT_S3_P30 = 30;

    private static final int SESSION_S1_P10_S100 = 100;
    private static final int SESSION_S1_P11_S110 = 110;
    private static final int SESSION_S2_P20_S200 = 200;
    private static final int SESSION_S3_P30_S300 = 300;
    private static final int SESSION_S3_P30_S301 = 301;

    private static final long ATTEMPT_S1_P10_S100_A1000 = 1000;
    private static final long ATTEMPT_S1_P10_S100_A1001 = 1001;
    private static final long ATTEMPT_S1_P10_S100_A1002 = 1002;
    private static final long ATTEMPT_S1_P11_S110_A1100 = 1100;
    private static final long ATTEMPT_S2_P20_S200_A2000 = 2000;
    private static final long ATTEMPT_S3_P30_S300_A3000 = 3000;
    private static final long ATTEMPT_S3_P30_S301_A3010 = 3010;

    private static final long TASK_S1_P10_S100_A1000_T10000 = 10000;
    private static final long TASK_S1_P10_S100_A1000_T10001 = 10001;
    private static final long TASK_S1_P10_S100_A1001_T10010 = 10010;
    private static final long TASK_S1_P10_S100_A1001_T10011 = 10011;
    private static final long TASK_S1_P10_S100_A1002_T10020 = 10020;
    private static final long TASK_S1_P10_S100_A1002_T10021 = 10021;
    private static final long TASK_S1_P11_S110_A1100_T11000 = 11000;
    private static final long TASK_S1_P11_S110_A1100_T11001 = 11001;
    private static final long TASK_S2_P20_S200_A2000_T20000 = 20000;
    private static final long TASK_S2_P20_S200_A2000_T20001 = 20001;
    private static final long TASK_S3_P30_S300_A3000_T30000 = 30000;
    private static final long TASK_S3_P30_S300_A3000_T30001 = 30001;
    private static final long TASK_S3_P30_S301_A3010_T30100 = 30100;
    private static final long TASK_S3_P30_S301_A3010_T30101 = 30101;

    private static final SessionStore SESSION_STORE_S1 =
            createSessionStore(
                    ImmutableMap.of(
                            ATTEMPT_S1_P10_S100_A1000,
                            createTasks(ATTEMPT_S1_P10_S100_A1000, "wf_s1_p10_s100", TASK_S1_P10_S100_A1000_T10000, TASK_S1_P10_S100_A1000_T10001),

                            ATTEMPT_S1_P10_S100_A1001,
                            createTasks(ATTEMPT_S1_P10_S100_A1001, "wf_s1_p10_s100", TASK_S1_P10_S100_A1001_T10010, TASK_S1_P10_S100_A1001_T10011),

                            ATTEMPT_S1_P10_S100_A1002,
                            createTasks(ATTEMPT_S1_P10_S100_A1002, "wf_s1_p10_s100", TASK_S1_P10_S100_A1002_T10020, TASK_S1_P10_S100_A1002_T10021),

                            ATTEMPT_S1_P11_S110_A1100,
                            createTasks(ATTEMPT_S1_P11_S110_A1100, "wf_s1_p11_s110", TASK_S1_P11_S110_A1100_T11000, TASK_S1_P11_S110_A1100_T11001)
                    )
            );
    private static final SessionStore SESSION_STORE_S2 =
            createSessionStore(
                    ImmutableMap.of(
                            ATTEMPT_S2_P20_S200_A2000,
                            createTasks(ATTEMPT_S2_P20_S200_A2000, "wf_s2_p20_s200", TASK_S2_P20_S200_A2000_T20000, TASK_S2_P20_S200_A2000_T20001)
                    )
            );
    private static final SessionStore SESSION_STORE_S3 =
            createSessionStore(
                    ImmutableMap.of(
                            ATTEMPT_S3_P30_S300_A3000,
                            createTasks(ATTEMPT_S3_P30_S300_A3000, "wf_s3_p30_s300", TASK_S3_P30_S300_A3000_T30000, TASK_S3_P30_S300_A3000_T30001),

                            ATTEMPT_S3_P30_S301_A3010,
                            createTasks(ATTEMPT_S3_P30_S301_A3010, "wf_s3_p30_s301", TASK_S3_P30_S301_A3010_T30100, TASK_S3_P30_S301_A3010_T30101)
                    )
            );

    @Mock TransactionManager transactionManager;
    @Mock SessionStoreManager sessionStoreManager;

    StoredSessionAttemptWithSession attempt_1_p10_s100_a1000 =
            createAttempt(SITE_1, PROJECT_S1_P10, "wf_s1_p10", SESSION_S1_P10_S100, ATTEMPT_S1_P10_S100_A1000, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    StoredSessionAttemptWithSession attempt_2_p20_s200_a2000 =
            createAttempt(SITE_2, PROJECT_S2_P20, "wf_s2_p20", SESSION_S2_P20_S200, ATTEMPT_S2_P20_S200_A2000, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    StoredSessionAttemptWithSession attempt_3_p30_s300_a3000 =
            createAttempt(SITE_3, PROJECT_S2_P20, "wf_s3_p30", SESSION_S3_P30_S300, ATTEMPT_S3_P30_S300_A3000, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE));

    StoredSessionAttemptWithSession attempt_1_p11_s110_a1100 =
            createAttempt(SITE_1, PROJECT_S1_P11, "wf_s1_p11", SESSION_S1_P11_S110, ATTEMPT_S1_P11_S110_A1100, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    StoredSessionAttemptWithSession attempt_3_p30_s301_a3010 =
            createAttempt(SITE_3, PROJECT_S3_P30, "wf_s3_p30", SESSION_S3_P30_S301, ATTEMPT_S3_P30_S301_A3010, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    StoredSessionAttemptWithSession attempt_1_p10_s100_a1001 =
            createAttempt(SITE_1, PROJECT_S1_P10, "wf_s1_p10", SESSION_S1_P10_S100, ATTEMPT_S1_P10_S100_A1001, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    StoredSessionAttemptWithSession attempt_1_p10_s100_a1002 =
            createAttempt(SITE_1, PROJECT_S1_P10, "wf_s1_p10", SESSION_S1_P10_S100, ATTEMPT_S1_P10_S100_A1002, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    private static List<ArchivedTask> createTasks(long attemptId, String rootTaskName, long rootTaskId, long childTaskId)
    {
        Instant startedAt = TIME_FROM.plusSeconds(600);

        // Root task which took 5 seconds
        ArchivedTask rootTask = ImmutableArchivedTask.builder()
                .attemptId(attemptId)
                .id(rootTaskId)
                .parentId(Optional.absent())
                .config(TaskConfig.validate(EMPTY_CONFIG))
                .subtaskConfig(EMPTY_CONFIG)
                .stateParams(EMPTY_CONFIG)
                .storeParams(EMPTY_CONFIG)
                .exportParams(EMPTY_CONFIG)
                .taskType(TaskType.of(TaskType.GROUPING_ONLY))
                .fullName(rootTaskName)
                .error(EMPTY_CONFIG)
                .state(TaskStateCode.SUCCESS)
                .stateFlags(TaskStateFlags.empty())
                .startedAt(startedAt)
                .updatedAt(startedAt.plusSeconds(5))
                .build();

        // Child task which started with 1 second delay.
        // This means the child task itself takes 4 seconds.
        ArchivedTask childTask = ImmutableArchivedTask.builder()
                .attemptId(attemptId)
                .id(childTaskId)
                .parentId(rootTaskId)
                .config(TaskConfig.validate(EMPTY_CONFIG))
                .subtaskConfig(EMPTY_CONFIG)
                .stateParams(EMPTY_CONFIG)
                .storeParams(EMPTY_CONFIG)
                .exportParams(EMPTY_CONFIG)
                .taskType(TaskType.of(0))
                .fullName(rootTaskName + "+child")
                .error(EMPTY_CONFIG)
                .state(TaskStateCode.SUCCESS)
                .stateFlags(TaskStateFlags.empty())
                .startedAt(startedAt.plusSeconds(1))
                .updatedAt(startedAt.plusSeconds(5))
                .build();

        return ImmutableList.of(rootTask, childTask);
    }

    private static SessionStore createSessionStore(Map<Long, List<ArchivedTask>> attemptAndTasksMap)
    {
        SessionStore sessionStore = mock(SessionStore.class);
        for (Map.Entry<Long, List<ArchivedTask>> attemptAndTasks : attemptAndTasksMap.entrySet()) {
            doReturn(attemptAndTasks.getValue())
                    .when(sessionStore)
                    .getTasksOfAttempt(eq(attemptAndTasks.getKey()));
        }
        return sessionStore;
    }

    private static StoredSessionAttemptWithSession createAttempt(
            int siteId,
            int projectId,
            String workflowName,
            int sessionId,
            long attemptId,
            AttemptStateFlags stateFlags)
    {
        ImmutableSession session = ImmutableSession.builder()
                .projectId(projectId)
                .sessionTime(Instant.now().minusSeconds(1800))
                .workflowName(workflowName)
                .build();

        return ImmutableStoredSessionAttemptWithSession.builder()
                .siteId(siteId)
                .session(session)
                .sessionId(sessionId)
                .sessionUuid(UUID.randomUUID())
                .id(attemptId)
                .createdAt(Instant.now().minusSeconds(1200))
                .finishedAt(Instant.now().minusSeconds(600))
                .index((int) attemptId)
                .params(EMPTY_CONFIG)
                .stateFlags(stateFlags)
                .timeZone(ZoneId.systemDefault())
                .workflowDefinitionId(42)
                .build();
    }

    @Before
    public void setUp()
    {
        doAnswer((invocation) -> {
            @SuppressWarnings("unchecked")
            TransactionManager.SupplierInTransaction<Void, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func =
                    invocation.getArgumentAt(0, TransactionManager.SupplierInTransaction.class);
            return func.get();
        }).when(transactionManager).begin(any());

        // Fetch the first 4 attempts
        doReturn(ImmutableList.of(
                attempt_1_p10_s100_a1000,
                attempt_1_p11_s110_a1100,
                attempt_2_p20_s200_a2000,
                attempt_3_p30_s300_a3000
        )).when(sessionStoreManager).findFinishedAttemptsWithSessions(
                eq(TIME_FROM), eq(TIME_TO), eq(0L), eq(FETCH_ATTEMPTS));

        // Fetch the rest of attempts
        doReturn(ImmutableList.of(
                attempt_3_p30_s301_a3010,
                attempt_1_p10_s100_a1001,
                attempt_1_p10_s100_a1002
        )).when(sessionStoreManager).findFinishedAttemptsWithSessions(
                eq(TIME_FROM), eq(TIME_TO), eq(attempt_3_p30_s300_a3000.getId()), eq(FETCH_ATTEMPTS));

        doReturn(SESSION_STORE_S1).when(sessionStoreManager).getSessionStore(eq(SITE_1));
        doReturn(SESSION_STORE_S2).when(sessionStoreManager).getSessionStore(eq(SITE_2));
        doReturn(SESSION_STORE_S3).when(sessionStoreManager).getSessionStore(eq(SITE_3));
    }

    public class MockDatabaseModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {
            binder.bind(TransactionManager.class).toInstance(transactionManager);
            binder.bind(SessionStoreManager.class).toInstance(sessionStoreManager);
        }
    }

    @Test
    public void run()
            throws IOException
    {
        ConfigElement configElement = ConfigElement.empty();

        Injector injector = Guice.createInjector(
                new ObjectMapperModule()
                        .registerModule(new GuavaModule())
                        .registerModule(new JacksonTimeModule()),
                new MockDatabaseModule(),
                new ConfigModule(),
                (binder) -> {
                    binder.bind(ConfigElement.class).toInstance(configElement);
                    binder.bind(Config.class).toProvider(DigdagEmbed.SystemConfigProvider.class);
                }
        );
        TaskAnalyzer taskAnalyzer = new TaskAnalyzer(injector);

        WholeTasksSummary tasksSummary = taskAnalyzer.run(
                TIME_FROM,
                TIME_TO,
                FETCH_ATTEMPTS,
                PARTITION_SIZE,
                10);

        assertEquals(7, tasksSummary.overall.attempts);
        // these counts doesn't contain root tasks
        assertEquals(7, tasksSummary.overall.totalTasks);
        assertEquals(7, tasksSummary.overall.totalRunTasks);
        assertEquals(7, tasksSummary.overall.totalSuccessTasks);
        assertEquals(0, tasksSummary.overall.totalErrorTasks);

        assertNotNull(tasksSummary.overall.mostDelayedTask);

        assertEquals(1000, tasksSummary.overall.startDelayMillis.min().longValue());
        assertEquals(1000, tasksSummary.overall.startDelayMillis.max().longValue());
        assertEquals(1000, tasksSummary.overall.startDelayMillis.mean().longValue());
        assertEquals(0, tasksSummary.overall.startDelayMillis.stdDev().longValue());

        assertEquals(4000, tasksSummary.overall.execDurationMillis.min().longValue());
        assertEquals(4000, tasksSummary.overall.execDurationMillis.max().longValue());
        assertEquals(4000, tasksSummary.overall.execDurationMillis.mean().longValue());
        assertEquals(0, tasksSummary.overall.execDurationMillis.stdDev().longValue());
    }
}
