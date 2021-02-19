package io.digdag.profiler;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.collect.ImmutableList;
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
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.ImmutableSession;
import io.digdag.core.session.ImmutableStoredSessionAttemptWithSession;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static io.digdag.client.DigdagClient.objectMapper;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TaskAnalyzerTest
{
    private static final int FETCH_ATTEMPTS = 4;
    private static final int PARTITION_SIZE = 2;
    private static final Config EMPTY_CONFIG = new ConfigFactory(objectMapper()).create();

    @Mock TransactionManager transactionManager;
    @Mock ProjectStoreManager projectStoreManager;
    @Mock SessionStoreManager sessionStoreManager;
    Instant timeFrom = Instant.now().minusSeconds(3600);
    Instant timeTo = Instant.now();

    StoredSessionAttemptWithSession attempt_1_p10_s100_a1000 =
            createAttempt(1, 10, "wf_s1_p10", 100, 1000, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));
    StoredSessionAttemptWithSession attempt_2_p20_s200_a2000 =
            createAttempt(2, 20, "wf_s2_p20", 200, 2000, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));
    StoredSessionAttemptWithSession attempt_3_p30_s300_a3000 =
            createAttempt(3, 30, "wf_s3_p30", 300, 3000, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE));
    StoredSessionAttemptWithSession attempt_1_p11_s110_a1100 =
            createAttempt(1, 11, "wf_s1_p11", 110, 1100, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));
    StoredSessionAttemptWithSession attempt_3_p30_s301_a3010 =
            createAttempt(3, 30, "wf_s3_p30", 301, 3010, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));
    StoredSessionAttemptWithSession attempt_1_p10_s100_a1001 =
            createAttempt(1, 10, "wf_s1_p10", 100, 1001, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));
    StoredSessionAttemptWithSession attempt_1_p10_s100_a1002 =
            createAttempt(1, 10, "wf_s1_p10", 100, 1002, AttemptStateFlags.of(AttemptStateFlags.DONE_CODE | AttemptStateFlags.SUCCESS_CODE));

    private StoredSessionAttemptWithSession createAttempt(
            int siteId,
            int projectId,
            String workflowName,
            int sessionId,
            int attemptId,
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
                .index(attemptId)
                .params(EMPTY_CONFIG)
                .stateFlags(stateFlags)
                .timeZone(ZoneId.systemDefault())
                .workflowDefinitionId(42)
                .build();
    }


    @Before
    public void setUp()
            throws Exception
    {
        doAnswer((invocation) -> {
            @SuppressWarnings("unchecked")
            TransactionManager.SupplierInTransaction<Void, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func =
                    invocation.getArgumentAt(0, TransactionManager.SupplierInTransaction.class);
            return func.get();
        }).when(transactionManager).begin(any());

        doReturn(ImmutableList.of(
                attempt_1_p10_s100_a1000,
                attempt_2_p20_s200_a2000,
                attempt_3_p30_s300_a3000,
                attempt_1_p11_s110_a1100
        )).when(sessionStoreManager).findFinishedAttemptsWithSessions(
                eq(timeFrom), eq(timeTo), eq(0L), eq(FETCH_ATTEMPTS));

        doReturn(ImmutableList.of(
                attempt_3_p30_s301_a3010,
                attempt_1_p10_s100_a1001,
                attempt_1_p10_s100_a1002
        )).when(sessionStoreManager).findFinishedAttemptsWithSessions(
                eq(timeFrom), eq(timeTo), eq(attempt_1_p11_s110_a1100.getId()), eq(FETCH_ATTEMPTS));
    }

    @After
    public void tearDown()
            throws Exception
    {
    }

    public class MockDatabaseModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {
            binder.bind(TransactionManager.class).toInstance(transactionManager);
            binder.bind(ProjectStoreManager.class).toInstance(projectStoreManager);
            binder.bind(SessionStoreManager.class).toInstance(sessionStoreManager);
        }
    }

    @Test
    public void run() throws IOException {
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

        taskAnalyzer.run(
                System.out,
                timeFrom,
                timeTo,
                FETCH_ATTEMPTS,
                PARTITION_SIZE,
                10);
    }
}
