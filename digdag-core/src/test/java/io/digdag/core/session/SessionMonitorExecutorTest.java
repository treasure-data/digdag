package io.digdag.core.session;

import com.google.inject.Guice;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.ErrorReporter;
import io.digdag.core.Limits;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ModelValidationException;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.metrics.DigdagMetrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SessionMonitorExecutorTest
{
    @Mock
    ConfigFactory configFactory;
    @Mock
    SessionStoreManager sessionStoreManager;
    @Mock
    TransactionManager transactionManager;
    @Mock
    WorkflowExecutor workflowExecutor;
    @Mock
    Limits limits;
    @Mock
    ErrorReporter errorReporter;
    @Mock
    DigdagMetrics digdagMetrics;

    private SessionMonitorExecutor sessionMonitorExecutor;

    @Before
    public void setUp()
    {
        // Make io.digdag.core.database.TransactionManager#begin execute a passed function
        doAnswer(answer -> {
            TransactionManager.SupplierInTransaction<Void, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func =
                    (TransactionManager.SupplierInTransaction<Void, RuntimeException, RuntimeException, RuntimeException, RuntimeException>) answer.getArguments()[0];
            func.get();
            return null;
        }).when(transactionManager).begin(any());

        // Make io.digdag.core.session.SessionStoreManager#lockReadySessionMonitors execute a passed function
        StoredSessionMonitor sessionMonitor = mock(StoredSessionMonitor.class);
        doAnswer(answer -> {
            SessionStoreManager.SessionMonitorAction action = (SessionStoreManager.SessionMonitorAction) answer.getArguments()[1];
            action.schedule(sessionMonitor);
            return null;
        }).when(sessionStoreManager).lockReadySessionMonitors(any(), any());

        sessionMonitorExecutor = spy(Guice.createInjector(binder -> {
            binder.bind(ConfigFactory.class).toInstance(configFactory);
            binder.bind(SessionStoreManager.class).toInstance(sessionStoreManager);
            binder.bind(TransactionManager.class).toInstance(transactionManager);
            binder.bind(WorkflowExecutor.class).toInstance(workflowExecutor);
            binder.bind(Limits.class).toInstance(limits);
            binder.bind(ErrorReporter.class).toInstance(errorReporter);
            binder.bind(DigdagMetrics.class).toInstance(digdagMetrics);
        }).getInstance(SessionMonitorExecutor.class));

    }

    @Test
    public void handleUnexpectedExceptionInRun()
    {
        doThrow(RuntimeException.class).when(sessionMonitorExecutor).runMonitor(any());

        sessionMonitorExecutor.run();

        verify(errorReporter, times(1)).reportUncaughtError(any(RuntimeException.class));
    }

    @Test
    public void handleConfigExceptionInRun()
    {
        doThrow(ConfigException.class).when(sessionMonitorExecutor).runMonitor(any());

        sessionMonitorExecutor.run();

        verify(errorReporter, times(0)).reportUncaughtError(any(RuntimeException.class));
    }

    @Test
    public void handleModelValidationExceptionInRun()
    {
        doThrow(ModelValidationException.class).when(sessionMonitorExecutor).runMonitor(any());

        sessionMonitorExecutor.run();

        verify(errorReporter, times(0)).reportUncaughtError(any(RuntimeException.class));
    }
}
