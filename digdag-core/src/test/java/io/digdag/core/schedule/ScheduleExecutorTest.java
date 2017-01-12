package io.digdag.core.schedule;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;

import static java.time.ZoneOffset.UTC;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleExecutorTest
{
    private static final int SCHEDULE_ID = 13;
    private static final int SITE_ID = 7;
    private static final long WORKFLOW_DEFINITION_ID = 17;

    private static final ConfigFactory CONFIG_FACTORY = new ConfigFactory(DigdagClient.objectMapper());

    @Mock ProjectStoreManager projectStoreManager;
    @Mock ScheduleStoreManager scheduleStoreManager;
    @Mock SchedulerManager schedulerManager;
    @Mock SessionStore sessionStore;
    @Mock SessionStoreManager sessionStoreManager;
    @Mock Scheduler scheduler;
    @Mock ScheduleControlStore scs;
    @Mock StoredSchedule schedule;
    @Mock StoredProject project;
    @Mock StoredWorkflowDefinitionWithProject workflowDefinition;
    @Mock StoredSessionAttemptWithSession attempt;
    @Mock ScheduleTime nextScheduleTime;
    @Mock AttemptBuilder attemptBuilder;
    @Mock WorkflowExecutor workflowExecutor;

    private ScheduleExecutor scheduleExecutor;

    private Instant now;
    private Config workflowConfig;

    @Before
    public void setUp()
            throws Exception
    {
        scheduleExecutor = spy(new ScheduleExecutor(projectStoreManager, scheduleStoreManager, schedulerManager, sessionStoreManager, attemptBuilder, workflowExecutor, CONFIG_FACTORY));

        now = Instant.now();

        when(project.getSiteId()).thenReturn(SITE_ID);

        workflowConfig = CONFIG_FACTORY.create();

        when(workflowDefinition.getTimeZone()).thenReturn(UTC);
        when(workflowDefinition.getId()).thenReturn(WORKFLOW_DEFINITION_ID);
        when(workflowDefinition.getProject()).thenReturn(project);
        when(workflowDefinition.getConfig()).thenReturn(workflowConfig);

        when(scheduler.nextScheduleTime(now)).thenReturn(nextScheduleTime);
        when(schedulerManager.getScheduler(workflowDefinition)).thenReturn(scheduler);

        when(schedule.getId()).thenReturn(SCHEDULE_ID);
        when(schedule.getWorkflowDefinitionId()).thenReturn(WORKFLOW_DEFINITION_ID);
        when(schedule.getNextScheduleTime()).thenReturn(now);
        when(schedule.getNextRunTime()).thenReturn(now);

        when(sessionStoreManager.getSessionStore(SITE_ID)).thenReturn(sessionStore);
        when(projectStoreManager.getWorkflowDetailsById(WORKFLOW_DEFINITION_ID)).thenReturn(workflowDefinition);

        doAnswer(invocation -> {
            ScheduleStoreManager.ScheduleAction func = invocation.getArgumentAt(1, ScheduleStoreManager.ScheduleAction.class);
            func.schedule(scs, schedule);
            return null;
        }).when(scheduleStoreManager).lockReadySchedules(any(Instant.class), any(ScheduleStoreManager.ScheduleAction.class));
    }

    @Test
    public void testSkipOnOvertime()
            throws Exception
    {
        // Enable skip_on_overtime
        workflowConfig.getNestedOrSetEmpty("schedule")
                .set("skip_on_overtime", true)
                .set("daily>", "12:00:00");

        // Indicate that there is an active attempt for this workflow
        when(sessionStore.getActiveAttemptsOfWorkflow(eq(WORKFLOW_DEFINITION_ID), anyInt(), any(Optional.class)))
                .thenReturn(ImmutableList.of(attempt));

        // Run the schedule executor...
        scheduleExecutor.runSchedules(now);

        // Verify that another attempt was not started
        verify(scheduleExecutor, never()).startSchedule(any(StoredSchedule.class), any(Scheduler.class), any(StoredWorkflowDefinitionWithProject.class));

        // Verify that the schedule skipped to the next time
        verify(scs).updateNextScheduleTime(SCHEDULE_ID, nextScheduleTime);
    }

    @Test
    public void testDefaultConcurrentOvertimeExecution()
            throws Exception
    {
        // Leave skip_on_overtime disabled (default)
        workflowConfig.getNestedOrSetEmpty("schedule")
                .set("daily>", "12:00:00");

        // Indicate that there is an active attempt for this workflow
        when(sessionStore.getActiveAttemptsOfWorkflow(eq(WORKFLOW_DEFINITION_ID), anyInt(), any(Optional.class)))
                .thenReturn(ImmutableList.of(attempt));

        // Run the schedule executor...
        scheduleExecutor.runSchedules(now);

        // Verify that another attempt was started
        verify(scheduleExecutor).startSchedule(any(StoredSchedule.class), any(Scheduler.class), any(StoredWorkflowDefinitionWithProject.class));

        // Verify that the schedule progressed to the next time
        verify(scs).updateNextScheduleTimeAndLastSessionTime(SCHEDULE_ID, nextScheduleTime, now);
    }
}
