package io.digdag.cli.profile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.ImmutableArchivedTask;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.core.session.TaskType;
import io.digdag.core.workflow.TaskConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class TasksSummaryTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JacksonTimeModule())
        .registerModule(new GuavaModule());

    private static final Config EMPTY_CONFIG = new ConfigFactory(OBJECT_MAPPER).create();

    private static final ArchivedTask BASE_TASK = ImmutableArchivedTask.builder()
            .id(-1)
            .fullName("xxxx")
            .state(TaskStateCode.CANCELED)
            .taskType(TaskType.of(TaskType.GROUPING_ONLY))
            .stateFlags(TaskStateFlags.empty())
            .updatedAt(Instant.now())
            .config(TaskConfig.assumeValidated(EMPTY_CONFIG, EMPTY_CONFIG))
            .subtaskConfig(EMPTY_CONFIG)
            .attemptId(42)
            .exportParams(EMPTY_CONFIG)
            .storeParams(EMPTY_CONFIG)
            .stateParams(EMPTY_CONFIG)
            .error(EMPTY_CONFIG)
            .build();

    @Test
    public void normalTasks()
    {
        /*
            (root task: +wf)
            # id:1
            # started_at: 00:00:00
            # updated_at: 00:00:25

            # id:2
            # started_at: 00:00:01 (delay: 1 sec)
            # updated_at: 00:00:02
            +start:
              echo>: Start

            # id:3
            # started_at: 00:00:02 (delay: 0 sec)
            # updated_at: 00:00:11
            +parent:
              # id:4
              # started_at: 00:00:03 (delay: 1 sec)
              # updated_at: 00:00:07
              +child_wait:
                sh>: sleep 3

              # id:5
              # started_at: 00:00:09 (delay: 2 sec)
              # updated_at: 00:00:10
              +child_hello:
                echo>: Hello

            # id:6
            # started_at: 00:00:14 (delay: 3 sec)
            # updated_at: 00:00:19
            +sleep:
              sh>: sleep 4

            # id:7
            # started_at: 00:00:24 (delay: 5 sec)
            # updated_at: 00:00:25
            +finish:
              echo>: Finish
         */

        TasksSummary.DefaultBuilder builder = new TasksSummary.DefaultBuilder();
        builder.updateWithTasks(ImmutableList.of(
                // Root task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(1)
                        .withFullName("+wf")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withStartedAt(Instant.parse("2000-01-01T00:00:00Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:25Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(2)
                        .withFullName("+wf+start")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay: 1 sec
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:01Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:02Z")),
                // Group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(3)
                        .withFullName("+wf+parent")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withParentId(1)
                        .withUpstreams(2)
                        // Delay: 0 sec
                        // Duration: 9 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:02Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:11Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(4)
                        .withFullName("+wf+parent+child_wait")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(3)
                        // Delay: 1 sec
                        // Duration: 4 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:03Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:07Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(5)
                        .withFullName("+wf+parent+child_hello")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(3)
                        .withUpstreams(4)
                        // Delay: 2 sec
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:09Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:10Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(6)
                        .withFullName("+wf+sleep")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        .withUpstreams(3)
                        // Delay: 3 sec
                        // Duration: 5 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:14Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:19Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(7)
                        .withFullName("+wf+finish")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        .withUpstreams(6)
                        // Delay: 5 sec
                        // Duration: 4 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:24Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:28Z"))
        ));
        TasksSummary summary = builder.build();
        assertEquals(6, summary.totalTasks);
        assertEquals(6, summary.totalRunTasks);
        assertEquals(6, summary.totalSuccessTasks);
        assertEquals(0, summary.totalErrorTasks);
        assertEquals(2000, summary.startDelayMillis.mean().longValue());
        assertEquals(4000, summary.execDurationMillis.mean().longValue());
    }

    @Test
    public void attemptContainsErrorTaskAndFailureAlert()
    {
        /*
            (root task: +wf)
            # id:1
            # started_at: 00:00:00
            # updated_at: 00:00:12

            # id:2
            # started_at: 00:00:01 (delay: 1 sec)
            # updated_at: 00:00:02
            +start:
              echo>: Start

            # id:3 (error)
            # started_at: 00:00:04 (delay: 3 sec)
            # updated_at: 00:00:06
            +fail:
              fail>: Ahhhhhhhhhhhh

            # id:4
            # started_at: null
            # updated_at: null
            +finish:
              echo>: Finish

            # id:5 (This task's start delay should be ignored)
            # started_at: 00:00:07
            # updated_at: 00:00:09
            +error_handling
              echo>: Error handling now...

            # id:6 (This task's start delay should be ignored)
            # started_at: 00:00:10
            # updated_at: 00:00:12
            (^failure-alert)
              notify: Workflow session attempt failed
         */

        TasksSummary.DefaultBuilder builder = new TasksSummary.DefaultBuilder();
        builder.updateWithTasks(ImmutableList.of(
                // Root task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(1)
                        .withFullName("+wf")
                        .withState(TaskStateCode.GROUP_ERROR)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withStartedAt(Instant.parse("2000-01-01T00:00:00Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:13Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(2)
                        .withFullName("+wf+start")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay: 1 sec
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:01Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:02Z")),
                // Non-group task (failed)
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(3)
                        .withFullName("+wf+fail")
                        .withState(TaskStateCode.ERROR)
                        .withParentId(1)
                        .withUpstreams(2)
                        // Delay: 2 sec
                        // Duration: 2 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:04Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:06Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(4)
                        .withFullName("+wf+finish")
                        .withState(TaskStateCode.BLOCKED)
                        .withParentId(1)
                        .withUpstreams(3)
                        // Not executed
                        .withStartedAt(Optional.absent())
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:03Z")),
                // Dynamically generated task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(5)
                        .withFullName("+wf^error")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay looks 7 seconds, but it's not actual delay and should be ignored
                        // Duration: 2 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:07Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:09Z")),
                // Dynamically generated task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(6)
                        .withFullName("+wf^failure-alert")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay looks 10 seconds, but it's not actual delay and should be ignored
                        // Duration: 3 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:10Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:13Z"))
        ));
        TasksSummary summary = builder.build();
        assertEquals(5, summary.totalTasks);
        assertEquals(4, summary.totalRunTasks);
        assertEquals(3, summary.totalSuccessTasks);
        assertEquals(1, summary.totalErrorTasks);
        assertEquals(1500, summary.startDelayMillis.mean().longValue());
        assertEquals(2000, summary.execDurationMillis.mean().longValue());
    }

    @Test
    public void attemptContainsCheckDirective()
    {
        /*
            (root task: +wf)
            # id:1
            # started_at: 00:00:00
            # updated_at: 00:00:14

            # id:2
            # started_at: 00:00:02 (delay: 0 sec)
            # updated_at: 00:00:11
            +parent:
              # id:3
              # started_at: 00:00:03 (delay: 1 sec)
              # updated_at: 00:00:07
              +child_wait:
                sh>: sleep 3

              # id:4 (dynamically generated)
              # started_at: 00:00:08
              # updated_at: 00:00:09
              _check:
                echo>: Done!

            # id:5
            # started_at: 00:00:14 (delay: 3 sec)
            # updated_at: 00:00:14
            +finish:
              echo>: Finish
         */

        TasksSummary.DefaultBuilder builder = new TasksSummary.DefaultBuilder();
        builder.updateWithTasks(ImmutableList.of(
                // Root task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(1)
                        .withFullName("+wf")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withStartedAt(Instant.parse("2000-01-01T00:00:00Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:14Z")),
                // Group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(2)
                        .withFullName("+wf+parent")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withParentId(1)
                        // Delay: 2 sec
                        // Duration: 9 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:02Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:11Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(3)
                        .withFullName("+wf+parent+child_wait")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(2)
                        // Delay: 1 sec
                        // Duration: 4 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:03Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:07Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(4)
                        .withFullName("+wf+parent^check")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(2)
                        // Delay looks 5 seconds, but it's not actual delay and should be ignored
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:08Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:09Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(5)
                        .withFullName("+wf+finish")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        .withUpstreams(2)
                        // Delay: 1 sec
                        // Duration: 2 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:14Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:16Z"))
        ));
        TasksSummary summary = builder.build();
        assertEquals(4, summary.totalTasks);
        assertEquals(4, summary.totalRunTasks);
        assertEquals(4, summary.totalSuccessTasks);
        assertEquals(0, summary.totalErrorTasks);
        assertEquals(2000, summary.startDelayMillis.mean().longValue());
        assertEquals(4000, summary.execDurationMillis.mean().longValue());
    }

    @Test
    public void attemptContainsSla()
    {
        /*
            -----------------------
            sla:
              duration: 01:00:00
              +notice:
                echo>: Timeout!
            -----------------------

            (root task: +wf)
            # id:1
            # started_at: 00:00:00
            # updated_at: 00:00:14

            # id:2
            # started_at: 00:00:02 (delay: 0 sec)
            # updated_at: 00:00:11
            +parent:
              # id:3
              # started_at: 00:00:03 (delay: 1 sec)
              # updated_at: 00:01:07
              +child_wait:
                sh>: sleep 999999

            # id:4 (dynamically generated)
            # started_at: 00:01:12
            # updated_at: 00:01:17
            ^sla:
              # id:5 (dynamically generated)
              # started_at: 00:01:13
              # updated_at: 00:01:19
              +notice
                echo>: Timeout!
         */

        TasksSummary.DefaultBuilder builder = new TasksSummary.DefaultBuilder();
        builder.updateWithTasks(ImmutableList.of(
                // Root task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(1)
                        .withFullName("+wf")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withStartedAt(Instant.parse("2000-01-01T00:00:00Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:14Z")),
                // Group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(2)
                        .withFullName("+wf+parent")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withParentId(1)
                        // Delay: 2 sec
                        // Duration: 9 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:02Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:11Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(3)
                        .withFullName("+wf+parent+child_wait")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(2)
                        // Delay: 1 sec
                        // Duration: 3604 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:03Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T01:00:07Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(4)
                        .withFullName("+wf^sla")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay looks 3612 seconds, but it's not actual delay and should be ignored
                        // Duration: 7 sec
                        .withStartedAt(Instant.parse("2000-01-01T01:00:12Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T01:00:19Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(5)
                        .withFullName("+wf^sla+notice")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(4)
                        // To make it simple, all tasks that contain ^sla are ignored
                        // Duration: 4 sec
                        .withStartedAt(Instant.parse("2000-01-01T01:00:15Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T01:00:19Z"))
        ));
        TasksSummary summary = builder.build();
        assertEquals(4, summary.totalTasks);
        assertEquals(4, summary.totalRunTasks);
        assertEquals(4, summary.totalSuccessTasks);
        assertEquals(0, summary.totalErrorTasks);
        // (2 + 1) / 2
        assertEquals(1500, summary.startDelayMillis.mean().longValue());
        // (9 + 3604 + 7 + 4) / 4
        // com.google.common.math.Stats.mean() returns a bit fuzzy value...
        assertThat(summary.execDurationMillis.mean().doubleValue(), is(closeTo(906000, 1)));
    }

    @Test
    public void attemptContainsRetriedTasks()
    {
        /*
            (root task: +wf)
            # id:1
            # started_at: 00:00:00
            # updated_at: 00:00:12

            ##### 1st try

            # id:2
            # started_at: 00:00:01 (delay: 1 sec)
            # updated_at: 00:00:02
            +start:
              echo>: Start

            # id:3 (error)
            # started_at: 00:00:04 (delay: 3 sec)
            # updated_at: 00:00:06
            +fail:
              fail>: Ahhhhhhhhhhhh

            ##### 2nd try

            # id:4
            # started_at: 00:00:08 (delay: 2 sec)
            # updated_at: 00:00:09
            +start:
              echo>: Start

            # id:5 (error)
            # started_at: 00:00:14 (delay: 5 sec)
            # updated_at: 00:00:18
            +fail:
              fail>: Ahhhhhhhhhhhh

            # id:6 (This task's start delay should be ignored)
            # started_at: 00:00:20
            # updated_at: 00:00:23
            (^failure-alert)
              notify: Workflow session attempt failed
         */

        TasksSummary.DefaultBuilder builder = new TasksSummary.DefaultBuilder();
        builder.updateWithTasks(ImmutableList.of(
                // Root task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(1)
                        .withFullName("+wf")
                        .withState(TaskStateCode.GROUP_ERROR)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withStartedAt(Instant.parse("2000-01-01T00:00:00Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:23Z")),
                // 1st try
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(2)
                        .withFullName("+wf+start")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay: 1 sec
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:01Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:02Z")),
                // Non-group task (failed)
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(3)
                        .withFullName("+wf+fail")
                        .withState(TaskStateCode.ERROR)
                        .withParentId(1)
                        .withUpstreams(2)
                        // Delay: 2 sec
                        // Duration: 2 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:04Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:06Z")),
                // 2nd try
                // Non-group task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(4)
                        .withFullName("+wf+start")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay looks 2 seconds, but it's not actual delay and should be ignored
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:08Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:09Z")),
                // Non-group task (failed)
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(5)
                        .withFullName("+wf+fail")
                        .withState(TaskStateCode.ERROR)
                        .withParentId(1)
                        .withUpstreams(4)
                        // Delay looks 5 seconds, but it's not actual delay and should be ignored
                        // Duration: 4 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:14Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:18Z")),
                // Dynamically generated task
                ImmutableArchivedTask.copyOf(BASE_TASK)
                        .withId(6)
                        .withFullName("+wf^failure-alert")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay looks 10 seconds, but it's not actual delay and should be ignored
                        // Duration: 2 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:20Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:22Z"))
        ));
        TasksSummary summary = builder.build();
        assertEquals(5, summary.totalTasks);
        assertEquals(5, summary.totalRunTasks);
        assertEquals(3, summary.totalSuccessTasks);
        assertEquals(2, summary.totalErrorTasks);
        assertEquals(1500, summary.startDelayMillis.mean().longValue());
        assertEquals(2000, summary.execDurationMillis.mean().longValue());
    }

    @Test
    public void propagatableTasksSummary()
    {
        TasksSummary.DefaultBuilder overall = new TasksSummary.DefaultBuilder();
        TasksSummary.DefaultBuilder site0 = new TasksSummary.DefaultBuilder();
        TasksSummary.DefaultBuilder site1 = new TasksSummary.DefaultBuilder();

        ArchivedTask task0_0 = mock(ArchivedTask.class);
        ArchivedTask task0_1 = mock(ArchivedTask.class);
        ArchivedTask task0_2 = mock(ArchivedTask.class);

        {
            // Update overall and site:0's TasksSummary.Builder
            WholeTasksSummary.PropagatableTasksSummaryBuilder builder =
                    new WholeTasksSummary.PropagatableTasksSummaryBuilder(ImmutableList.of(overall, site0));

            // 1st attempt
            //   total tasks: 4
            //   total run tasks: 3
            //   total success tasks: 2
            //   total error tasks: 1
            //   (skipped tasks: 1)
            builder.incrementAttempts();
            builder.incrementTotalTasks(4);
            builder.incrementTotalRunTasks();
            builder.incrementTotalRunTasks();
            builder.incrementTotalRunTasks();
            builder.incrementTotalSuccessTasks();
            builder.incrementTotalSuccessTasks();
            builder.incrementTotalErrorTasks();
            builder.addExecDurationMillis(750);
            builder.addExecDurationMillis(1000);
            builder.addExecDurationMillis(1250);
            builder.addStartDelayMillis(400, () -> task0_0);
            builder.addStartDelayMillis(800, () -> task0_1);
            builder.addStartDelayMillis(600, () -> task0_2);
        }

        ArchivedTask task1_0 = mock(ArchivedTask.class);
        ArchivedTask task1_1 = mock(ArchivedTask.class);
        ArchivedTask task1_2 = mock(ArchivedTask.class);
        ArchivedTask task1_3 = mock(ArchivedTask.class);
        ArchivedTask task1_4 = mock(ArchivedTask.class);

        {
            // Update overall and site:1's TasksSummary.Builder
            WholeTasksSummary.PropagatableTasksSummaryBuilder builder =
                    new WholeTasksSummary.PropagatableTasksSummaryBuilder(ImmutableList.of(overall, site1));

            // 1st attempt
            //   total tasks: 2
            //   total run tasks: 2
            //   total success tasks: 2
            builder.incrementAttempts();
            builder.incrementTotalTasks(2);
            builder.incrementTotalRunTasks();
            builder.incrementTotalRunTasks();
            builder.incrementTotalSuccessTasks();
            builder.incrementTotalSuccessTasks();
            builder.addExecDurationMillis(1000);
            builder.addExecDurationMillis(1500);
            builder.addStartDelayMillis(400, () -> task1_0);
            builder.addStartDelayMillis(250, () -> task1_1);

            // 2nd attempt
            //   total tasks: 3
            //   total run tasks: 3
            //   total success tasks: 2
            //   total error tasks: 1
            //   (skipped tasks: 0)
            builder.incrementAttempts();
            builder.incrementTotalTasks(3);
            builder.incrementTotalRunTasks();
            builder.incrementTotalRunTasks();
            builder.incrementTotalRunTasks();
            builder.incrementTotalSuccessTasks();
            builder.incrementTotalSuccessTasks();
            builder.incrementTotalErrorTasks();
            builder.addExecDurationMillis(250);
            builder.addExecDurationMillis(1250);
            builder.addExecDurationMillis(1000);
            builder.addStartDelayMillis(300, () -> task1_2);
            builder.addStartDelayMillis(100, () -> task1_3);
            builder.addStartDelayMillis(350, () -> task1_4);
        }

        {
            TasksSummary summary = overall.build();
            assertEquals(3, summary.attempts);
            assertEquals(9, summary.totalTasks);
            assertEquals(8, summary.totalRunTasks);
            assertEquals(6, summary.totalSuccessTasks);
            assertEquals(2, summary.totalErrorTasks);
            // (750 + 1000 + 1250 + 1000 + 1500 + 250 + 1250 + 1000) / 8
            assertEquals(1000, summary.execDurationMillis.mean().longValue());
            assertEquals(250, summary.execDurationMillis.min().longValue());
            assertEquals(1500, summary.execDurationMillis.max().longValue());
            // (400 + 800 + 600 + 400 + 250 + 300 + 100 + 350) / 8
            assertEquals(400, summary.startDelayMillis.mean().longValue());
            assertEquals(100, summary.startDelayMillis.min().longValue());
            assertEquals(800, summary.startDelayMillis.max().longValue());
            assertEquals(task0_1, summary.mostDelayedTask);
        }

        {
            TasksSummary summary = site0.build();
            assertEquals(1, summary.attempts);
            assertEquals(4, summary.totalTasks);
            assertEquals(3, summary.totalRunTasks);
            assertEquals(2, summary.totalSuccessTasks);
            assertEquals(1, summary.totalErrorTasks);
            // (750 + 1000 + 1250) / 3
            assertEquals(1000, summary.execDurationMillis.mean().longValue());
            assertEquals(750, summary.execDurationMillis.min().longValue());
            assertEquals(1250, summary.execDurationMillis.max().longValue());
            // (400 + 800 + 600) / 3
            assertEquals(600, summary.startDelayMillis.mean().longValue());
            assertEquals(400, summary.startDelayMillis.min().longValue());
            assertEquals(800, summary.startDelayMillis.max().longValue());
            assertEquals(task0_1, summary.mostDelayedTask);
        }

        {
            TasksSummary summary = site1.build();
            assertEquals(2, summary.attempts);
            assertEquals(5, summary.totalTasks);
            assertEquals(5, summary.totalRunTasks);
            assertEquals(4, summary.totalSuccessTasks);
            assertEquals(1, summary.totalErrorTasks);
            // (1000 + 1500 + 250 + 1250 + 1000) / 5
            assertEquals(1000, summary.execDurationMillis.mean().longValue());
            assertEquals(250, summary.execDurationMillis.min().longValue());
            assertEquals(1500, summary.execDurationMillis.max().longValue());
            // (400 + 250 + 300 + 100 + 350) / 5
            assertEquals(280, summary.startDelayMillis.mean().longValue());
            assertEquals(100, summary.startDelayMillis.min().longValue());
            assertEquals(400, summary.startDelayMillis.max().longValue());
            assertEquals(task1_0, summary.mostDelayedTask);
        }
    }

    @Test
    public void isIgnorableDynamicTask()
    {
        assertFalse(TasksSummary.Builder.isIgnorableDynamicTask("+task0+task1+task2"));

        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0+task1^error"));
        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0^error+task1"));

        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0+task1^check"));
        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0^check+task1"));

        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0+task1^failure-alert"));
        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0^failure-alert+task1"));

        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0+task1^sla"));
        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0^sla+task1"));

        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0+task1^sla^alert"));
        assertTrue(TasksSummary.Builder.isIgnorableDynamicTask("+task0^sla^alert+task1"));
    }
}
