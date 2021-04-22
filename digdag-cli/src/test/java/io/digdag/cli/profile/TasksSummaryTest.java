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

import static org.junit.Assert.assertEquals;

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
              # id:2
              # started_at: 00:00:03 (delay: 1 sec)
              # updated_at: 00:00:07
              +child_wait:
                sh>: sleep 3

              # id:3 (dynamically generated)
              # started_at: 00:00:08
              # updated_at: 00:00:09
              _check:
                echo>: Done!

            # id:4
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
}
