package io.digdag.profiler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
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
public class TaskSummaryTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JacksonTimeModule())
        .registerModule(new GuavaModule());

    @Test
    public void taskSummary()
    {
        /*
            (root task: +wf)
            # id:1
            # started_at: 00:00:00
            # updated_at: 00:00:00

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

        Config emptyConfig = new ConfigFactory(OBJECT_MAPPER).create();
        TaskConfig taskConfig = TaskConfig.assumeValidated(emptyConfig, emptyConfig);
        ArchivedTask base = ImmutableArchivedTask.builder()
                .id(-1)
                .fullName("xxxx")
                .state(TaskStateCode.CANCELED)
                .taskType(TaskType.of(TaskType.GROUPING_ONLY))
                .stateFlags(TaskStateFlags.empty())
                .updatedAt(Instant.now())
                .config(taskConfig)
                .subtaskConfig(emptyConfig)
                .attemptId(42)
                .exportParams(emptyConfig)
                .storeParams(emptyConfig)
                .stateParams(emptyConfig)
                .error(emptyConfig)
                .build();

        TasksSummary.Builder builder = new TasksSummary.Builder();
        TasksSummary.updateBuilderWithTasks(builder, ImmutableList.of(
                // Root task
                ImmutableArchivedTask.copyOf(base)
                        .withId(1)
                        .withFullName("+wf+start")
                        .withState(TaskStateCode.SUCCESS)
                        .withTaskType(TaskType.of(TaskType.GROUPING_ONLY))
                        .withStartedAt(Instant.parse("2000-01-01T00:00:00Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:25Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(base)
                        .withId(2)
                        .withFullName("+wf+start")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(1)
                        // Delay: 1 sec
                        // Duration: 1 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:01Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:02Z")),
                // Group task
                ImmutableArchivedTask.copyOf(base)
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
                ImmutableArchivedTask.copyOf(base)
                        .withId(4)
                        .withFullName("+wf+parent+child_wait")
                        .withState(TaskStateCode.SUCCESS)
                        .withParentId(3)
                        // Delay: 1 sec
                        // Duration: 4 sec
                        .withStartedAt(Instant.parse("2000-01-01T00:00:03Z"))
                        .withUpdatedAt(Instant.parse("2000-01-01T00:00:07Z")),
                // Non-group task
                ImmutableArchivedTask.copyOf(base)
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
                ImmutableArchivedTask.copyOf(base)
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
                ImmutableArchivedTask.copyOf(base)
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
        assertEquals(4000, summary.execDuration.mean().longValue());
    }
}
