package io.digdag.core.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.session.ImmutableResumingTask;
import io.digdag.core.session.ResumingTask;
import io.digdag.core.session.TaskControlStore;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.core.session.TaskType;
import io.digdag.spi.TaskReport;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;

@RunWith(MockitoJUnitRunner.class)
public class TaskControlTest
{
    @Mock TaskControlStore store;
    @Mock ObjectMapper mapper;

    @Test
    public void addDuplicateResumingTasks() throws Exception
    {
        Config config = new ConfigFactory(mapper).create();
        WorkflowTask task = new WorkflowTask.Builder()
            .name("+test")
            .fullName("test task")
            .index(0)
            .upstreamIndexes(Collections.emptyList())
            .taskType(new TaskType.Builder().build())
            .config(config)
            .build();
            
        ResumingTask resumingTask = ImmutableResumingTask.builder()
            .sourceTaskId(0L)
            .fullName("test task")
            .config(TaskConfig.assumeValidated(config, config))
            .updatedAt(Instant.now())
            .subtaskConfig(config)
            .exportParams(config)
            .resetStoreParams(Collections.emptyList())
            .storeParams(config)
            .report(TaskReport.empty())
            .error(config)
            .build();

        when(store.addResumedSubtask(
                anyLong(),
                anyLong(),
                any(TaskType.class),
                any(TaskStateCode.class),
                any(TaskStateFlags.class),
                eq(resumingTask))).thenReturn(0L);

        when(store.getTaskCountOfAttempt(anyLong())).thenReturn(0L);

        TaskControl.addInitialTasksExceptingRootTask(
            store,
            0L,
            0L,
            WorkflowTaskList.of(Arrays.asList(task, task)),
            Arrays.asList(resumingTask, resumingTask));

        verify(store, times(1)).getTaskCountOfAttempt(anyLong());
        verify(store, times(1)).addResumedSubtask(
                anyLong(),
                anyLong(),
                any(TaskType.class),
                any(TaskStateCode.class),
                any(TaskStateFlags.class),
                eq(resumingTask));
    }
}
