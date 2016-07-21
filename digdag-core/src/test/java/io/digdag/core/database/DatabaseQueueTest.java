package io.digdag.core.database;

import java.time.ZoneId;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.TaskStateException;
import io.digdag.core.repository.ResourceNotFoundException;
import com.google.common.base.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.ExpectedException;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.setupDatabase;

public class DatabaseQueueTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private DatabaseFactory factory;
    private DatabaseTaskQueueServer taskQueue;

    private int taskIdSequence = 150;

    @Before
    public void setUp()
        throws Exception
    {
        factory = setupDatabase();
        Config systemConfig = createConfigFactory()
            .create()
            .set("queue.db.max_concurrency", 2);
        taskQueue = new DatabaseTaskQueueServer(
                factory.get(),
                factory.getConfig(),
                new DatabaseTaskQueueConfig(systemConfig),
                objectMapper());
    }

    @After
    public void destroy()
    {
        factory.close();
    }

    @Test
    public void siteConcurrencyLimit()
    {
        TaskRequest req1 = generateTaskRequest("+t1");
        TaskRequest req2 = generateTaskRequest("+t2");
        TaskRequest req3 = generateTaskRequest("+t3");

        // enqueue 3 tasks
        taskQueue.enqueueDefaultQueueTask(req1);
        taskQueue.enqueueDefaultQueueTask(req2);
        taskQueue.enqueueDefaultQueueTask(req3);

        // poll 3 tasks
        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);
        List<TaskRequest> poll2 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);
        List<TaskRequest> poll3 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);

        assertThat(poll1.size(), is(1));
        assertThat(poll1, is(Arrays.asList(TaskRequest.withLockId(req1, poll1.get(0).getLockId()))));
        assertThat(poll2.size(), is(1));
        assertThat(poll2, is(Arrays.asList(TaskRequest.withLockId(req2, poll2.get(0).getLockId()))));
        // max concurrency of this site is 2. 3rd task is not acquired.
        assertThat(poll3, is(Arrays.asList()));

        // delete 1 task and get next
        taskQueue.deleteTask(0, poll1.get(0).getLockId(), "agent1");

        List<TaskRequest> poll4 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);

        assertThat(poll4.size(), is(1));
        assertThat(poll4, is(Arrays.asList(TaskRequest.withLockId(req3, poll4.get(0).getLockId()))));
    }

    @Test
    public void batchPollOrder()
    {
        TaskRequest req1 = generateTaskRequest("+t1");
        TaskRequest req2 = generateTaskRequest("+t2");
        TaskRequest req3 = generateTaskRequest("+t3");
        TaskRequest req4 = generateTaskRequest("+t4");

        taskQueue.enqueueDefaultQueueTask(req1);
        taskQueue.enqueueDefaultQueueTask(req2);
        taskQueue.enqueueDefaultQueueTask(req3);
        taskQueue.enqueueDefaultQueueTask(req4);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(2, "agent1", 300, 10);
        assertThat(poll1.size(), is(2));
        assertThat(poll1.get(0).getTaskName(), is("+t1"));
        assertThat(poll1.get(1).getTaskName(), is("+t2"));

        taskQueue.deleteTask(0, poll1.get(0).getLockId(), "agent1");
        taskQueue.deleteTask(0, poll1.get(1).getLockId(), "agent1");

        List<TaskRequest> poll2 = taskQueue.lockSharedAgentTasks(2, "agent1", 300, 10);
        assertThat(poll2.size(), is(2));
        assertThat(poll2.get(0).getTaskName(), is("+t3"));
        assertThat(poll2.get(1).getTaskName(), is("+t4"));
    }

    @Test
    public void enqueueRejectedIfDuplicatedTaskId()
    {
        TaskRequest req1 = generateTaskRequest("+t1");
        TaskRequest req2 = generateTaskRequest("+t2");
        TaskRequest req1Dup = ImmutableTaskRequest.builder()
            .from(req2)
            .taskId(req1.getTaskId())
            .build();

        taskQueue.enqueueDefaultQueueTask(req1);

        exception.expect(TaskStateException.class);
        taskQueue.enqueueDefaultQueueTask(req1Dup);
    }

    @Test
    public void deleteRejectedIfAgentIdMismatch()
    {
        TaskRequest req1 = generateTaskRequest("+t1");

        taskQueue.enqueueDefaultQueueTask(req1);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);

        exception.expect(TaskStateException.class);
        taskQueue.deleteTask(0, poll1.get(0).getLockId(), "different-agent");
    }

    @Test
    public void deleteRejectedIfSiteIdMismatch()
    {
        TaskRequest req1 = generateTaskRequest("+t1");

        taskQueue.enqueueDefaultQueueTask(req1);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);

        exception.expect(TaskStateException.class);
        taskQueue.deleteTask(19832, poll1.get(0).getLockId(), "agent1");
    }

    @Test
    public void expireLockAndRetry()
        throws InterruptedException
    {
        TaskRequest req1 = generateTaskRequest("+t1");
        TaskRequest req2 = generateTaskRequest("+t2");

        taskQueue.enqueueDefaultQueueTask(req1);
        taskQueue.enqueueDefaultQueueTask(req2);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(2, "agent1", 0, 10);  // lockSeconds = 0
        assertThat(poll1.size(), is(2));

        Thread.sleep(2000);
        taskQueue.expireLocks();

        List<TaskRequest> poll2 = taskQueue.lockSharedAgentTasks(2, "agent1", 3, 10);
        assertThat(poll2.size(), is(2));

        // TODO this needs API to get retry_count to validate retry_count
    }

    @Test
    public void heartbeatPreventsExpireLock()
        throws InterruptedException
    {
        TaskRequest req1 = generateTaskRequest("+t1");
        TaskRequest req2 = generateTaskRequest("+t2");

        taskQueue.enqueueDefaultQueueTask(req1);
        taskQueue.enqueueDefaultQueueTask(req2);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(2, "agent1", 0, 10);  // lockSeconds = 0
        assertThat(poll1.size(), is(2));

        Thread.sleep(2000);
        // heartbeat req1
        taskQueue.taskHeartbeat(0, Arrays.asList(poll1.get(0).getLockId()), "agent1", 3);

        taskQueue.expireLocks();

        List<TaskRequest> poll2 = taskQueue.lockSharedAgentTasks(2, "agent1", 3, 10);

        // req2 is expired but req1 is not
        assertThat(poll2.size(), is(1));
        assertThat(poll2.get(0).getTaskName(), is("+t2"));
    }

    /* TODO taskHeartbeat should notice failure of partial heartbeat failure but it doesn't
    @Test
    public void heartbeatRejectedIfAgentIdMismatch()
        throws InterruptedException
    {
        TaskRequest req1 = generateTaskRequest("+t1");

        taskQueue.enqueueDefaultQueueTask(req1);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);

        exception.expect(TaskStateException.class);
        taskQueue.taskHeartbeat(0, Arrays.asList(poll1.get(0).getLockId()), "different-agent", 3);
    }

    @Test
    public void heartbeatRejectedIfSiteIdMismatch()
        throws InterruptedException
    {
        TaskRequest req1 = generateTaskRequest("+t1");

        taskQueue.enqueueDefaultQueueTask(req1);

        List<TaskRequest> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10);

        exception.expect(TaskStateException.class);
        taskQueue.taskHeartbeat(19832, Arrays.asList(poll1.get(0).getLockId()), "agent1", 3);
    }
    */

    private TaskRequest generateTaskRequest(String name)
    {
        ConfigFactory cf = createConfigFactory();
        return TaskRequest.builder()
            .siteId(0)
            .projectId(1)
            .projectName(Optional.of("proj1"))
            .workflowName("wf1")
            .revision(Optional.of("rev1"))
            .taskId(taskIdSequence++)
            .attemptId(11)
            .sessionId(10)
            .retryAttemptName(Optional.absent())
            .taskName(name)
            .queueName(Optional.absent())
            .lockId("")
            .priority(0)
            .timeZone(ZoneId.of("UTC"))
            .sessionUuid(UUID.randomUUID())
            .sessionTime(Instant.ofEpochSecond(Instant.now().getEpochSecond()))
            .createdAt(Instant.ofEpochSecond(Instant.now().getEpochSecond()))
            .localConfig(cf.create())
            .config(cf.create())
            .lastStateParams(cf.create())
            .build();
    }
}
