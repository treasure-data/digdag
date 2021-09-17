package io.digdag.core.database;

import java.util.List;
import java.util.Arrays;
import io.digdag.client.config.Config;
import io.digdag.core.acroute.DefaultAccountRoutingFactory;
import io.digdag.spi.AccountRouting;
import io.digdag.spi.TaskQueueData;
import io.digdag.spi.TaskQueueRequest;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import com.google.common.base.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.rules.ExpectedException;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigMapper;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.setupDatabase;

public class DatabaseQueueTest
{
    private static final int siteId = 0;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private DatabaseFactory factory;
    private DatabaseTaskQueueServer taskQueue;

    private int taskIdSequence = 150;

    private AccountRouting accountRoutingDisabled;
    private AccountRouting accountRoutingInclude0;
    private AccountRouting accountRoutingExclude0;

    @Before
    public void setUp()
        throws Exception
    {
        factory = setupDatabase(true);
        Config systemConfig = createConfigFactory()
            .create()
            .set("queue.db.max_concurrency", 2);
        taskQueue = new DatabaseTaskQueueServer(
                factory.getConfig(),
                factory.get(),
                createConfigMapper(),
                new DatabaseTaskQueueConfig(systemConfig),
                objectMapper());
        setUpAccountRouting();;
    }

    private void setUpAccountRouting()
    {
        Config cf1 = newConfig();
        this.accountRoutingDisabled = DefaultAccountRoutingFactory.fromConfig(cf1, Optional.of(AccountRouting.ModuleType.AGENT.toString()));
        cf1.set("agent.account_routing.enabled", "true")
                .set("agent.account_routing.include", "0");
        this.accountRoutingInclude0 = DefaultAccountRoutingFactory.fromConfig(cf1, Optional.of(AccountRouting.ModuleType.AGENT.toString()));
        cf1.remove("agent.account_routing.include")
                .set("agent.account_routing.exclude", "0");
        this.accountRoutingExclude0 = DefaultAccountRoutingFactory.fromConfig(cf1, Optional.of(AccountRouting.ModuleType.AGENT.toString()));
    }

    @After
    public void destroy()
    {
        factory.close();
    }

    @Test
    public void siteConcurrencyLimit()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");
        TaskQueueRequest req2 = generateRequest("2");
        TaskQueueRequest req3 = generateRequest("3");

        // enqueue 3 tasks
        taskQueue.enqueueDefaultQueueTask(siteId, req1);
        taskQueue.enqueueDefaultQueueTask(siteId, req2);
        taskQueue.enqueueDefaultQueueTask(siteId, req3);

        // poll 3 tasks
        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);
        assertThat(poll1.size(), is(1));
        assertThat(poll1, is(Arrays.asList(withLockId(req1, poll1.get(0).getLockId()))));

        List<TaskQueueLock> poll2 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);
        assertThat(poll2.size(), is(1));
        assertThat(poll2, is(Arrays.asList(withLockId(req2, poll2.get(0).getLockId()))));

        List<TaskQueueLock> poll3 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);
        // max concurrency of this site is 2. 3rd task is not acquired.
        assertThat(poll3, is(Arrays.asList()));

        // delete 1 task and get next
        taskQueue.deleteTask(siteId, poll1.get(0).getLockId(), "agent1");

        List<TaskQueueLock> poll4 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);
        assertThat(poll4.size(), is(1));
        assertThat(poll4, is(Arrays.asList(withLockId(req3, poll4.get(0).getLockId()))));
    }

    @Test
    public void batchPollOrder()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");
        TaskQueueRequest req2 = generateRequest("2");
        TaskQueueRequest req3 = generateRequest("3");
        TaskQueueRequest req4 = generateRequest("4");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);
        taskQueue.enqueueDefaultQueueTask(siteId, req2);
        taskQueue.enqueueDefaultQueueTask(siteId, req3);
        taskQueue.enqueueDefaultQueueTask(siteId, req4);

        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(2, "agent1", 300, 10, accountRoutingDisabled);

        assertThat(poll1.size(), is(2));
        assertThat(poll1.get(0).getUniqueName(), is("1"));
        assertThat(poll1.get(1).getUniqueName(), is("2"));

        taskQueue.deleteTask(siteId, poll1.get(0).getLockId(), "agent1");
        taskQueue.deleteTask(siteId, poll1.get(1).getLockId(), "agent1");

        List<TaskQueueLock> poll2 = taskQueue.lockSharedAgentTasks(2, "agent1", 300, 10, accountRoutingDisabled);
        assertThat(poll2.size(), is(2));
        assertThat(poll2.get(0).getUniqueName(), is("3"));
        assertThat(poll2.get(1).getUniqueName(), is("4"));
    }

    @Test
    public void enqueueRejectedIfDuplicatedTaskId()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");
        TaskQueueRequest req1Dup = generateRequest("1");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);

        exception.expect(TaskConflictException.class);
        taskQueue.enqueueDefaultQueueTask(siteId, req1Dup);
    }

    @Test
    public void deleteRejectedIfAgentIdMismatch()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);

        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);

        exception.expect(TaskConflictException.class);
        taskQueue.deleteTask(siteId, poll1.get(0).getLockId(), "different-agent");
    }

    @Test
    public void deleteRejectedIfSiteIdMismatch()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);
        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);

        exception.expect(TaskNotFoundException.class);
        taskQueue.deleteTask(19832, poll1.get(0).getLockId(), "agent1");
    }

    @Test
    public void expireLockAndRetry()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");
        TaskQueueRequest req2 = generateRequest("2");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);
        taskQueue.enqueueDefaultQueueTask(siteId, req2);

        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(2, "agent1", 0, 10, accountRoutingDisabled);  // lockSeconds = 0
        assertThat(poll1.size(), is(2));

        Thread.sleep(2000);

        taskQueue.expireLocks();

        List<TaskQueueLock> poll2 = taskQueue.lockSharedAgentTasks(2, "agent1", 3, 10, accountRoutingDisabled);
        assertThat(poll2.size(), is(2));
        // TODO this needs API to get retry_count to validate retry_count
    }

    @Test
    public void heartbeatPreventsExpireLock()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");
        TaskQueueRequest req2 = generateRequest("2");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);
        taskQueue.enqueueDefaultQueueTask(siteId, req2);

        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(2, "agent1", 0, 10, accountRoutingDisabled);  // lockSeconds = 0
        assertThat(poll1.size(), is(2));

        Thread.sleep(2000);

        // heartbeat req1
        taskQueue.taskHeartbeat(siteId, Arrays.asList(poll1.get(0).getLockId()), "agent1", 3);

        taskQueue.expireLocks();

        List<TaskQueueLock> poll2 = taskQueue.lockSharedAgentTasks(2, "agent1", 3, 10, accountRoutingDisabled);

        // req2 is expired but req1 is not
        assertThat(poll2.size(), is(1));
        assertThat(poll2.get(0).getUniqueName(), is("2"));
    }

    @Test
    public void heartbeatRejectedIfAgentIdMismatch()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);

        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);

        List<String> failedLockIdList = taskQueue.taskHeartbeat(0, Arrays.asList(poll1.get(0).getLockId()), "different-agent", 3);
        assertThat(failedLockIdList, is(Arrays.asList(poll1.get(0).getLockId())));
    }

    @Test
    public void heartbeatRejectedIfSiteIdMismatch()
        throws Exception
    {
        TaskQueueRequest req1 = generateRequest("1");

        taskQueue.enqueueDefaultQueueTask(siteId, req1);

        List<TaskQueueLock> poll1 = taskQueue.lockSharedAgentTasks(1, "agent1", 300, 10, accountRoutingDisabled);

        List<String> failedLockIdList = taskQueue.taskHeartbeat(19832, Arrays.asList(poll1.get(0).getLockId()), "agent1", 3);
        assertThat(failedLockIdList, is(Arrays.asList(poll1.get(0).getLockId())));
    }

    private TaskQueueRequest generateRequest(String uniqueName)
    {
        return TaskQueueRequest.builder()
            .priority(0)
            .uniqueName(uniqueName)
            .data(Optional.absent())
            .build();
    }

    private static TaskQueueLock withLockId(TaskQueueData data, String lockId)
    {
        return TaskQueueLock.builder()
            .lockId(lockId)
            .uniqueName(data.getUniqueName())
            .data(data.getData())
            .build();
    }

    @Test
    public void testAccountRoutingDisabled()
            throws Exception
    {
        taskQueue.enqueueDefaultQueueTask(siteId, generateRequest("1"));
        taskQueue.enqueueDefaultQueueTask(siteId, generateRequest("2"));
        List<TaskQueueLock> poll1= taskQueue.lockSharedAgentTasks(100, "agent1", 300, 10, accountRoutingDisabled);
        assertThat(poll1.size(), is(2));
    }

    @Test
    public void testAccountRoutingInclude()
            throws Exception
    {
        taskQueue.enqueueDefaultQueueTask(siteId, generateRequest("1"));
        taskQueue.enqueueDefaultQueueTask(siteId, generateRequest("2"));
        List<TaskQueueLock> poll1= taskQueue.lockSharedAgentTasks(100, "agent1", 300, 10, accountRoutingInclude0);
        assertThat(poll1.size(), is(2));
    }

    @Test
    public void testAccountRoutingExclude()
            throws Exception
    {
        taskQueue.enqueueDefaultQueueTask(siteId, generateRequest("1"));
        taskQueue.enqueueDefaultQueueTask(siteId, generateRequest("2"));
        List<TaskQueueLock> poll1= taskQueue.lockSharedAgentTasks(100, "agent1", 300, 10, accountRoutingExclude0);
        assertThat(poll1.size(), is(0));
    }
}
