package io.digdag.server.rs;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.api.RestTaskHeartbeatCallback;
import io.digdag.client.api.RestTaskPollRequest;
import io.digdag.client.api.RestTaskRequestCollection;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.AgentId;
import io.digdag.core.agent.InProcessTaskCallbackApi;
import io.digdag.core.agent.InProcessTaskServerApi;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.spi.TaskRequest;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/")
@Produces("application/json")
public class QueueResource
    extends AuthenticatedResource
{
    // POST /api/agent/poll?queue=<name>&count=<N>           # lock and get queued tasks
    // POST /api/agent/heartbeat                             # extend lock expire timeout of tasks

    private static final int MAX_ACQUIRE_COUNT = 10;
    private static final int MAX_SLEEP_MILLIS = 1000;

    private final InProcessTaskServerApi taskServer;
    private final ConfigFactory cf;

    @Inject
    public QueueResource(
            InProcessTaskServerApi taskServer,
            ConfigFactory cf)
    {
        this.taskServer = taskServer;
        this.cf = cf;
    }

    @Path("/api/agent/heartbeat")
    @POST
    public void heartbeat(RestTaskHeartbeatCallback callback)
    {
        taskServer.getQueueClient().taskHeartbeat(getSiteId(),
                callback.getLockIds(), callback.getAgentId(), callback.getLockSeconds());
    }

    @Path("/api/agent/poll")
    @POST
    public RestTaskRequestCollection poll(RestTaskPollRequest request)
    {
        if (request.getQueueName().isPresent()) {
            // queue-bound remote agent
            // TODO implement
            return RestTaskRequestCollection.builder()
                .taskRequests(ImmutableList.of())
                .build();
        }
        else if (isSuperAgent()) {
            // cross-site cross-queue super agent
            List<TaskRequest> taskRequests = taskServer.lockSharedAgentTasks(
                    Math.min(request.getCount(), MAX_ACQUIRE_COUNT),
                    AgentId.of(request.getAgentId()),
                    request.getLockSeconds(),
                    Math.min(request.getMaxSleepMillis(), MAX_SLEEP_MILLIS));
            // convert List<TaskRequest> to List<Config>
            List<Config> tasks = taskRequests.stream()
                .map(t -> cf.create().set("t", t).getNested("t"))
                .collect(Collectors.toList());
            return RestTaskRequestCollection.builder()
                .taskRequests(tasks)
                .build();
        }
        else {
            throw new IllegalArgumentException("queue= parameter is required");
        }

    }
}
