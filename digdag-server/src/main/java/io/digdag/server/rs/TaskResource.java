package io.digdag.server.rs;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import io.digdag.spi.TaskResult;
import io.digdag.core.agent.InProcessTaskCallbackApi;
import io.digdag.client.api.RestTaskFailedCallback;
import io.digdag.client.api.RestTaskHeartbeatCallback;
import io.digdag.client.api.RestTaskRetriedCallback;
import io.digdag.client.api.RestTaskSucceededCallback;
import io.digdag.core.agent.AgentId;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;

@Path("/")
@Produces("application/json")
public class TaskResource
    extends AuthenticatedResource
{
    // POST /api/agent/heartbeat                             # extend lock expire timeout of tasks
    // POST /api/tasks/{id}/success                          # set success state to a task
    // POST /api/tasks/{id}/fail                             # set error state to a task
    // POST /api/tasks/{id}/retry                            # retry a task
    // POST /api/tasks/{id}/secrets/{key}                    # get a secret

    private final SessionStoreManager sm;
    private final InProcessTaskCallbackApi api;

    @Inject
    public TaskResource(
            SessionStoreManager sm,
            InProcessTaskCallbackApi api)
    {
        this.sm = sm;
        this.api = api;
    }

    @Path("/api/agent/heartbeat")
    @POST
    public void heartbeat(RestTaskHeartbeatCallback callback)
    {
        // TODO exception handling if necessary
        api.taskHeartbeat(getSiteId(),
                callback.getLockIds(), AgentId.of(callback.getAgentId()), callback.getLockSeconds());
    }

    @Path("/api/tasks/{id}/success")
    @POST
    public void success(@PathParam("id") long taskId,
            RestTaskSucceededCallback callback)
    {
        TaskResult taskResult = callback.getTaskResult().convert(TaskResult.class);
        api.taskSucceeded(getSiteId(),
                taskId, callback.getLockId(), AgentId.of(callback.getAgentId()),
                taskResult);
    }

    @Path("/api/tasks/{id}/fail")
    @POST
    public void fail(@PathParam("id") long taskId,
            RestTaskFailedCallback callback)
    {
        api.taskFailed(getSiteId(),
                taskId, callback.getLockId(), AgentId.of(callback.getAgentId()),
                callback.getError());
    }

    @Path("/api/tasks/{id}/retry")
    @POST
    public void retry(@PathParam("id") long taskId,
            RestTaskRetriedCallback callback)
    {
        api.retryTask(getSiteId(),
                taskId, callback.getLockId(), AgentId.of(callback.getAgentId()),
                callback.getRetryInterval(), callback.getRetryStateParams(),
                callback.getError());
    }

    // TODO
    //@Path("/api/tasks/{id}/secrets/{key}")
    //@POST
    //public Optional<String> getSecret(@PathParam("id") long taskId, @PathParam("key") String key)
    //{
    //}
}
