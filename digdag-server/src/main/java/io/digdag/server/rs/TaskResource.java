package io.digdag.server.rs;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import io.digdag.spi.TaskResult;
import io.digdag.core.agent.InProcessTaskCallbackApi;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.client.api.RestTaskFailedCallback;
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
    // POST /api/tasks/{id}/succeeded                        # set success state to a task
    // POST /api/tasks/{id}/failed                           # set error state to a task
    // POST /api/tasks/{id}/retried                          # retry a task
    // POST /api/tasks/{id}/secrets/{key}                    # get a secret

    private final WorkflowExecutor exec;

    @Inject
    public TaskResource(
            WorkflowExecutor exec)
    {
        this.exec = exec;
    }

    @Path("/api/tasks/{id}/succeeded")
    @POST
    public void succeeded(@PathParam("id") long taskId,
            RestTaskSucceededCallback callback)
    {
        // TODO verify ResourceNotFoundException if site id is wrong
        TaskResult taskResult = callback.getTaskResult().convert(TaskResult.class);
        exec.taskSucceeded(getAgentSiteId(),
                taskId, callback.getLockId(), AgentId.of(callback.getAgentId()),
                taskResult);
    }

    @Path("/api/tasks/{id}/failed")
    @POST
    public void failed(@PathParam("id") long taskId,
            RestTaskFailedCallback callback)
    {
        // TODO verify ResourceNotFoundException if site id is wrong
        exec.taskFailed(getAgentSiteId(),
                taskId, callback.getLockId(), AgentId.of(callback.getAgentId()),
                callback.getError());
    }

    @Path("/api/tasks/{id}/retried")
    @POST
    public void retried(@PathParam("id") long taskId,
            RestTaskRetriedCallback callback)
    {
        // TODO verify ResourceNotFoundException if site id is wrong
        exec.retryTask(getAgentSiteId(),
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
