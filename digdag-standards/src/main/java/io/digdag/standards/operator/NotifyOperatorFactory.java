package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.Notifier;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;

public class NotifyOperatorFactory
        implements OperatorFactory
{
    private final Notifier notifier;

    @Inject
    public NotifyOperatorFactory(Notifier notifier)
    {
        this.notifier = notifier;
    }

    public String getType()
    {
        return "notify";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new NotifyOperator(request, notifier);
    }

    private static class NotifyOperator
            implements Operator
    {
        private final TaskRequest request;
        private final Notifier notifier;

        public NotifyOperator(TaskRequest request, Notifier notifier)
        {
            this.request = request;
            this.notifier = notifier;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);

            Notification notification = Notification.builder(Instant.now(), message)
                    .siteId(request.getSiteId())
                    .projectName(request.getProjectName())
                    .projectId(request.getProjectId())
                    .workflowName(request.getWorkflowName())
                    .revision(request.getRevision().or(""))
                    .attemptId(request.getAttemptId())
                    .sessionId(request.getSessionId())
                    .taskName(request.getTaskName())
                    .timeZone(request.getTimeZone())
                    .sessionUuid(request.getSessionUuid())
                    .sessionTime(OffsetDateTime.ofInstant(request.getSessionTime(), request.getTimeZone()))
                    .build();

            try {
                notifier.sendNotification(notification);
            }
            catch (NotificationException e) {
                throw new TaskExecutionException(e, ConfigElement.copyOf(params));
            }

            return TaskResult.empty(request);
        }
    }
}
