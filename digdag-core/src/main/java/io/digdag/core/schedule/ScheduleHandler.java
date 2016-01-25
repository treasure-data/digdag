package io.digdag.core.schedule;

import java.util.List;
import java.util.TimeZone;
import java.util.Locale;
import java.time.Instant;
import java.time.ZoneId;
import java.text.SimpleDateFormat;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.session.Session;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.SessionMonitor;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.WorkflowExecutor;

public class ScheduleHandler
{
    private final ConfigFactory cf;
    private final RepositoryStoreManager rm;
    private final WorkflowExecutor exec;

    @Inject
    public ScheduleHandler(
            ConfigFactory cf,
            RepositoryStoreManager rm,
            WorkflowExecutor exec)
    {
        this.cf = cf;
        this.rm = rm;
        this.exec = exec;
    }

    public StoredSessionAttemptWithSession start(StoredWorkflowDefinitionWithRepository def,
            List<SessionMonitor> monitors, ZoneId timeZone, ScheduleTime time)
            throws ResourceNotFoundException, ResourceConflictException
    {
        AttemptRequest ar = AttemptRequest.builder()
            .repositoryId(def.getRepository().getId())
            .workflowName(def.getName())
            .instant(time.getScheduleTime())
            .retryAttemptName(Optional.absent())
            .defaultTimeZone(timeZone)
            .defaultParams(def.getRevisionDefaultParams())
            .overwriteParams(cf.create())
            .build();

        return exec.submitWorkflow(def.getRepository().getSiteId(),
                ar, def, monitors);
    }
}
