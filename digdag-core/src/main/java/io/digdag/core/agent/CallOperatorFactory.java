package io.digdag.core.agent;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Path;

import com.google.inject.Inject;
import com.google.common.base.Throwables;
import io.digdag.spi.TaskExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.util.Workspace;
import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;

public class CallOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(CallOperatorFactory.class);

    private final TaskCallbackApi callback;
    private final ConfigLoaderManager configLoader;

    @Inject
    public CallOperatorFactory(TaskCallbackApi callback, ConfigLoaderManager configLoader)
    {
        this.callback = callback;
        this.configLoader = configLoader;
    }

    public String getType()
    {
        return "call";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new CallOperator(workspacePath, request);
    }

    private class CallOperator
            implements Operator
    {
        private final Workspace workspace;
        private final TaskRequest request;

        public CallOperator(Path workspacePath, TaskRequest request)
        {
            this.workspace = new Workspace(workspacePath);
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config config = request.getConfig();

            String workflowName = config.get("_command", String.class);
            Config exportParams = config.getNestedOrGetEmpty("params");

            Config def;
            if (workflowName.endsWith(WORKFLOW_FILE_SUFFIX)) {
                Path path = workspace.getPath(workflowName);
                try {
                    def = configLoader.loadParameterizedFile(path.toFile());
                }
                catch (FileNotFoundException ex) {
                    throw new ConfigException("File does not exist: " + workflowName);
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }
            else {
                int projectId = config.get("project_id", int.class);

                try {
                    def = callback.getWorkflowDefinition(
                            request.getSiteId(),
                            projectId,
                            workflowName);
                }
                catch (ResourceNotFoundException ex) {
                    throw new ConfigException(ex);
                }
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(def)
                .exportParams(exportParams)
                .build();
        }
    }
}
