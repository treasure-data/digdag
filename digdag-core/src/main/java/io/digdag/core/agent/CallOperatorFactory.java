package io.digdag.core.agent;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowFile;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.util.BaseOperator;

import java.io.IOException;
import java.nio.file.Path;

import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;
import static io.digdag.util.Workspace.propagateIoException;

public class CallOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(CallOperatorFactory.class);

    private final ProjectArchiveLoader projectLoader;

    @Inject
    public CallOperatorFactory(ProjectArchiveLoader projectLoader)
    {
        this.projectLoader = projectLoader;
    }

    public String getType()
    {
        return "call";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        System.out.println("=============== newOperator ================"); // きてない
        return new CallOperator(context);
    }

    private class CallOperator
            extends BaseOperator
    {
        public CallOperator(OperatorContext context)
        {
            super(context);
        }

        @Override
        public TaskResult runTask()
        {
            Config config = request.getConfig();

            String workflowFileName = config.get("_command", String.class);
            if (!workflowFileName.endsWith(WORKFLOW_FILE_SUFFIX)) {
                workflowFileName += WORKFLOW_FILE_SUFFIX;
            }

            Path workflowPath = workspace.getPath(workflowFileName);

            WorkflowFile workflowFile;
            try {
                workflowFile = projectLoader.loadWorkflowFileFromPath(
                        workspace.getProjectPath(), workflowPath, config);
            }
            catch (IOException ex) {
                throw propagateIoException(ex, workflowFileName, ConfigException::new);
            }

            Config def = workflowFile.toWorkflowDefinition().getConfig();

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(def)
                .build();
        }
    }
}
