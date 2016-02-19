package io.digdag.standards.operator;

import java.util.List;
import java.util.ArrayList;
import java.nio.file.Path;
import com.google.common.collect.*;
import com.google.common.base.*;
import io.digdag.core.agent.RetryControl;
import io.digdag.core.agent.OperatorManager;
import io.digdag.spi.*;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;

public abstract class BaseOperator
        implements Operator
{
    protected final Path archivePath;
    protected final ArchiveFiles archive;
    protected final TaskRequest request;

    protected final List<Config> inputs;
    protected final List<Config> outputs;

    public BaseOperator(Path archivePath, TaskRequest request)
    {
        this.archivePath = archivePath;
        this.archive = new ArchiveFiles(archivePath);
        this.request = request;
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
    }

    public void addInput(Config input)
    {
        inputs.add(input);
    }

    public void addOutput(Config output)
    {
        outputs.add(output);
    }

    @Override
    public TaskResult run()
    {
        RetryControl retry = RetryControl.prepare(request.getConfig(), request.getLastStateParams(), true);
        try {
            try {
                return runTask();
            }
            finally {
                archive.close();
            }
        }
        catch (RuntimeException ex) {
            Config error = OperatorManager.makeExceptionError(request.getConfig().getFactory(), ex);
            boolean doRetry = retry.evaluate();
            if (doRetry) {
                throw new TaskExecutionException(ex, error,
                        retry.getNextRetryInterval(),
                        retry.getNextRetryStateParams());
            }
            else {
                throw ex;
            }
        }
    }

    public abstract TaskResult runTask();
}
