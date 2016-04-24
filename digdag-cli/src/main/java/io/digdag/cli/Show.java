package io.digdag.cli;

import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import com.google.inject.Injector;
import com.beust.jcommander.Parameter;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.cli.SystemExitException.systemExit;

public class Show
    extends Command
{
    @Parameter(names = {"-o", "--output"})
    String output = "digdag.png";

    // TODO support -p option? for jinja template rendering

    public Show(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        show(args.get(0));
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag show <digdag.yml> [options...]");
        err.println("  Options:");
        err.println("    -s, --show PATH.png              store a PNG file to this path (default: digdag.png)");
        Main.showCommonOptions(err);
        return systemExit(error);
    }

    private void show(String workflowPath)
            throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .withWorkflowExecutor(false)
            .withScheduleExecutor(false)
            .withLocalAgent(false)
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);

        List<WorkflowDefinition> workflowSources = loader.loadParameterizedFile(new File(workflowPath), cf.create()).convert(WorkflowDefinitionList.class).get();

        List<Workflow> workflows = workflowSources
            .stream()
            .map(wf -> compiler.compile(wf.getName(), wf.getConfig()))
            .collect(Collectors.toList());

        Workflow workflow = workflows.get(0);

        List<WorkflowVisualizerNode> nodes = workflow.getTasks()
            .stream()
            .map(task -> WorkflowVisualizerNode.of(task))
            .collect(Collectors.toList());

        show(nodes, new File(output));
    }

    // used also by Run.run
    void show(List<WorkflowVisualizerNode> nodes, File path)
            throws InterruptedException
    {
        new GraphvizWorkflowVisualizer().visualize(nodes, path);
        err.println("Stored PNG file at '"+path+"'");
    }
}
