package io.digdag.cli;

import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import com.google.inject.Injector;
import com.beust.jcommander.Parameter;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.WorkflowSourceList;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.spi.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;

public class Show
    extends Command
{
    @Parameter(names = {"-o", "--output"})
    String output = "workflow.png";

    // TODO support -p option? for jinja template rendering

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
        System.err.println("Usage: digdag show <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -s, --show PATH.png              store a PNG file to this path (default: workflow.png)");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    private void show(String workflowPath)
            throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);

        List<WorkflowSource> workflowSources = loader.load(new File(workflowPath), cf.create()).convert(WorkflowSourceList.class).get();

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
    static void show(List<WorkflowVisualizerNode> nodes, File path)
            throws InterruptedException
    {
        new GraphvizWorkflowVisualizer().visualize(nodes, path);
        System.err.println("Stored PNG file at '"+path+"'");
    }
}
