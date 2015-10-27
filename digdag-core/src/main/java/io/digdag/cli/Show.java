package io.digdag.cli;

import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Injector;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import io.digdag.core.*;
import io.digdag.cli.Main.SystemExitException;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Show
{
    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        parser.acceptsAll(asList("s", "show")).withRequiredArg().ofType(String.class);

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        File visualizePath = new File(Optional.fromNullable((String) op.valueOf("s")).or("workflow.png"));
        File workflowPath = new File(argv.get(0));

        new Show().show(workflowPath, visualizePath);
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag show <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -s, --show PATH.png              store a PNG file to this path (default: workflow.png)");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    public void show(File workflowPath, File visualizePath)
            throws Exception
    {
        Injector injector = Main.embed().getInjector();

        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);

        final ConfigSource ast = loader.loadFile(workflowPath);

        List<Workflow> workflows = ast.getKeys()
            .stream()
            .map(key -> compiler.compile(key, ast.getNested(key)))
            .collect(Collectors.toList());

        Workflow workflow = workflows.get(0);
        show(workflow.getTasks()
                .stream()
                .map(task -> WorkflowVisualizerNode.of(task))
                .collect(Collectors.toList()),
            visualizePath);
    }

    public static void show(List<WorkflowVisualizerNode> nodes, File path)
            throws InterruptedException
    {
        new GraphvizWorkflowVisualizer().visualize(nodes, path);
        System.err.println("Stored PNG file at '"+path+"'");
    }
}
