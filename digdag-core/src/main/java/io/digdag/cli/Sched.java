package io.digdag.cli;

import java.util.List;
import java.util.Date;
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

public class Sched
{
    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        parser.acceptsAll(asList("o", "output")).withRequiredArg().ofType(String.class);

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        File workflowPath = new File(argv.get(0));

        String workflowFileName = workflowPath.getName().replaceFirst("\\.\\w*$", "");
        File outputPath = Optional.fromNullable((String) op.valueOf("s")).transform(it -> new File(it)).or(
                    new File(workflowPath.getParent(), workflowFileName));

        new Sched().sched(workflowPath, outputPath);
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag sched <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -o, --output DIR                  store execution results to this directory (default: same name with workflow file name)");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    public void sched(File workflowPath, File outputPath)
            throws Exception
    {
        // TODO override ScheduleStarter to store state on local filesystem

        Injector injector = Main.embed().getInjector();
        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigSourceFactory cf = injector.getInstance(ConfigSourceFactory.class);
        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);

        outputPath.mkdir();

        List<WorkflowSource> workflowSources = Run.loadWorkflowSources(loader.loadFile(workflowPath));

        localSite.scheduleWorkflows(workflowSources, new Date());

        localSite.runUntilAny();
    }
}
