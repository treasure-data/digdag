package io.digdag.cli.client;

import io.digdag.cli.EntityCollectionPrinter;
import io.digdag.cli.EntityPrinter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.core.Version;

import javax.ws.rs.NotFoundException;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowWorkflow
    extends ClientCommand
{
    public ShowWorkflow(Version version, Map<String, String> env, PrintStream out, PrintStream err)
    {
        super(version, env, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 0:
            showWorkflows(null);
            break;
        case 1:
            showWorkflows(args.get(0));
            break;
        case 2:
            showWorkflowDetails(args.get(0), args.get(1));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag workflows [project-name] [name]");
        showCommonOptions();
        return systemExit(error);
    }

    private void showWorkflows(String projName)
        throws Exception
    {
        DigdagClient client = buildClient();

        EntityCollectionPrinter<RestWorkflowDefinition> formatter = new EntityCollectionPrinter<>();
        formatter.field("PROJECT", wf -> wf.getProject().getName());
        formatter.field("PROJECT ID", wf -> Integer.toString(wf.getProject().getId()));
        formatter.field("WORKFLOW", RestWorkflowDefinition::getName);
        formatter.field("REVISION", RestWorkflowDefinition::getRevision);

        List<RestWorkflowDefinition> defs;
        if (projName != null) {
            RestProject proj = client.getProject(projName);
            defs = client.getWorkflowDefinitions(proj.getId());
        }
        else {
            defs = new ArrayList<>();
            for (RestProject proj : client.getProjects()) {
                try {
                    defs.addAll(client.getWorkflowDefinitions(proj.getId()));
                }
                catch (NotFoundException ex) {
                    continue;
                }
            }
        }
        formatter.print(format, defs, out);
        out.println();
        out.flush();
        err.println("Use `digdag workflows <project-name> <name>` to show details.");
    }

    private void showWorkflowDetails(String projName, String defName)
            throws Exception
    {
        DigdagClient client = buildClient();

        RestProject proj = client.getProject(projName);
        RestWorkflowDefinition def = client.getWorkflowDefinition(proj.getId(), defName);

        EntityPrinter<RestWorkflowDefinition> formatter = new EntityPrinter<>();

        formatter.field("project", wf -> wf.getProject().getName());
        formatter.field("project id", wf -> Integer.toString(wf.getProject().getId()));
        formatter.field("workflow", RestWorkflowDefinition::getName);
        formatter.field("revision", RestWorkflowDefinition::getRevision);

        formatter.print(format, def, out);
    }
}
