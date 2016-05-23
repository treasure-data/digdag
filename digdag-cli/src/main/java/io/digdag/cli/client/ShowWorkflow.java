package io.digdag.cli.client;

import java.io.PrintStream;
import java.util.List;

import javax.ws.rs.NotFoundException;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestProject;
import io.digdag.core.Version;
import org.hjson.JsonValue;
import org.hjson.Stringify;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowWorkflow
    extends ClientCommand
{
    public ShowWorkflow(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
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

        if (projName != null) {
            RestProject proj = client.getProject(projName);
            ln("  %s", proj.getName());
            for (RestWorkflowDefinition def : client.getWorkflowDefinitions(proj.getId())) {
                ln("    %s", def.getName());
            }
        }
        else {
            for (RestProject proj : client.getProjects()) {
                try {
                    List<RestWorkflowDefinition> defs = client.getWorkflowDefinitions(proj.getId());
                    ln("  %s", proj.getName());
                    for (RestWorkflowDefinition def : defs) {
                        ln("    %s", def.getName());
                    }
                }
                catch (NotFoundException ex) {
                }
            }
        }
        ln("");
        err.println("Use `digdag workflows <project-name> <name>` to show details.");
    }

    private void showWorkflowDetails(String projName, String defName)
        throws Exception
    {
        DigdagClient client = buildClient();

        if (projName != null) {
            RestProject proj = client.getProject(projName);
            RestWorkflowDefinition def = client.getWorkflowDefinition(proj.getId(), defName);
            String dig = JsonValue.readJSON(def.getConfig().toString()).toString(Stringify.HJSON);
            ln("%s", dig);
        }
        else {
            for (RestProject proj : client.getProjects()) {
                try {
                    RestWorkflowDefinition def = client.getWorkflowDefinition(proj.getId(), defName);
                    String dig = JsonValue.readJSON(def.getConfig().toString()).toString(Stringify.HJSON);
                    ln("%s", dig);
                    return;
                }
                catch (NotFoundException ex) {
                }
            }
            throw systemExit("Workflow definition '" + defName + "' does not exist.");
        }
    }
}
