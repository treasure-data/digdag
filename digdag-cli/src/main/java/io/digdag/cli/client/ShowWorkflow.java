package io.digdag.cli.client;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Optional;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestWorkflowDefinition;

import javax.ws.rs.NotFoundException;

import java.util.List;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowWorkflow
    extends ClientCommand
{
    @Parameter(names = {"--last-id"})
    Id lastId = null;
        
    @Parameter(names = {"--count"})
    int count = 100;

    @Parameter(names = {"--name"})
    String namePattern = null;

    @Parameter(names = {"--order"}, validateWith = OrderValidator.class)
    String order = "asc";

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
        err.println("Usage: " + programName + " workflows [project-name] [name]");
        err.println("  Options:");
        err.println("    --count number   number of workflows");
        err.println("    --last-id id     last id of workflow");
        err.println("    --order (asc|desc) order of listing default is `asc`. work only without `project-name` argument");
        err.println("    --name name      search by part of workflow name. work only without `project-name` argument");
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
            for (RestWorkflowDefinition def : client.getWorkflowDefinitions(proj.getId()).getWorkflows()) {
                ln("    %s", def.getName());
            }
        }
        else {
            List<RestWorkflowDefinition> defs = client.getWorkflowDefinitions(
                    Optional.fromNullable(lastId),
                    count,
                    order,
                    Optional.fromNullable(namePattern)
            ).getWorkflows();
            String lastProjName = null;
            for (RestWorkflowDefinition def : defs) {
                if (!def.getProject().getName().equals(lastProjName)) {
                    ln("  %s", def.getProject().getName());
                    lastProjName = def.getProject().getName();
                }
                ln("    %s", def.getName());
            }
        }
        ln("");
        err.println("Use `" + programName + " workflows <project-name> <name>` to show details.");
    }

    private void showWorkflowDetails(String projName, String defName)
        throws Exception
    {
        DigdagClient client = buildClient();

        if (projName != null) {
            RestProject proj = client.getProject(projName);
            RestWorkflowDefinition def = client.getWorkflowDefinition(proj.getId(), defName);
            String yaml = yamlMapper().toYaml(def.getConfig());
            ln("%s", yaml);
        }
        else {
            for (RestProject proj : client.getProjects().getProjects()) {
                try {
                    RestWorkflowDefinition def = client.getWorkflowDefinition(proj.getId(), defName);
                    String yaml = yamlMapper().toYaml(def.getConfig());
                    ln("%s", yaml);
                    return;
                }
                catch (NotFoundException ex) {
                }
            }
            throw systemExit("Workflow definition '" + defName + "' does not exist.");
        }
    }

    //ToDo move to outer of the class if other command need same parameter
    public static class OrderValidator implements IParameterValidator
    {
        @Override
        public void validate(String name, String value)
                throws ParameterException
        {
            if (value.equals("asc") || value.equals("desc")) {
                return;
            }
            throw new ParameterException("Invalid parameter value for " + name + ": " + value);
        }
    }
}
