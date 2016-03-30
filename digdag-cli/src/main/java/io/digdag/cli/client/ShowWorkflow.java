package io.digdag.cli.client;

import java.util.List;
import com.beust.jcommander.Parameter;
import javax.ws.rs.NotFoundException;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestRepository;
import io.digdag.client.api.RestWorkflowDefinition;
import static io.digdag.cli.Main.systemExit;

public class ShowWorkflow
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 0:
            showWorkflows(null);
            break;
        case 1:
            if (args.get(0).startsWith("+")) {
                showWorkflowDetails(null, args.get(0));
            }
            else {
                showWorkflows(args.get(0));
            }
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
        System.err.println("Usage: digdag workflows [repo-name] [+name]");
        System.err.println("  Options:");
        System.err.println("    -r, --repository NAME            repository name");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showWorkflows(String repoName)
        throws Exception
    {
        DigdagClient client = buildClient();

        if (repoName != null) {
            RestRepository repo = client.getRepository(repoName);
            ln("  %s", repo.getName());
            for (RestWorkflowDefinition def : client.getWorkflowDefinitions(repo.getId())) {
                ln("    %s", def.getName());
            }
        }
        else {
            for (RestRepository repo : client.getRepositories()) {
                try {
                    List<RestWorkflowDefinition> defs = client.getWorkflowDefinitions(repo.getId());
                    ln("  %s", repo.getName());
                    for (RestWorkflowDefinition def : defs) {
                        ln("    %s", def.getName());
                    }
                }
                catch (NotFoundException ex) {
                }
            }
        }
        ln("");
        System.err.println("Use `digdag workflows +NAME` to show details.");
    }

    public void showWorkflowDetails(String repoName, String defName)
        throws Exception
    {
        DigdagClient client = buildClient();

        if (repoName != null) {
            RestRepository repo = client.getRepository(repoName);
            RestWorkflowDefinition def = client.getWorkflowDefinition(repo.getId(), defName);
            String yaml = yamlMapper().toYaml(def.getConfig());
            ln("%s", yaml);
        }
        else {
            for (RestRepository repo : client.getRepositories()) {
                try {
                    RestWorkflowDefinition def = client.getWorkflowDefinition(repo.getId(), defName);
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
}
