package io.digdag.cli.client;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.io.File;
import com.google.common.collect.ImmutableList;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.cli.Run;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestProject;
import static io.digdag.cli.Main.systemExit;

public class Push
    extends ClientCommand
{
    @Parameter(names = {"-f", "--file"})
    String dagfilePath = Run.DEFAULT_DAGFILE;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    @Parameter(names = {"-r", "--revision"})
    String revision = null;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        if (revision == null) {
            throw usage("-r, --revision option is required");
        }
        push(args.get(0));
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag push <project> -r <revision>");
        System.err.println("  Options:");
        System.err.println("    -f, --file PATH                  use this file to load a project (default: digdag.yml)");
        System.err.println("    -r, --revision REVISION          revision name");
        System.err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        //System.err.println("        --time-revision              use current time as the revision name");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void push(String projName)
        throws Exception
    {
        String path = "digdag.archive.tar.gz";
        new File(path).deleteOnExit();
        Archive.archive(dagfilePath, params, paramsFile, path);

        DigdagClient client = buildClient();
        RestProject proj = client.putProjectRevision(projName, revision, new File(path));
        Upload.showUploadedProject(proj);
    }
}
