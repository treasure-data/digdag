package io.digdag.cli.client;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.UUID;

import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.core.Version;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.client.ProjectUtil.showUploadedProject;

public class Upload
    extends ClientCommand
{
    @Parameter(names = {"-r", "--revision"})
    String revision = null;

    public Upload(Version version, Map<String, String> env, PrintStream out, PrintStream err)
    {
        super(version, env, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 2) {
            throw usage(null);
        }
        upload(args.get(0), args.get(1));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag upload <path.tar.gz> <project>");
        err.println("  Options:");
        err.println("    -r, --revision REVISION          specific revision name instead of auto-generated UUID");
        //err.println("        --time-revision              use current time as the revision name");
        showCommonOptions();
        return systemExit(error);
    }

    private void upload(String path, String projName)
        throws IOException, SystemExitException
    {
        DigdagClient client = buildClient();
        if (revision == null) {
            revision = generateDefaultRevisionName();
        }
        RestProject proj = client.putProjectRevision(projName, revision, new File(path));
        showUploadedProject(out, proj, format);

        err.println("");
        err.println("Use `digdag workflows` to show all workflows.");
    }

    public static String generateDefaultRevisionName()
    {
        return UUID.randomUUID().toString();
    }
}
