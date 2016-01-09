package io.digdag.cli;

import java.io.File;
import java.io.PrintStream;
import static io.digdag.cli.Main.systemExit;

public class Gen
    extends Command
{
    @Override
    public void main()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        gen(args.get(0));
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag gen <name>");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void gen(String name)
        throws Exception
    {
        File file = new File(name+".yml");
        if (file.exists()) {
            throw systemExit("File "+file+" already exists.");
        }

        PrintStream out = new PrintStream(file);
        out.println(name+":");
        out.println("  +ls:");
        out.println("    sh>: ls -l");
        out.println("  +echo:");
        out.println("    sh>: echo ok");
        out.println("  +show:");
        out.println("    sh>: echo ok");

        System.out.println("Generated "+file+". Use `digdag run "+file+"` to run this workflow.");
    }
}
