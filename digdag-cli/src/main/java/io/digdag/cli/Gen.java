package io.digdag.cli;

import java.util.List;
import java.io.File;
import java.io.PrintStream;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import io.digdag.cli.Main.SystemExitException;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Gen
{
    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        new Gen().gen(argv.get(0));
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag gen <name>");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    public void gen(String name)
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
