package io.digdag.cli;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;

public abstract class Command
{
    @Parameter()
    protected List<String> args = new ArrayList<>();

    @Parameter(names = {"-L", "--log"})
    protected String logPath = "-";

    @Parameter(names = {"-l", "--log-level"})
    protected String logLevel = "info";

    @DynamicParameter(names = "-X")
    protected Map<String, String> systemProperties = new HashMap<>();

    @Parameter(names = {"-help", "--help"}, help = true, hidden = true)
    protected boolean help;

    public abstract void main() throws Exception;

    public abstract SystemExitException usage(String error);
}
