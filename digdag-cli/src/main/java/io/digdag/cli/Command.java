package io.digdag.cli;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import io.digdag.core.config.PropertyUtils;
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

    protected Properties loadSystemProperties()
        throws IOException
    {
        Properties props;

        // load from ~/.digdag/config
        try {
            props = PropertyUtils.loadFile(
                    Paths.get(System.getProperty("user.home")).resolve(".digdag").resolve("config").toFile()
                    );
        }
        catch (FileNotFoundException ex) {
            // ignore if file doesn't exist
            props = new Properties();
        }

        // system properties (mostly given by cmdline arguments) overwrite
        props.putAll(System.getProperties());

        return props;
    }
}
