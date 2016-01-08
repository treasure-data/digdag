package io.digdag.guice.rs;

import java.util.Map;
import javax.servlet.ServletContext;
import com.google.inject.Module;
import com.google.inject.Binder;

public class GuiceRsCommandLineModule
        implements Module
{
    public static String getInitParameterKey()
    {
        return "io.digdag.guice.rs.commandLine";
    }

    public static String buildInitParameterValue(String[] args)
    {
        // TODO escape
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : args) {
            sb.append(s);
            if (first) {
                first = false;
            }
            else {
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    public static void setInitParameter(Map<String, String> map, String[] args)
    {
        map.put(getInitParameterKey(), buildInitParameterValue(args));
    }

    public static String[] restoreInitParameterValue(String value)
    {
        // TODO escape
        return value.split("\t");
    }

    public static GuiceRsCommandLineModule fromInitParameter(String value)
    {
        if (value == null) {
            return null;
        }
        return new GuiceRsCommandLineModule(restoreInitParameterValue(value));
    }

    public static GuiceRsCommandLineModule fromInitParameter(ServletContext context)
    {
        return fromInitParameter(context.getInitParameter(getInitParameterKey()));
    }

    public static GuiceRsCommandLineModule fromInitParameter(Map<String, String> parameters)
    {
        return fromInitParameter(parameters.get(getInitParameterKey()));
    }

    private final String[] args;

    public GuiceRsCommandLineModule(String[] args)
    {
        this.args = args;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(String[].class).annotatedWith(ForCommandLine.class).toInstance(args);
        binder.bind(GuiceRsCommandLine.class);
    }
}
