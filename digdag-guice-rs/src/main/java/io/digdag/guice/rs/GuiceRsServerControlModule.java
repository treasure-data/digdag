package io.digdag.guice.rs;

import java.util.Map;
import javax.servlet.ServletContext;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Provider;
import io.digdag.commons.ThrowablesUtil;

public class GuiceRsServerControlModule
        implements Module
{
    public static String getInitParameterKey()
    {
        return "io.digdag.guice.rs.servletControl";
    }

    public static String buildInitParameterValue(Class<? extends Provider<GuiceRsServerControl>> clazz)
    {
        return clazz.getName();
    }

    public static void setInitParameter(Map<String, String> map, Class<? extends Provider<GuiceRsServerControl>> clazz)
    {
        map.put(getInitParameterKey(), buildInitParameterValue(clazz));
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends Provider<GuiceRsServerControl>> restoreInitParameterValue(String value)
    {
        try {
            // TODO which class loader should here use?
            return (Class<? extends Provider<GuiceRsServerControl>>) Class.forName(value);
        }
        catch (ClassNotFoundException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    public static GuiceRsServerControlModule fromInitParameter(String value)
    {
        if (value == null) {
            return null;
        }
        return new GuiceRsServerControlModule(restoreInitParameterValue(value));
    }

    public static GuiceRsServerControlModule fromInitParameter(ServletContext context)
    {
        return fromInitParameter(context.getInitParameter(getInitParameterKey()));
    }

    public static GuiceRsServerControlModule fromInitParameter(Map<String, String> parameters)
    {
        return fromInitParameter(parameters.get(getInitParameterKey()));
    }

    private final Class<?> clazz;

    @SuppressWarnings("unchecked")
    public GuiceRsServerControlModule(Class<? extends Provider<GuiceRsServerControl>> clazz)
    {
        this.clazz = clazz;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Binder binder)
    {
        binder.bind(GuiceRsServerControl.class).toProvider((Class<? extends Provider<GuiceRsServerControl>>) clazz);
    }
}
