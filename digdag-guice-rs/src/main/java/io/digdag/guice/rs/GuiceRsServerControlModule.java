package io.digdag.guice.rs;

import java.util.Map;
import javax.servlet.ServletContext;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.common.base.Throwables;

public class GuiceRsServerControlModule
        implements Module
{
    public static String getInitParameterKey()
    {
        return "io.digdag.guice.rs.servletControl";
    }

    public static String buildInitParameterValue(Class<? extends GuiceRsServerControl> clazz)
    {
        return clazz.getName();
    }

    public static void setInitParameter(Map<String, String> map, Class<? extends GuiceRsServerControl> clazz)
    {
        map.put(getInitParameterKey(), buildInitParameterValue(clazz));
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends GuiceRsServerControl> restoreInitParameterValue(String value)
    {
        try {
            // TODO which class loader should here use?
            return (Class<? extends GuiceRsServerControl>) Class.forName(value);
        }
        catch (ClassNotFoundException ex) {
            throw Throwables.propagate(ex);
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
    public GuiceRsServerControlModule(Class<? extends GuiceRsServerControl> clazz)
    {
        this.clazz = clazz;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Binder binder)
    {
        binder.bind(GuiceRsServerControl.class).to((Class<? extends GuiceRsServerControl>) clazz);
    }
}
