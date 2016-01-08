package io.digdag.guice.rs;

import java.util.Map;
import javax.servlet.ServletContext;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.common.base.Throwables;

public class GuiceRsServerControllerModule
        implements Module
{
    public static String getInitParameterKey()
    {
        return "io.digdag.guice.rs.servletController";
    }

    public static String buildInitParameterValue(Class<? extends GuiceRsServerController> clazz)
    {
        return clazz.getName();
    }

    public static void setInitParameter(Map<String, String> map, Class<? extends GuiceRsServerController> clazz)
    {
        map.put(getInitParameterKey(), buildInitParameterValue(clazz));
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends GuiceRsServerController> restoreInitParameterValue(String value)
    {
        try {
            // TODO which class loader should here use?
            return (Class<? extends GuiceRsServerController>) Class.forName(value);
        }
        catch (ClassNotFoundException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public static GuiceRsServerControllerModule fromInitParameter(String value)
    {
        return new GuiceRsServerControllerModule(restoreInitParameterValue(value));
    }

    public static GuiceRsServerControllerModule fromInitParameter(ServletContext context)
    {
        return fromInitParameter(context.getInitParameter(getInitParameterKey()));
    }

    public static GuiceRsServerControllerModule fromInitParameter(Map<String, String> parameters)
    {
        return fromInitParameter(parameters.get(getInitParameterKey()));
    }

    private final Class<?> clazz;

    @SuppressWarnings("unchecked")
    public GuiceRsServerControllerModule(Class<? extends GuiceRsServerController> clazz)
    {
        this.clazz = clazz;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Binder binder)
    {
        binder.bind(GuiceRsServerController.class).to((Class<? extends GuiceRsServerController>) clazz);
    }
}
