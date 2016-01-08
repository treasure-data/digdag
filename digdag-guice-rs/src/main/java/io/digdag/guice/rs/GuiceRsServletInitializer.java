package io.digdag.guice.rs;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import com.google.inject.Key;
import com.google.inject.Inject;
import com.google.inject.Injector;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

public abstract class GuiceRsServletInitializer
{
    protected String name = null;
    protected List<String> urlPatterns = new ArrayList<>();
    protected Map<String, String> initParameters = new HashMap<>();
    protected boolean asyncSupported = false;
    protected String runAsRole = null;

    public void setName(String name)
    {
        this.name = name;
    }

    public void addMapping(String urlPattern)
    {
        this.urlPatterns.add(urlPattern);
    }

    public void setInitParameter(String key, String value)
    {
        this.initParameters.put(key, value);
    }

    public void setAsyncSupported(boolean asyncSupported)
    {
        this.asyncSupported = asyncSupported;
    }

    public void setRunAsRole(String runAsRole)
    {
        this.runAsRole = runAsRole;
    }

    protected abstract Servlet initializeServlet(Injector injector);

    public void register(Injector injector, ServletContext context)
    {
        Servlet servlet = initializeServlet(injector);
        String servletName = name;
        if (servletName == null) {
            servletName = servlet.getClass().getName();
        }
        ServletRegistration.Dynamic reg = context.addServlet(servletName, servlet);
        reg.setAsyncSupported(asyncSupported);
        if (urlPatterns != null) {
            for (String urlPattern : urlPatterns) {
                reg.addMapping(urlPattern);
            }
        }
        if (initParameters != null && !initParameters.isEmpty()) {
            reg.setInitParameters(initParameters);
        }
        if (runAsRole != null) {
            reg.setRunAsRole(runAsRole);
        }
    }
}
