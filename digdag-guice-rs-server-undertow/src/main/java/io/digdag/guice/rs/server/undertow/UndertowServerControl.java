package io.digdag.guice.rs.server.undertow;

import com.google.inject.Injector;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.server.ServerLifeCycleManager;
import io.undertow.Undertow;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.servlet.api.DeploymentManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.XnioWorker;

public class UndertowServerControl
        implements GuiceRsServerControl
{
    private static final Logger logger = LoggerFactory.getLogger(UndertowServer.class);

    private UndertowServerRuntimeInfo runtimeInfo = new UndertowServerRuntimeInfo();
    private DeploymentManager deployment = null;
    private Injector injector = null;
    private List<GracefulShutdownHandler> handlers = Collections.synchronizedList(new ArrayList<>());
    private XnioWorker worker = null;
    private Undertow server = null;
    private boolean started = false;
    private ServerLifeCycleManager lifeCycleManager = null;

    UndertowServerControl()
    { }

    void deploymentInitialized(DeploymentManager deployment)
    {
        this.deployment = deployment;
    }

    void injectorInitialized(Injector injector)
    {
        this.injector = injector;
    }

    void addHandler(GracefulShutdownHandler handler)
    {
        this.handlers.add(handler);
    }

    void workerInitialized(XnioWorker worker)
    {
        this.worker = worker;
    }

    void serverInitialized(Undertow server)
    {
        this.server = server;
    }

    void serverStarted()
    {
        this.started = true;
    }

    boolean isServerStarted()
    {
        return started;
    }

    void postStart()
    {
        if (injector != null) {
            lifeCycleManager = injector.getInstance(ServerLifeCycleManager.class);
            try {
                lifeCycleManager.postStart();
            }
            catch (Exception ex) {
                throw ThrowablesUtil.propagate(ex);
            }
        }
    }

    @Override
    public UndertowServerRuntimeInfo getRuntimeInfo()
    {
        return runtimeInfo;
    }

    @Override
    public void stop()
    {
        logger.info("Started shutdown process");

        // stops background execution threads that may take time first
        // so that HTTP requests work during waiting for background tasks.
        if (lifeCycleManager != null) {
            try {
                lifeCycleManager.preStop();
            }
            catch (Exception ex) {
                // nothing we can do here excepting force killing this process
                throw ThrowablesUtil.propagate(ex);
            }
        }

        // closes HTTP listening channels
        logger.info("Closing HTTP listening sockets");
        if (server != null) {
            server.stop();
        }

        // HTTP handlers will return 503 Service Unavailable for new requests
        for (GracefulShutdownHandler handler : handlers) {
            handler.shutdown();
        }

        // waits for completion of currently handling requests upto 30 seconds
        logger.info("Waiting for completion of running HTTP requests...");
        for (GracefulShutdownHandler handler : handlers) {
            try {
                handler.awaitShutdown(30 * 1000);
            }
            catch (InterruptedException ex) {
                logger.info("Interrupted. Force shutting down running requests");
            }
        }

        // kills all processing threads. These threads should be already
        // idling because there're no currently handling requests.
        logger.info("Shutting down HTTP worker threads");
        if (worker != null) {
            worker.shutdownNow();
        }
    }

    @Override
    public void destroy()
    {
        logger.info("Shutting down system");
        try {
            // calls Servlet.destroy that is GuiceRsApplicationServlet.destroy defined at
            // RESTEasy HttpServlet30Dispatcher class
            deployment.stop();
        }
        catch (ServletException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
        finally {
            // calls ServletContextListener.contextDestroyed that calls @PreDestroy hooks
            // through GuiceRsServletContainerInitializer.CloseableInjectorDestroyListener listener
            deployment.undeploy();
        }
    }
}
