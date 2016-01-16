package io.digdag.server;

import java.util.List;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import javax.servlet.ServletContext;
import com.google.common.base.Optional;
import com.google.inject.Injector;
import io.digdag.core.DigdagEmbed;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.core.LocalSite;
import io.digdag.core.repository.*;
import io.digdag.core.yaml.YamlConfigLoader;
import io.digdag.spi.config.*;

public class ServerBootstrap
        implements GuiceRsBootstrap
{
    @Override
    public Injector initialize(ServletContext context)
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(new ServerModule())
            .initialize()
            .getInjector();

        /* TODO use Dagfile
        String workflowPath = context.getInitParameter("io.digdag.server.workflowPath");
        if (workflowPath != null) {
            File path = new File(workflowPath);
            LocalSite site = injector.getInstance(LocalSite.class);
            YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);
            ConfigFactory cf = injector.getInstance(ConfigFactory.class);
            // TODO use ArgumentConfigLoader
            try {
                List<WorkflowSource> workflowSources = loader.loadFile(path, Optional.of(new File(new File(path.getAbsolutePath()).getParent())), Optional.of(cf.create())).convert(WorkflowSourceList.class).get();
                site.scheduleWorkflows(workflowSources, new Date());
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            site.startLocalAgent();
            site.startMonitor();

            Thread thread = new Thread(() -> {
                try {
                    site.run();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }, "local-site");
            thread.setDaemon(true);
            thread.start();
        }
        */

        return injector;
    }
}
