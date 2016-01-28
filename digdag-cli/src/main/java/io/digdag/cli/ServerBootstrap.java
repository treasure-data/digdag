package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.io.File;
import java.time.ZoneId;
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import com.google.common.collect.ImmutableList;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.server.ServerModule;
import io.digdag.core.database.DatabaseStoreConfig;
import io.digdag.core.agent.ArchiveManager;
import io.digdag.core.agent.InProcessArchiveManager;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.client.config.ConfigFactory;

public class ServerBootstrap
    implements GuiceRsBootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    private GuiceRsServerControl control;

    @Inject
    public ServerBootstrap(GuiceRsServerControl control)
    {
        this.control = control;
    }

    @Override
    public Injector initialize(ServletContext context)
    {
        final String database = context.getInitParameter("io.digdag.cli.server.database");

        Injector injector = new DigdagEmbed.Bootstrap()
            .addModules(new ServerModule())
            .addModules((binder) -> {
                binder.bind(RevisionAutoReloader.class).in(Scopes.SINGLETON);
            })
            .overrideModules((list) -> ImmutableList.of(Modules.override(list).with((binder) -> {
                binder.bind(ArchiveManager.class).to(InProcessArchiveManager.class).in(Scopes.SINGLETON);
                if (database != null) {
                    new File(database).mkdirs();
                    // override default memory database
                    String path = database + "/digdag";
                    binder.bind(DatabaseStoreConfig.class).toInstance(
                            DatabaseStoreConfig.builder()
                            .type("h2")
                            //.url("jdbc:h2:./test;DB_CLOSE_ON_EXIT=FALSE")
                            .url("jdbc:h2:" + path + ";DB_CLOSE_ON_EXIT=FALSE")
                            .build());
                }
            })))
            .initialize()
            .getInjector();

        // TODO create global site
        LocalSite site = injector.getInstance(LocalSite.class);

        String autoLoadFile = context.getInitParameter("io.digdag.cli.server.autoLoadFile");
        if (autoLoadFile != null) {
            ConfigFactory cf = injector.getInstance(ConfigFactory.class);
            RevisionAutoReloader autoReloader = injector.getInstance(RevisionAutoReloader.class);
            try {
                autoReloader.loadFile(new File(autoLoadFile), ZoneId.systemDefault(), cf.create());
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        // start server
        site.startLocalAgent();
        site.startMonitor();

        Thread thread = new Thread(() -> {
            try {
                site.run();
            }
            catch (Exception ex) {
                logger.error("Uncaught error", ex);
                control.destroy();
            }
        }, "local-site");
        thread.setDaemon(true);
        thread.start();

        return injector;
    }
}
