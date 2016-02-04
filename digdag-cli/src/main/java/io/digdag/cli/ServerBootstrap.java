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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.server.ServerModule;
import io.digdag.server.ServerConfig;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.agent.ArchiveManager;
import io.digdag.core.agent.InProcessArchiveManager;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;

public class ServerBootstrap
    implements GuiceRsBootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    public static final String CONFIG_INIT_PARAMETER_KEY = "io.digdag.cli.server.config";

    private GuiceRsServerControl control;

    @Inject
    public ServerBootstrap(GuiceRsServerControl control)
    {
        this.control = control;
    }

    @Override
    public Injector initialize(ServletContext context)
    {
        ConfigElement systemConfig;
        String configJson = context.getInitParameter(CONFIG_INIT_PARAMETER_KEY);
        if (configJson == null) {
            systemConfig = ConfigElement.empty();
        }
        else {
            systemConfig = ConfigElement.fromJson(configJson);
        }

        ServerConfig serverConfig = ServerConfig.convertFrom(systemConfig);
        Optional<String> autoLoadLocalDagfile = serverConfig.getAutoLoadLocalDagfile();

        Injector injector = new DigdagEmbed.Bootstrap()
            .setSystemConfig(systemConfig)
            .addModules(new ServerModule())
            .addModules((binder) -> {
                binder.bind(RevisionAutoReloader.class).in(Scopes.SINGLETON);
                binder.bind(ServerConfig.class).toInstance(serverConfig);
            })
            .overrideModules((list) -> ImmutableList.of(Modules.override(list).with((binder) -> {
                if (autoLoadLocalDagfile.isPresent()) {
                    // default is CurrentDirectoryArchiveManager
                }
                else {
                    binder.bind(ArchiveManager.class).to(InProcessArchiveManager.class).in(Scopes.SINGLETON);
                }
            })))
            .initialize()
            .getInjector();

        // TODO create global site
        LocalSite site = injector.getInstance(LocalSite.class);

        if (autoLoadLocalDagfile.isPresent()) {
            ConfigFactory cf = injector.getInstance(ConfigFactory.class);
            RevisionAutoReloader autoReloader = injector.getInstance(RevisionAutoReloader.class);
            try {
                autoReloader.loadFile(new File(autoLoadLocalDagfile.get()), ZoneId.systemDefault(), cf.create());
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
