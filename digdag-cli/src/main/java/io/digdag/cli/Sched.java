package io.digdag.cli;

import com.beust.jcommander.Parameter;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.executor.DigdagEmbed;
import io.digdag.client.Version;
import io.digdag.core.agent.LocalWorkspaceManager;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.PropertyUtils;
import io.digdag.server.ServerBootstrap;
import io.digdag.server.ServerConfig;
import org.embulk.guice.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

import java.util.Properties;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.Arguments.loadProject;
import static io.digdag.cli.SystemExitException.systemExit;

public class Sched
    extends Server
{
    private static final Logger logger = LoggerFactory.getLogger(Sched.class);

    private static final String SYSTEM_CONFIG_AUTO_LOAD_LOCAL_PROJECT_KEY = "scheduler.autoLoadLocalProject";
    private static final String SYSTEM_CONFIG_LOCAL_OVERRIDE_PARAMS = "scheduler.localOverrideParams";

    @Parameter(names = {"--project"})
    String projectDirName = null;

    // TODO no-schedule mode

    @Override
    public void main()
            throws Exception
    {
        JvmUtil.validateJavaRuntime(err);

        if (args.size() != 0) {
            throw usage(null);
        }

        startScheduler();
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " sched [options...]");
        err.println("  Options:");
        err.println("        --project DIR                use this directory as the project directory (default: current directory)");
        err.println("    -n, --port PORT                  port number to listen for web interface and api clients (default: 65432)");
        err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: 127.0.0.1)");
        err.println("    -o, --database DIR               store status to this database");
        err.println("    -O, --task-log DIR               store task logs to this path");
        err.println("        --max-task-threads N         limit maximum number of task execution threads");
        err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        Main.showCommonOptions(env, err);
        return systemExit(error);
    }

    private void startScheduler()
            throws Exception
    {
        Properties props;

        try (DigdagEmbed digdag = new DigdagEmbed.Bootstrap()
                .withWorkflowExecutor(false)
                .withScheduleExecutor(false)
                .withLocalAgent(false)
                .initializeWithoutShutdownHook()) {
            Injector injector = digdag.getInjector();

            ConfigFactory cf = injector.getInstance(ConfigFactory.class);
            ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);

            Config overrideParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

            props = buildServerProperties();

            // use memory database by default
            if (!props.containsKey("database.type")) {
                props.setProperty("database.type", "memory");
            }

            props.setProperty(SYSTEM_CONFIG_AUTO_LOAD_LOCAL_PROJECT_KEY, projectDirName != null ? projectDirName : "");  // Properties can't store null
            props.setProperty(SYSTEM_CONFIG_LOCAL_OVERRIDE_PARAMS, overrideParams.toString());
        }

        ConfigElement ce = PropertyUtils.toConfigElement(props);
        ServerConfig serverConfig = ServerConfig.convertFrom(ce);

        // this method doesn't block. it starts some non-daemon threads, setup shutdown handlers, and returns immediately
        ServerBootstrap.start(new SchedulerServerBootStrap(version, serverConfig));
    }

    public static class SchedulerServerBootStrap
            extends ServerBootstrap
    {
        public SchedulerServerBootStrap(Version version, ServerConfig serverConfig)
        {
            super(version, serverConfig);
        }

        @Override
        public Bootstrap bootstrap()
        {
            return super.bootstrap()
                .addModules((binder) -> {
                    binder.bind(RevisionAutoReloader.class).in(Scopes.SINGLETON);
                    binder.bind(RevisionAutoReloaderStarter.class).asEagerSingleton();
                })
                .overrideModulesWith((binder) -> {
                    // overwrite server that uses ExtractArchiveManager
                    binder.bind(WorkspaceManager.class).to(LocalWorkspaceManager.class).in(Scopes.SINGLETON);
                });
        }
    }

    private static class RevisionAutoReloaderStarter
    {
        private final String projectDirName;
        private final Config overrideParams;
        private final ProjectArchiveLoader projectLoader;
        private final RevisionAutoReloader autoReloader;

        @Inject
        public RevisionAutoReloaderStarter(Config systemConfig, ProjectArchiveLoader projectLoader, ConfigFactory cf, RevisionAutoReloader autoReloader)
        {
            this.projectDirName = systemConfig.get(SYSTEM_CONFIG_AUTO_LOAD_LOCAL_PROJECT_KEY, String.class);
            this.overrideParams = cf.fromJsonString(systemConfig.get(SYSTEM_CONFIG_LOCAL_OVERRIDE_PARAMS, String.class));
            this.projectLoader = projectLoader;
            this.autoReloader = autoReloader;
        }

        @PostConstruct
        public void start()
        {
            try {
                ProjectArchive project = loadProject(projectLoader, projectDirName, overrideParams);
                autoReloader.watch(project);
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
