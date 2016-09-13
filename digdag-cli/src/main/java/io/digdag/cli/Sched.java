package io.digdag.cli;

import com.beust.jcommander.Parameter;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.Version;
import io.digdag.core.agent.LocalWorkspaceManager;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.server.ServerBootstrap;
import io.digdag.server.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import java.util.Properties;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.Arguments.loadProject;
import static io.digdag.cli.SystemExitException.systemExit;

public class Sched
    extends Server
{
    private static final Logger logger = LoggerFactory.getLogger(Sched.class);

    private static final String SYSTEM_CONFIG_AUTO_LOAD_LOCAL_PROJECT_KEY = "scheduler.autoLoadLocalProject";
    private static final String SYSTEM_CONFIG_LOCAL_OVERWRITE_PARAMS = "scheduler.localOverwriteParams";

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

        sched();
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
        err.println("        --max-task-threads N         limit maxium number of task execution threads");
        err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        err.println("    -c, --config PATH.properties     server configuration property path");
        Main.showCommonOptions(env, err);
        return systemExit(error);
    }

    private void sched()
            throws ServletException, Exception
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

            Config overwriteParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

            props = buildServerProperties();

            // use memory database by default
            if (!props.containsKey("database.type")) {
                props.setProperty("database.type", "memory");
            }

            props.setProperty(SYSTEM_CONFIG_AUTO_LOAD_LOCAL_PROJECT_KEY, projectDirName != null ? projectDirName : "");  // Properties can't store null
            props.setProperty(SYSTEM_CONFIG_LOCAL_OVERWRITE_PARAMS, overwriteParams.toString());
        }

        ServerBootstrap.startServer(version, props, SchedulerServerBootStrap.class);
    }

    public static class SchedulerServerBootStrap
            extends ServerBootstrap
    {
        @Inject
        public SchedulerServerBootStrap(GuiceRsServerControl control)
        {
            super(control);
        }

        @Override
        public Injector initialize(ServletContext context)
        {
            Injector injector = super.initialize(context);

            ConfigFactory cf = injector.getInstance(ConfigFactory.class);
            RevisionAutoReloader autoReloader = injector.getInstance(RevisionAutoReloader.class);
            ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
            ProjectArchiveLoader projectLoader = injector.getInstance(ProjectArchiveLoader.class);

            Config systemConfig = injector.getInstance(Config.class);

            String projectDirName = systemConfig.get(SYSTEM_CONFIG_AUTO_LOAD_LOCAL_PROJECT_KEY, String.class);
            Config overwriteParams = cf.fromJsonString(systemConfig.get(SYSTEM_CONFIG_LOCAL_OVERWRITE_PARAMS, String.class));

            try {
                ProjectArchive project = loadProject(projectLoader, projectDirName, overwriteParams);
                autoReloader.watch(project);
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            return injector;
        }

        @Override
        protected DigdagEmbed.Bootstrap bootstrap(DigdagEmbed.Bootstrap bootstrap, ServerConfig serverConfig, Version version)
        {
            return super.bootstrap(bootstrap, serverConfig, version)
                .addModules((binder) -> {
                    binder.bind(RevisionAutoReloader.class).in(Scopes.SINGLETON);
                })
                .overrideModulesWith((binder) -> {
                    // overwrite server that uses ExtractArchiveManager
                    binder.bind(WorkspaceManager.class).to(LocalWorkspaceManager.class).in(Scopes.SINGLETON);
                });
        }
    }
}
