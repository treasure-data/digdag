package acceptance;

import com.beust.jcommander.JCommander;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.digdag.cli.Main;
import io.digdag.cli.Server;
import io.digdag.client.DigdagClient;
import io.digdag.client.Version;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestProjectCollection;
import io.digdag.client.config.Config;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.ErrorReporter;
import io.digdag.core.agent.ExtractArchiveWorkspaceManager;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.server.ClientVersionChecker;
import io.digdag.server.JmxErrorReporter;
import io.digdag.server.ServerBootstrap;
import io.digdag.server.ServerConfig;
import io.digdag.server.ServerModule;
import io.digdag.server.ServerRuntimeInfoWriter;
import io.digdag.server.WorkflowExecutionTimeoutEnforcer;
import io.digdag.server.WorkflowExecutorLoop;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import io.digdag.server.ac.DefaultAccessController;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.spi.ac.SiteTarget;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import javax.ws.rs.NotFoundException;

import static io.digdag.client.DigdagVersion.buildVersion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.weakref.jmx.guice.ExportBinder.newExporter;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getSessionId;
import static utils.TestUtils.main;

public class AccessControllerIT
{
    public static class ACTestMain
            extends Main
    {
        public static void main(String... args)
        {
            int code = new ACTestMain(buildVersion(), System.getenv(), System.out, System.err, System.in).cli(args);
            if (code != 0) {
                System.exit(code);
            }
        }

        ACTestMain(final Version version,
                final Map<String, String> env,
                final PrintStream out,
                final PrintStream err,
                final InputStream in)
        {
            super(version, env, out, err, in);
        }

        @Override
        public void addCommands(final JCommander jc, final Injector injector)
        {
            jc.addCommand("server", injector.getInstance(ACTestServer.class));
        }
    }

    static class ACTestServer
            extends Server
    {
        @Override
        public ServerBootstrap buildServerBootstrap(final Version version, final ServerConfig serverConfig)
        {
            return new ACTestServerBootstrap(version, serverConfig);
        }
    }

    static class ACTestServerBootstrap
            extends ServerBootstrap
    {
        ACTestServerBootstrap(Version version, ServerConfig serverConfig)
        {
            super(version, serverConfig);
        }

        protected DigdagEmbed.Bootstrap digdagBootstrap()
        {
            return new DigdagEmbed.Bootstrap()
                    .setEnvironment(serverConfig.getEnvironment())
                    .setSystemConfig(serverConfig.getSystemConfig())
                    //.setSystemPlugins(loadSystemPlugins(serverConfig.getSystemConfig()))
                    .overrideModulesWith((binder) -> {
                        binder.bind(WorkspaceManager.class).to(ExtractArchiveWorkspaceManager.class).in(Scopes.SINGLETON);
                        binder.bind(Version.class).toInstance(version);
                    })
                    .addModules((binder) -> {
                        binder.bind(ServerRuntimeInfoWriter.class).asEagerSingleton();
                        binder.bind(ServerConfig.class).toInstance(serverConfig);
                        binder.bind(WorkflowExecutorLoop.class).asEagerSingleton();
                        binder.bind(WorkflowExecutionTimeoutEnforcer.class).asEagerSingleton();
                        binder.bind(ClientVersionChecker.class).toProvider(ClientVersionCheckerProvider.class);

                        binder.bind(ErrorReporter.class).to(JmxErrorReporter.class).in(Scopes.SINGLETON);
                        newExporter(binder).export(ErrorReporter.class).withGeneratedName();
                    })
                    .addModules(new ServerModule(serverConfig) {
                        @Override
                        public void bindAuthorization()
                        {
                            binder().bind(AccessController.class).to(TestAccessController.class);
                        }
                    });
        }

        private static class ClientVersionCheckerProvider
                implements Provider<ClientVersionChecker>
        {
            private final Config systemConfig;

            @Inject
            public ClientVersionCheckerProvider(Config systemConfig)
            {
                this.systemConfig = systemConfig;
            }

            @Override
            public ClientVersionChecker get()
            {
                return ClientVersionChecker.fromSystemConfig(systemConfig);
            }
        }
    }

    static class TestAccessController
            extends DefaultAccessController
    {
        @Override
        public void checkPutProject(final ProjectTarget target, final AuthenticatedUser user)
                throws AccessControlException
        {
        }

        @Override
        public void checkGetProject(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "get_project_ok":
                    return; // ok
                case "get_projects_with_name_ok":
                    return; // ok
                case "get_project_403":
                    throw new AccessControlException("not allow"); // not allow
                case "get_projects_with_name_403":
                    throw new AccessControlException("not allow"); // not allow
                default:
                    return;
            }
        }

        @Override
        public void checkListProjectsOfSite(SiteTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            throw new AccessControlException("not allow");
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .commandMainClassName(ACTestMain.class.getName())
            .build();

    private DigdagClient client;
    private OkHttpClient httpClient;

    private Path config;

    @Before
    public void setUp()
            throws Exception
    {
        // digdag client
        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        // okhttp client
        httpClient = new OkHttpClient();

        // config
         config = folder.newFile().toPath();
    }

    @Test
    public void getProject() // ProjectResource#getProject
            throws Exception
    {
        { // ok
            final String projectName = "get_project_ok";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // get the project
            //final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/projects").newBuilder();
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/project").newBuilder()
                    .addQueryParameter("name", projectName);
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            System.out.println(response.body().string());
            assertThat(response.code(), is(200));
        }

        {
            final String projectName = "get_project_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // get the project
            //final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/projects").newBuilder();
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/project").newBuilder()
                    .addQueryParameter("name", projectName);
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            System.out.println(response.body().string());
            assertThat(response.code(), is(403));
        }
    }

    @Test
    public void getProjects() // ProjectResource#getProjects
            throws Exception
    {
        { // ok with name
            final String projectName = "get_projects_with_name_ok";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // get the project
            final RestProject rp = client.getProject(projectName);
            assertThat(rp.getName(), is(projectName));
        }

        { // 404 with name
            final String projectName = "get_projects_with_name_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // get the projects
            try {
                final RestProject rp = client.getProject(projectName);
                Assert.fail();
            }
            catch (NotFoundException e) { }
        }

        { // 404 without name
            final String projectName = "get_projects_without_name_ok";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // get the projects
            assertTrue(client.getProjects().getProjects().isEmpty());
        }
    }

    /**
    @Test
    public void putProject() // ProjectResource#putProject
            throws Exception
    {
        { // ok
            final Path projectDir = folder.getRoot().toPath().resolve("put_project_ok");

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");

            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        { // 403
            final Path projectDir = folder.getRoot().toPath().resolve("put_project_403");

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(403));
        }
    }
    */
}
