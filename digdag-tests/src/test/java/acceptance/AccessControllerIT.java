package acceptance;

import com.beust.jcommander.JCommander;
import com.google.common.base.Optional;
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
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestWorkflowDefinition;
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
import java.util.Map;

import io.digdag.server.ac.DefaultAccessController;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.AttemptTarget;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;

import static io.digdag.client.DigdagVersion.buildVersion;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.weakref.jmx.guice.ExportBinder.newExporter;
import static utils.TestUtils.copyResource;
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
                            binder().bind(AccessController.class).to(ProjectBasedAccessController.class);
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

    static class ProjectBasedAccessController
            extends DefaultAccessController
    {
        @Override
        public void checkGetProject(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "get_project_403":
                case "get_projects_with_name_403":
                case "get_projects_with_id_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkListProjectsOfSite(SiteTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            throw new AccessControlException("not allow");
        }

        @Override
        public void checkGetWorkflow(WorkflowTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "get_workflow_in_project_403":
                case "get_workflows_with_name_in_project_403":
                case "get_workflow_in_workflow_403":
                case "get_workflow_with_id_in_workflow_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkRunWorkflow(WorkflowTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "run_workflow_in_attempt_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkListWorkflowsOfProject(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "get_workflows_without_name_in_project_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkGetProjectArchive(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "get_project_archive_with_rev_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkDeleteProject(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "delete_project_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkPutProject(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "put_project_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkListWorkflowsOfSite(SiteTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            throw new AccessControlException("not allow"); // 403
        }

        @Override
        public void checkGetSession(WorkflowTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "get_session_in_session_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkGetAttemptsFromSession(WorkflowTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "get_session_attempts_in_session_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkKillAttempt(AttemptTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "kill_attempt_in_session_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkGetTasksFromAttempt(WorkflowTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "get_attempts_tasks_in_attempt_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkListSessionsOfProject(ProjectTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getName()) {
                case "get_session_attempts_by_project_in_attempt_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
        }

        @Override
        public void checkListSessionsOfWorkflow(WorkflowTarget target, AuthenticatedUser user)
                throws AccessControlException
        {
            switch (target.getProjectName()) {
                case "get_session_attempts_by_workflow_in_attempt_403":
                    throw new AccessControlException("not allow"); // 403
                default:
                    return; // ok
            }
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
            assertThat(response.code(), is(200));
        }

        { // 403
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

        { // empty with name
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
                client.getProject(projectName);
                Assert.fail();
            }
            catch (NotFoundException e) {
                // GET api/projects returns empty project list but, client throws NotFound.
            }
        }

        { // empty without name
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

    @Test
    public void getProjectById() // ProjectResource#getProject
            throws Exception
    {
        { // ok with id
            final String projectName = "get_projects_with_id_ok";
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

            // get the project by id
            final RestProject rp = client.getProject(Id.of("1"));
            assertThat(rp.getName(), is(projectName));
        }

        { // 403 with id
            final String projectName = "get_projects_with_id_403";
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

            // get the project by id
            try {
                client.getProject(Id.of("2"));
                fail();
            }
            catch (ForbiddenException e) { }
        }
    }

    @Test
    public void getProjectWorkflow() // ProjectResource#getWorkflow
            throws Exception
    {
        { // ok
            final String projectName = "get_workflow_in_project_ok";
            final String wfName = projectName;
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

            // get the workflow
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/projects/1/workflow").newBuilder()
                    .addQueryParameter("name", wfName);
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            assertThat(response.code(), is(200));
        }

        { // 403
            final String projectName = "get_workflow_in_project_403";
            final String wfName = projectName;
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

            // get the workflow
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/projects/2/workflow").newBuilder()
                    .addQueryParameter("name", wfName);
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            assertThat(response.code(), is(403));
        }
    }

    @Test
    public void getProjectWorkflows() // ProjectResource#getWorkflows
            throws Exception
    {
        { // ok with name
            final String projectName = "get_workflows_with_name_in_project_ok";
            final String wfName = projectName;
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

            // get the workflow
            RestWorkflowDefinition wf = client.getWorkflowDefinition(Id.of("1"), wfName);
            assertThat(wf.getName(), is(wfName));
        }

        { // 403 with name
            final String projectName = "get_workflows_with_name_in_project_403";
            final String wfName = projectName;
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

            // get the workflow
            try {
                client.getWorkflowDefinition(Id.of("2"), wfName);
            }
            catch (NotFoundException e) { }
        }

        { // 403 without name
            final String projectName = "get_workflows_without_name_in_project_403";
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

            // get the workflows
            assertTrue(client.getWorkflowDefinitions(Id.of("3")).getWorkflows().isEmpty());
        }
    }

    @Test
    public void getArchive() // ProjectResource#getArchive()
            throws Exception
    {
        { // ok with rev
            final String projectName = "get_project_archive_with_rev_ok";
            final String wfName = projectName;
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

            // Get the project archive
            CommandStatus downloadStatus = main("download",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(downloadStatus.errUtf8(), pushStatus.code(), is(0));
        }

        { // 403 with rev
            final String projectName = "get_project_archive_with_rev_403";
            final String wfName = projectName;
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

            // Get the project archive
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/projects/2/archive").newBuilder()
                    .addQueryParameter("revision", "4711");
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            assertThat(response.code(), is(403));
        }
    }

    @Test
    public void deleteProject() // ProjectResource#deleteProject()
            throws Exception
    {
        { // ok
            final String projectName = "delete_project_ok";
            final String wfName = projectName;
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

            // Delete the project
            CommandStatus downloadStatus = main("delete",
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(downloadStatus.errUtf8(), pushStatus.code(), is(0));
        }

        { // 403
            final String projectName = "delete_project_403";
            final String wfName = projectName;
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

            // Delete the project
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/projects/2").newBuilder();
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .delete()
                    .build()
            ).execute();
            assertThat(response.code(), is(403));
        }
    }

    @Test
    public void putProject() // ProjectResource#putProject()
            throws Exception
    {
        { // ok
            final String projectName = "put_project_ok";
            final String wfName = projectName;
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
        }

        { // 403
            final String projectName = "put_project_ok";
            final String wfName = projectName;
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
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(1));
        }
    }

    @Test
    public void getWorkflowDefinition() // WorkflowResource#getWorkflowDefinition
            throws Exception
    {
        { // ok
            final String projectName = "get_workflow_in_workflow_ok";
            final String wfName = projectName;
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

            // get the workflow
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/workflow").newBuilder()
                    .addQueryParameter("project", projectName)
                    .addQueryParameter("name", wfName);
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            assertThat(response.code(), is(200));
        }

        { // 403
            final String projectName = "get_workflow_in_workflow_403";
            final String wfName = projectName;
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

            // get the workflow
            final HttpUrl.Builder httpBuider = HttpUrl.parse(server.endpoint() + "/api/workflow").newBuilder()
                    .addQueryParameter("project", projectName)
                    .addQueryParameter("name", wfName);
            final Response response = httpClient.newCall(new Request.Builder()
                    .url(httpBuider.build())
                    .get()
                    .build()
            ).execute();
            assertThat(response.code(), is(403));
        }
    }

    @Test
    public void getWorkflowDefinitions() // WorkflowResource#getWorkflowDefinitions
            throws Exception
    {
        { // 403
            final String projectName = "get_workflows_in_workflow_403";
            final String wfName = projectName;
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

            // get workflows
            try {
                client.getWorkflowDefinitions(); // call /api/workflows
                fail();
            }
            catch (ForbiddenException e) { }
        }
    }

    @Test
    public void getWorkflowDefinitionById() // WorkflowResource#getWorkflowDefinition
            throws Exception
    {
        { // ok
            final String projectName = "get_workflow_with_id_in_workflow_ok";
            final String wfName = projectName;
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

            // get workflow
            RestWorkflowDefinition wf = client.getWorkflowDefinition(Id.of("1"));
            assertThat(wf.getName(), is(wfName));
        }

        { // 403
            final String projectName = "get_workflow_with_id_in_workflow_403";
            final String wfName = projectName;
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

            // get workflow
            try {
                client.getWorkflowDefinition(Id.of("2"));
                fail();
            }
            catch (ForbiddenException e) { }
        }
    }


    @Test
    public void getSession() // SessionResource#getSession
            throws Exception
    {
        { // ok
            final String projectName = "get_session_in_session_ok";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            // Get the session
            RestSession s = client.getSession(Id.of("1")); // /api/sessions/{id}
            assertThat(s.getWorkflow().getName(), is("hourly_sleep"));
        }

        { // 403
            final String projectName = "get_session_in_session_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            // Get the session
            try {
                client.getSession(Id.of("2")); // /api/sessions/{id}
                fail();
            }
            catch (ForbiddenException e) { }
        }
    }

    @Test
    public void getSessionAttempts() // SessionResource#getSessionAttempts
            throws Exception
    {
        { // 403
            final String projectName = "get_session_attempts_in_session_ok";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            // Get the attempt sessions
            try {
                client.getSessionAttempts(Id.of("1"), Optional.absent(), Optional.absent());
            }
            catch (ForbiddenException e) { }
        }
    }

    @Test
    public void getAttemptAttempts() // AttemptResource#getAttempts
            throws Exception
    {
        { // 403
            final String projectName = "get_session_attempts_by_workflow_in_attempt_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            // Get the attempts
            try {
                client.getSessionAttempts(projectName, "hourly_sleep", Optional.absent()); // /api/attempts
            }
            catch (ForbiddenException e) { }
        }

        { // 403
            final String projectName = "get_session_attempts_by_project_in_attempt_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            // Get the attempts
            try {
                client.getSessionAttempts(projectName, Optional.absent()); // /api/attempts
            }
            catch (ForbiddenException e) { }
        }
    }

    @Test
    public void getAttemptsTasks()
            throws Exception
    {
        { // 403
            final String projectName = "get_attempts_tasks_in_attempt_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            // Get the attempts tasks
            try {
                client.getTasks(Id.of("1")); // /api/attempts/{id}/tasks
            }
            catch (ForbiddenException e) { }
        }
    }

    @Test
    public void startAttempt()
            throws Exception
    {
        { // 403
            final String projectName = "run_workflow_in_attempt_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(1));
        }
    }

    @Test
    public void killAttempt()
            throws Exception
    {
        { // 403
            final String projectName = "kill_attempt_in_session_403";
            final Path projectDir = folder.getRoot().toPath().resolve(projectName);

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

            // Push the project
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    projectName,
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            // Start a session
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    projectName, "hourly_sleep",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

            try {
                client.killSessionAttempt(Id.of("1"));
            }
            catch (ForbiddenException e) { }
        }
    }
}
