package io.digdag.client;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.api.Id;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestLogFileHandleCollection;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestProjectCollection;
import io.digdag.client.api.RestRevisionCollection;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleCollection;
import io.digdag.client.api.RestScheduleBackfillRequest;
import io.digdag.client.api.RestScheduleSkipRequest;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSecret;
import io.digdag.client.api.RestSecretList;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestSetSecretRequest;
import io.digdag.client.api.RestTaskCollection;
import io.digdag.client.api.RestVersionCheckResult;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowDefinitionCollection;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.SecretValidation;
import io.digdag.client.api.SessionTimeTruncate;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.plugins.interceptors.encoding.AcceptEncodingGZIPFilter;
import org.jboss.resteasy.plugins.interceptors.encoding.GZIPDecodingInterceptor;
import org.jboss.resteasy.plugins.interceptors.encoding.GZIPEncodingInterceptor;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static io.digdag.client.DigdagVersion.buildVersion;
import static java.util.Locale.ENGLISH;
import static javax.ws.rs.core.HttpHeaders.USER_AGENT;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static org.jboss.resteasy.client.jaxrs.internal.ClientInvocation.handleErrorStatus;

public class DigdagClient implements AutoCloseable
{
    private static final int TOO_MANY_REQUESTS_429 = 429;
    private static final int REQUEST_TIMEOUT_408 = 408;
    private static final int MAX_REDIRECT = 10;

    public static class Builder
    {
        private String host = null;
        private int port = -1;
        private boolean ssl = false;
        private String proxyHost = null;
        private Integer proxyPort = null;
        private String proxyScheme = null;
        private final Map<String, String> baseHeaders = new HashMap<>();
        private Function<Map<String, String>, Map<String, String>> headerBuilder = null;
        private boolean disableCertValidation;
        private boolean disableDirectDownload = false;

        public Builder host(String host)
        {
            this.host = host;
            return this;
        }

        public Builder port(int port)
        {
            this.port = port;
            return this;
        }

        public Builder ssl(boolean ssl)
        {
            this.ssl = ssl;
            return this;
        }

        public Builder proxyHost(String proxyHost)
        {
            this.proxyHost = proxyHost;
            return this;
        }

        public Builder proxyPort(int proxyPort)
        {
            this.proxyPort = proxyPort;
            return this;
        }

        public Builder proxyScheme(String proxyScheme)
        {
            this.proxyScheme = proxyScheme;
            return this;
        }

        public Builder header(String key, String value)
        {
            this.baseHeaders.put(key, value);
            return this;
        }

        public Builder headers(Map<String, String> map)
        {
            this.baseHeaders.putAll(map);
            return this;
        }

        public Builder headerBuilder(Function<Map<String, String>, Map<String, String>> headerBuilder)
        {
            this.headerBuilder = headerBuilder;
            return this;
        }

        public Builder disableCertValidation(boolean value)
        {
            this.disableCertValidation = value;
            return this;
        }

        public Builder disableDirectDownload(boolean value)
        {
            this.disableDirectDownload = value;
            return this;
        }

        public DigdagClient build()
        {
            return new DigdagClient(this);
        }
    }

    public static ObjectMapper objectMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JacksonTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // InjectableValues makes @JacksonInject work which is used at io.digdag.client.config.Config.<init>
        InjectableValues.Std injects = new InjectableValues.Std();
        injects.addValue(ObjectMapper.class, mapper);
        mapper.setInjectableValues(injects);

        return mapper;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final String endpoint;

    private final Supplier<MultivaluedMap<String, Object>> headers;

    private final Client client;
    private final ConfigFactory cf;
    private final boolean disableDirectDownload;

    private DigdagClient(Builder builder)
    {
        if (builder.host == null) {
            throw new IllegalArgumentException("host is not set");
        }

        if (builder.ssl) {
            if (builder.port < 0) {
                this.endpoint = "https://" + builder.host;
            }
            else {
                this.endpoint = "https://" + builder.host + ":" + builder.port;
            }
        }
        else {
            if (builder.port < 0) {
                this.endpoint = "http://" + builder.host;
            }
            else {
                this.endpoint = "http://" + builder.host + ":" + builder.port;
            }
        }

        final Map<String, String> baseHeaders = builder.baseHeaders;
        final Function<Map<String, String>, Map<String, String>> headerBuilder = builder.headerBuilder;

        if (headerBuilder != null) {
            this.headers = () -> new MultivaluedHashMap<>(headerBuilder.apply(new HashMap<>(baseHeaders)));
        }
        else {
            final MultivaluedMap<String, Object> staticHeaders = new MultivaluedHashMap<>(baseHeaders);
            this.headers = () -> staticHeaders;
        }

        ObjectMapper mapper = objectMapper();

        ResteasyClientBuilder clientBuilder = new ResteasyClientBuilder()
                .register(AcceptEncodingGZIPFilter.class)
                .register(GZIPDecodingInterceptor.class)
                .register(GZIPEncodingInterceptor.class)
                .register(new UserAgentFilter("DigdagClient/" + buildVersion()))
                .register(new JacksonJsonProvider(mapper));

        // TODO: support proxy user/pass
        if (builder.proxyHost != null) {
            if (builder.proxyPort == null) {
                clientBuilder.defaultProxy(builder.proxyHost);
            } else {
                if (builder.proxyScheme == null) {
                    clientBuilder.defaultProxy(builder.proxyHost, builder.proxyPort);
                } else {
                    clientBuilder.defaultProxy(builder.proxyHost, builder.proxyPort, builder.proxyScheme);
                }
            }
        }

        if (builder.disableCertValidation) {
            clientBuilder.disableTrustManager();
        }

        this.client = clientBuilder.build();

        this.cf = new ConfigFactory(mapper);

        this.disableDirectDownload = builder.disableDirectDownload;

    }

    @Override
    public void close()
    {
        client.close();
    }

    public Config newConfig()
    {
        return cf.create();
    }

    public RestProject getProject(String name)
    {
        List<RestProject> projs = doGet(RestProjectCollection.class,
                target("/api/projects")
                .queryParam("name", name))
            .getProjects();
        if (projs.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "project not found: %s",
                        name));
        }
        else {
            return projs.get(0);
        }
    }

    public RestProjectCollection getProjects()
    {
        return doGet(RestProjectCollection.class,
                target("/api/projects"));
    }

    public RestProject getProject(Id projId)
    {
        return doGet(RestProject.class,
                target("/api/projects/{id}")
                .resolveTemplate("id", projId));
    }

    public void deleteProject(Id projId)
    {
        doDelete(target("/api/projects/{id}")
                .resolveTemplate("id", projId));
    }

    public RestRevisionCollection getRevisions(Id projId, Optional<Id> lastId)
    {
        return doGet(RestRevisionCollection.class,
                target("/api/projects/{id}/revisions")
                .resolveTemplate("id", projId)
                .queryParam("last_id", lastId.orNull()));
    }

    public RestWorkflowDefinitionCollection getWorkflowDefinitions()
    {
        return doGet(RestWorkflowDefinitionCollection.class,
                target("/api/workflows"));
    }

    public RestWorkflowDefinitionCollection getWorkflowDefinitions(Optional<Id> lastId)
    {
        return doGet(RestWorkflowDefinitionCollection.class,
                target("/api/workflows")
                .queryParam("last_id", lastId.orNull()));
    }

    public RestWorkflowDefinitionCollection getWorkflowDefinitions(Id projId)
    {
        return doGet(RestWorkflowDefinitionCollection.class,
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId));
    }

    public RestWorkflowDefinitionCollection getWorkflowDefinitions(Id projId, String revision)
    {
        return doGet(RestWorkflowDefinitionCollection.class,
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId)
                .queryParam("revision", revision));
    }

    public RestWorkflowDefinition getWorkflowDefinition(Id projId, String name)
    {
        List<RestWorkflowDefinition> defs = doGet(RestWorkflowDefinitionCollection.class,
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId)
                .queryParam("name", name))
            .getWorkflows();
        if (defs.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "workflow not found in the latest revision of project id = %s: %s",
                        projId, name));
        }
        else {
            return defs.get(0);
        }
    }

    public RestWorkflowDefinition getWorkflowDefinition(Id projId, String name, String revision)
    {
        List<RestWorkflowDefinition> defs = doGet(RestWorkflowDefinitionCollection.class,
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId)
                .queryParam("name", name)
                .queryParam("revision", revision))
            .getWorkflows();
        if (defs.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "workflow not found in revision = %s of project id = %s: %s",
                        revision, projId, name));
        }
        else {
            return defs.get(0);
        }
    }

    public RestWorkflowDefinition getWorkflowDefinition(Id workflowId)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/workflows/{id}")
                .resolveTemplate("id", workflowId));
    }

    public RestWorkflowSessionTime getWorkflowTruncatedSessionTime(Id workflowId, LocalTimeOrInstant time)
    {
        return doGet(RestWorkflowSessionTime.class,
                target("/api/workflows/{id}/truncated_session_time")
                .resolveTemplate("id", workflowId)
                .queryParam("session_time", time.toString()));
    }

    public RestWorkflowSessionTime getWorkflowTruncatedSessionTime(Id workflowId, LocalTimeOrInstant time, SessionTimeTruncate mode)
    {
        return doGet(RestWorkflowSessionTime.class,
                target("/api/workflows/{id}/truncated_session_time")
                .resolveTemplate("id", workflowId)
                .queryParam("session_time", time.toString())
                .queryParam("mode", mode == null ? null : mode.toString()));
    }

    public RestProject putProjectRevision(String projName, String revision, File body)
        throws IOException
    {
        return putProjectRevision(projName, revision, body, Optional.absent());
    }

    public RestProject putProjectRevision(String projName, String revision, File body, Optional<Instant> scheduleFrom)
        throws IOException
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(projName), "projName");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(revision), "revision");
        if (scheduleFrom.isPresent()) {
            return doPut(RestProject.class,
                    "application/gzip",
                    body,
                    target("/api/projects")
                    .queryParam("project", projName)
                    .queryParam("revision", revision)
                    .queryParam("schedule_from", scheduleFrom.get().toString()));
        }
        else {
            return doPut(RestProject.class,
                    "application/gzip",
                    body,
                    target("/api/projects")
                    .queryParam("project", projName)
                    .queryParam("revision", revision));
        }
    }

    @FunctionalInterface
    interface RequestWithFollowingRedirect<T>
    {
        T invoke(WebTarget webTarget, Optional<Response> lastResponse);
    }

    private <T> T withFollowingRedirect(WebTarget initialWebTarget, RequestWithFollowingRedirect<T> request)
    {
        WebApplicationException firstRedirectException = null;
        WebTarget webTarget = initialWebTarget;
        Optional<Response> lastResponse = Optional.absent();

        for (int i = 0; i < MAX_REDIRECT; i++) {
            try {
                return request.invoke(webTarget, lastResponse);
            }
            catch (WebApplicationException e) {
                if (firstRedirectException == null) {
                    firstRedirectException = e;
                }
                Response response = checkNotNull(e.getResponse());
                int status = response.getStatus();
                if (status % 100 == 3 && response.getLocation() != null) {
                    lastResponse = Optional.of(response);
                    webTarget = client.target(UriBuilder.fromUri(response.getLocation()));
                    continue;
                }
                throw e;
            }
        }
        throw firstRedirectException;
    }

    // TODO getArchive with streaming
    public InputStream getProjectArchive(Id projId, String revision)
    {
        WebTarget webTarget = target("/api/projects/{id}/archive")
                .resolveTemplate("id", projId)
                .queryParam("revision", revision);
        webTarget = addDisableDirectDownloadParam(webTarget);
        return withFollowingRedirect(webTarget,
                (wt, lastResponse) -> {
                    Invocation.Builder request = wt.request();
                    if (!lastResponse.isPresent()) {
                        // Headers shouldn't be appended when redirecting.
                        // With headers S3 can return "Bad Request"
                        request.headers(headers.get());
                    }
                    return invokeWithRetry(request.buildGet())
                            .readEntity(InputStream.class);
                }
        );
    }

    public RestScheduleCollection getSchedules()
    {
        return doGet(RestScheduleCollection.class,
                target("/api/schedules"));
    }

    public RestScheduleCollection getSchedules(Optional<Id> lastId)
    {
        return doGet(RestScheduleCollection.class,
                target("/api/schedules")
                .queryParam("last_id", lastId.orNull()));
    }

    public RestScheduleCollection getSchedules(Id projectId, Optional<Id> lastId)
    {
        return doGet(RestScheduleCollection.class,
                target("/api/projects/{id}/schedules")
                .resolveTemplate("id", projectId)
                .queryParam("last_id", lastId.orNull()));
    }

    public RestSchedule getSchedule(Id projectId, String workflowName)
    {
        List<RestSchedule> scheds = doGet(RestScheduleCollection.class,
                target("/api/projects/{id}/schedules")
                .resolveTemplate("id", projectId)
                .queryParam("workflow", workflowName))
            .getSchedules();
        if (scheds.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "schedule not found in the latest revision of project id = %s: %s",
                        projectId, workflowName));
        }
        else {
            return scheds.get(0);
        }
    }

    public RestSchedule getSchedule(Id id)
    {
        return doGet(RestSchedule.class,
                target("/api/schedules/{id}")
                .resolveTemplate("id", id));
    }

    public RestSessionCollection getSessions() {
        return getSessions(Optional.absent());
    }

    public RestSessionCollection getSessions(Optional<Id> lastId)
    {
        return doGet(RestSessionCollection.class,
                target("/api/sessions")
                        .queryParam("last_id", lastId.orNull()));
    }

    public RestSessionCollection getSessions(Id projectId) {
        return getSessions(projectId, Optional.absent(), Optional.absent());
    }

    public RestSessionCollection getSessions(Id projectId, Optional<Id> lastId, Optional<Integer> pageSize)
    {
        return doGet(RestSessionCollection.class,
                target("/api/projects/{projectId}/sessions")
                        .resolveTemplate("projectId", projectId)
                        .queryParam("last_id", lastId.orNull())
                        .queryParam("page_size", pageSize.orNull()));
    }

    public RestSessionCollection getSessions(Id projectId, String workflowName) {
        return getSessions(projectId, workflowName, Optional.absent(), Optional.absent());
    }

    public RestSessionCollection getSessions(Id projectId, String workflowName, Optional<Id> lastId, Optional<Integer> pageSize)
    {
        return doGet(RestSessionCollection.class,
                target("/api/projects/{projectId}/sessions")
                        .resolveTemplate("projectId", projectId)
                        .queryParam("workflow", workflowName)
                        .queryParam("last_id", lastId.orNull())
                        .queryParam("page_size", pageSize.orNull()));
    }

    public RestSession getSession(Id sessionId)
    {
        return doGet(RestSession.class,
                target("/api/sessions/{id}")
                        .resolveTemplate("id", sessionId));
    }

    public RestSessionAttemptCollection getSessionAttempts(Id sessionId, Optional<Id> lastId, Optional<Integer> pageSize)
    {
        return doGet(RestSessionAttemptCollection.class,
                target("/api/sessions/{sessionId}/attempts")
                        .resolveTemplate("sessionId", sessionId)
                        .queryParam("last_id", lastId.orNull())
                        .queryParam("page_size", pageSize.orNull()));
    }

    public RestSessionAttemptCollection getSessionAttempts(Optional<Id> lastId, Optional<Integer> pageSize)
    {
        return doGet(RestSessionAttemptCollection.class,
                target("/api/attempts")
                .queryParam("last_id", lastId.orNull())
                .queryParam("page_size", pageSize.orNull()));
    }

    public RestSessionAttemptCollection getSessionAttempts(String projName, Optional<Id> lastId)
    {
        return doGet(RestSessionAttemptCollection.class,
                target("/api/attempts")
                .queryParam("project", projName)
                .queryParam("last_id", lastId.orNull()));
    }

    public RestSessionAttemptCollection getSessionAttempts(String projName, String workflowName, Optional<Id> lastId)
    {
        return doGet(RestSessionAttemptCollection.class,
                target("/api/attempts")
                .queryParam("project", projName)
                .queryParam("workflow", workflowName)
                .queryParam("last_id", lastId.orNull()));
    }

    public RestSessionAttempt getSessionAttempt(Id attemptId)
    {
        return doGet(RestSessionAttempt.class,
                target("/api/attempts/{id}")
                .resolveTemplate("id", attemptId));
    }

    public RestTaskCollection getTasks(Id attemptId)
    {
        return doGet(RestTaskCollection.class,
                target("/api/attempts/{id}/tasks")
                .resolveTemplate("id", attemptId));
    }

    public RestLogFileHandleCollection getLogFileHandlesOfAttempt(Id attemptId)
    {
        WebTarget webTarget = target("/api/logs/{id}/files")
                .resolveTemplate("id", attemptId);
        webTarget = addDisableDirectDownloadParam(webTarget);

        return doGet(RestLogFileHandleCollection.class, webTarget);
    }

    public RestLogFileHandleCollection getLogFileHandlesOfTask(Id attemptId, String taskName)
    {
        try {
            WebTarget webTarget = target("/api/logs/{id}/files")
                    .resolveTemplate("id", attemptId)
                    .queryParam("task", URLEncoder.encode(taskName, "UTF-8"));
            webTarget = addDisableDirectDownloadParam(webTarget);

            return doGet(RestLogFileHandleCollection.class, webTarget);
        } catch (UnsupportedEncodingException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public InputStream getLogFile(Id attemptId, RestLogFileHandle handle)
    {
        if (handle.getDirect().isPresent()) {
            Invocation request = client.target(UriBuilder.fromUri(handle.getDirect().get().getUrl()))
                    .request()
                    .buildGet();
            return invokeWithRetry(request).readEntity(InputStream.class);
        }
        else {
            return getLogFile(attemptId, handle.getFileName());
        }
    }

    public InputStream getLogFile(Id attemptId, String fileName)
    {
        Invocation request = target("/api/logs/{id}/files/{fileName}")
                .resolveTemplate("id", attemptId)
                .resolveTemplate("fileName", fileName)
                .request()
                .headers(this.headers.get())
                .buildGet();

        return invokeWithRetry(request)
                .readEntity(InputStream.class);
    }

    private WebTarget addDisableDirectDownloadParam(WebTarget target)
    {
        // direct_download is default true
        // So only set direct_download=false when disableDirectDownload == true
        if (disableDirectDownload) {
            target = target.queryParam("direct_download", false);
        }
        return target;
    }

    private Response invokeWithRetry(Invocation request)
    {
        Retryer<Response> retryer = RetryerBuilder.<Response>newBuilder()
                .retryIfException(not(DigdagClient::isDeterministicError))
                .withWaitStrategy(exponentialWait())
                .withStopStrategy(stopAfterAttempt(10))
                .build();

        try {
            return retryer.call(() -> {
                Response res = request.invoke();
                if (res.getStatusInfo().getFamily() != SUCCESSFUL) {
                    res.close();
                    return handleErrorStatus(res);
                }
                return res;
            });
        }
        catch (ExecutionException | RetryException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw Throwables.propagate(cause);
        }
    }

    private static boolean isDeterministicError(Throwable ex)
    {
        return ex instanceof WebApplicationException &&
                isDeterministicError(((WebApplicationException) ex).getResponse());
    }

    private static boolean isDeterministicError(Response res)
    {
        return isDeterministicError(res.getStatus());
    }

    private static boolean isDeterministicError(int status)
    {
        // Retrying against 3xx doesn't make sense
        if (status >= 300 && status < 500) {
            switch (status) {
                case TOO_MANY_REQUESTS_429:
                case REQUEST_TIMEOUT_408:
                    return false;
                default:
                    return true;
            }
        }
        return false;
    }

    public RestSessionAttempt startSessionAttempt(RestSessionAttemptRequest request)
    {
        return doPut(RestSessionAttempt.class,
                "application/json",
                request,
                target("/api/attempts"));
    }

    public void killSessionAttempt(Id attemptId)
    {
        doPost(RestSessionAttempt.class,
                new HashMap<String, String>(),
                target("/api/attempts/{id}/kill")
                .resolveTemplate("id", attemptId));
    }

    public RestScheduleSummary skipSchedulesToTime(Id scheduleId, Instant untilTime, Optional<Instant> runTime, boolean dryRun)
    {
        return doPost(RestScheduleSummary.class,
                RestScheduleSkipRequest.builder()
                    .nextTime(LocalTimeOrInstant.of(untilTime))
                    .nextRunTime(runTime)
                    .dryRun(dryRun)
                    .build(),
                target("/api/schedules/{id}/skip")
                .resolveTemplate("id", scheduleId));
    }

    public RestScheduleSummary skipSchedulesToTime(Id scheduleId, LocalDateTime untilTime, Optional<Instant> runTime, boolean dryRun)
    {
        return doPost(RestScheduleSummary.class,
                RestScheduleSkipRequest.builder()
                    .nextTime(LocalTimeOrInstant.of(untilTime))
                    .nextRunTime(runTime)
                    .dryRun(dryRun)
                    .build(),
                target("/api/schedules/{id}/skip")
                .resolveTemplate("id", scheduleId));
    }

    public RestScheduleSummary skipSchedulesByCount(Id scheduleId, Instant fromTime, int count, Optional<Instant> runTime, boolean dryRun)
    {
        return doPost(RestScheduleSummary.class,
                RestScheduleSkipRequest.builder()
                    .fromTime(fromTime)
                    .count(count)
                    .nextRunTime(runTime)
                    .dryRun(dryRun)
                    .build(),
                target("/api/schedules/{id}/skip")
                .resolveTemplate("id", scheduleId));
    }

    public RestSessionAttemptCollection backfillSchedule(Id scheduleId, Instant fromTime, String attemptName, Optional<Integer> count, boolean dryRun)
    {
        return doPost(RestSessionAttemptCollection.class,
                RestScheduleBackfillRequest.builder()
                    .fromTime(fromTime)
                    .dryRun(dryRun)
                    .attemptName(attemptName)
                    .count(count)
                    .build(),
                target("/api/schedules/{id}/backfill")
                .resolveTemplate("id", scheduleId));
    }

    public RestScheduleSummary disableSchedule(Id scheduleId)
    {
        return doPost(RestScheduleSummary.class,
                ImmutableMap.of(),
                target("/api/schedules/{id}/disable")
                        .resolveTemplate("id", scheduleId));
    }

    public RestScheduleSummary enableSchedule(Id scheduleId)
    {
        return doPost(RestScheduleSummary.class,
                ImmutableMap.of(),
                target("/api/schedules/{id}/enable")
                        .resolveTemplate("id", scheduleId));
    }

    static class Version
    {
        private final Map<String, Object> map;

        @JsonCreator
        Version(Map<String, Object> map)
        {
            this.map = map;
        }

        @JsonValue
        public Map<String, Object> get()
        {
            return map;
        }
    }

    public Map<String, Object> getVersion()
    {
        return doGet(Version.class, target("/api/version")).get();
    }

    public RestVersionCheckResult checkClientVersion(String clientVersion)
    {
        return doGet(RestVersionCheckResult.class,
                target("/api/version/check")
                .queryParam("client", clientVersion));
    }

    public void setProjectSecret(Id projectId, String key, String value)
    {
        if (!SecretValidation.isValidSecret(key, value)) {
            throw new IllegalArgumentException();
        }

        Response response = target("/api/projects/{id}/secrets/{key}")
                .resolveTemplate("id", projectId)
                .resolveTemplate("key", key)
                .request()
                .headers(headers.get())
                .put(Entity.entity(RestSetSecretRequest.of(value), "application/json"));
        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            throw new WebApplicationException("Failed to set project secret: " + response.getStatusInfo());
        }

        response.readEntity(RestSecret.class);
    }

    public void deleteProjectSecret(Id projectId, String key)
    {
        if (!SecretValidation.isValidSecretKey(key)) {
            throw new IllegalArgumentException();
        }

        Response response = target("/api/projects/{id}/secrets/{key}")
                .resolveTemplate("id", projectId)
                .resolveTemplate("key", key)
                .request()
                .headers(headers.get())
                .delete();

        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            throw new WebApplicationException("Failed to delete project secret: " + response.getStatusInfo());
        }

        response.readEntity(RestSecret.class);
    }

    public RestSecretList listProjectSecrets(Id projectId)
    {
        return target("/api/projects/{id}/secrets")
                .resolveTemplate("id", projectId)
                .request("application/json")
                .headers(headers.get())
                .get(RestSecretList.class);
    }

    public Config adminGetAttemptUserInfo(Id attemptId) {
        return doGet(Config.class,
                target("/api/admin/attempts/{id}/userinfo")
                        .resolveTemplate("id", attemptId));
    }

    private WebTarget target(String path)
    {
        return client.target(UriBuilder.fromUri(endpoint + path));
    }

    private <T> T doGet(GenericType<T> type, WebTarget target)
    {
        return invokeWithRetry(target.request("application/json")
            .headers(headers.get())
            .buildGet()).readEntity(type);
    }

    private <T> T doGet(Class<T> type, WebTarget target)
    {
        return invokeWithRetry(target.request("application/json")
            .headers(headers.get())
            .buildGet()).readEntity(type);
    }

    private <T> T doPut(Class<T> type, String contentType, Object body, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .put(Entity.entity(body, contentType), type);
    }

    private <T> T doPost(GenericType<T> type, Object body, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .post(Entity.entity(body, "application/json"), type);
    }

    private <T> T doPost(Class<T> type, Object body, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .post(Entity.entity(body, "application/json"), type);
    }

    private void doDelete(WebTarget target)
    {
        // must consume body to avoid this error from httpclient:
        // "Make sure to release the connection before allocating another one."
        doDelete(Object.class, target);
    }

    private <T> T doDelete(Class<T> type, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .delete(type);
    }

    private static class UserAgentFilter
            implements ClientRequestFilter
    {
        private final String userAgent;

        UserAgentFilter(String userAgent)
        {
            this.userAgent = userAgent;
        }

        @Override
        public void filter(ClientRequestContext requestContext)
                throws IOException
        {
            requestContext.getHeaders().putSingle(USER_AGENT, userAgent);
        }
    }
}
