package io.digdag.client;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.base.Optional;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.LocalTimeOrInstant;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestRevision;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleBackfillRequest;
import io.digdag.client.api.RestScheduleSkipRequest;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.SessionTimeTruncate;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Locale.ENGLISH;

public class DigdagClient
{
    public static class Builder
    {
        private String host = null;
        private int port = -1;
        private boolean ssl = false;
        private final Map<String, String> baseHeaders = new HashMap<>();
        private Function<Map<String, String>, Map<String, String>> headerBuilder = null;

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

        // InjectableValues makes @JacksonInject work which is used at io.digdag.client.config.Config.<init>
        InjectableValues.Std injects = new InjectableValues.Std();
        injects.addValue(ObjectMapper.class, mapper);
        mapper.setInjectableValues(injects);

        this.client = new ResteasyClientBuilder()
            .register(new JacksonJsonProvider(mapper))
            .build();
        this.cf = new ConfigFactory(mapper);
    }

    public Config newConfig()
    {
        return cf.create();
    }

    public RestProject getProject(String name)
    {
        List<RestProject> projs = doGet(new GenericType<List<RestProject>>() { },
                target("/api/projects")
                .queryParam("name", name));
        if (projs.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "project not found: %s",
                        name));
        }
        else {
            return projs.get(0);
        }
    }

    public List<RestProject> getProjects()
    {
        return doGet(new GenericType<List<RestProject>>() { },
                target("/api/projects"));
    }

    public RestProject getProject(int projId)
    {
        return doGet(RestProject.class,
                target("/api/projects/{id}")
                .resolveTemplate("id", projId));
    }

    public void deleteProject(int projId)
    {
        doDelete(target("/api/projects/{id}")
                .resolveTemplate("id", projId));
    }

    public List<RestRevision> getRevisions(int projId, Optional<Integer> lastId)
    {
        return doGet(new GenericType<List<RestRevision>>() { },
                target("/api/projects/{id}/revisions")
                .resolveTemplate("id", projId)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestWorkflowDefinition> getWorkflowDefinitions(int projId)
    {
        return doGet(new GenericType<List<RestWorkflowDefinition>>() { },
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId));
    }

    public List<RestWorkflowDefinition> getWorkflowDefinitions(int projId, String revision)
    {
        return doGet(new GenericType<List<RestWorkflowDefinition>>() { },
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId)
                .queryParam("revision", revision));
    }

    public RestWorkflowDefinition getWorkflowDefinition(int projId, String name)
    {
        List<RestWorkflowDefinition> defs = doGet(new GenericType<List<RestWorkflowDefinition>>() { },
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId)
                .queryParam("name", name));
        if (defs.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "workflow not found in the latest revision of project id = %d: %s",
                        projId, name));
        }
        else {
            return defs.get(0);
        }
    }

    public RestWorkflowDefinition getWorkflowDefinition(int projId, String name, String revision)
    {
        List<RestWorkflowDefinition> defs = doGet(new GenericType<List<RestWorkflowDefinition>>() { },
                target("/api/projects/{id}/workflows")
                .resolveTemplate("id", projId)
                .queryParam("name", name)
                .queryParam("revision", revision));
        if (defs.isEmpty()) {
            throw new NotFoundException(String.format(ENGLISH,
                        "workflow not found in revision = %s of project id = %d: %s",
                        revision, projId, name));
        }
        else {
            return defs.get(0);
        }
    }

    public RestWorkflowDefinition getWorkflowDefinition(long workflowId)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/workflows/{id}")
                .resolveTemplate("id", workflowId));
    }

    public RestWorkflowSessionTime getWorkflowTruncatedSessionTime(long workflowId, LocalTimeOrInstant time)
    {
        return doGet(RestWorkflowSessionTime.class,
                target("/api/workflows/{id}/truncated_session_time")
                .resolveTemplate("id", workflowId)
                .queryParam("session_time", time.toString()));
    }

    public RestWorkflowSessionTime getWorkflowTruncatedSessionTime(long workflowId, LocalTimeOrInstant time, SessionTimeTruncate mode)
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
        return doPut(RestProject.class,
                "application/gzip",
                body,
                target("/api/projects")
                .queryParam("project", projName)
                .queryParam("revision", revision));
    }

    // TODO getArchive with streaming
    public InputStream getProjectArchive(int projId, String revision)
    {
        Response res = target("/api/projects/{id}/archive")
            .resolveTemplate("id", projId)
            .queryParam("revision", revision)
            .request()
            .headers(headers.get())
            .get();
        // TODO check status code
        return res.readEntity(InputStream.class);
    }

    public List<RestSchedule> getSchedules()
    {
        return doGet(new GenericType<List<RestSchedule>>() { },
                target("/api/schedules"));
    }

    public RestSchedule getSchedule(long id)
    {
        return doGet(RestSchedule.class,
                target("/api/schedules/{id}")
                .resolveTemplate("id", id));
    }

    public List<RestSession> getSessions() {
        return getSessions(Optional.absent());
    }

    public List<RestSession> getSessions(Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSession>>() {},
                target("/api/sessions")
                        .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSession> getSessions(int projectId) {
        return getSessions(projectId, Optional.absent());
    }

    public List<RestSession> getSessions(int projectId, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSession>>() {},
                target("/api/projects/{projectId}/sessions")
                        .resolveTemplate("projectId", projectId)
                        .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSession> getSessions(int projectId, String workflowName) {
        return getSessions(projectId, workflowName, Optional.absent());
    }

    public List<RestSession> getSessions(int projectId, String workflowName, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSession>>() {},
                target("/api/projects/{projectId}/sessions")
                        .resolveTemplate("projectId", projectId)
                        .queryParam("workflow", workflowName)
                        .queryParam("last_id", lastId.orNull()));
    }

    public RestSession getSession(long sessionId)
    {
        return doGet(RestSession.class,
                target("/api/sessions/{id}")
                        .resolveTemplate("id", sessionId));
    }

    public List<RestSessionAttempt> getSessionAttempts(long sessionId, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() {},
                target("/api/sessions/{sessionId}/attempts")
                        .resolveTemplate("sessionId", sessionId)
                        .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSessionAttempt> getSessionAttempts(Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() { },
                target("/api/attempts")
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSessionAttempt> getSessionAttempts(String projName, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() { },
                target("/api/attempts")
                .queryParam("project", projName)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSessionAttempt> getSessionAttempts(String projName, String workflowName, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() { },
                target("/api/attempts")
                .queryParam("project", projName)
                .queryParam("workflow", workflowName)
                .queryParam("last_id", lastId.orNull()));
    }

    public RestSessionAttempt getSessionAttempt(long attemptId)
    {
        return doGet(RestSessionAttempt.class,
                target("/api/attempts/{id}")
                .resolveTemplate("id", attemptId));
    }

    public List<RestTask> getTasks(long attemptId)
    {
        return doGet(new GenericType<List<RestTask>>() { },
                target("/api/attempts/{id}/tasks")
                .resolveTemplate("id", attemptId));
    }

    public List<RestLogFileHandle> getLogFileHandlesOfAttempt(long attemptId)
    {
        return doGet(new GenericType<List<RestLogFileHandle>>() { },
                target("/api/logs/{id}/files")
                .resolveTemplate("id", attemptId));
    }

    public List<RestLogFileHandle> getLogFileHandlesOfTask(long attemptId, String taskName)
    {
        return doGet(new GenericType<List<RestLogFileHandle>>() { },
                target("/api/logs/{id}/files")
                .resolveTemplate("id", attemptId)
                .queryParam("task", taskName));
    }

    public InputStream getLogFile(long attemptId, RestLogFileHandle handle)
    {
        if (handle.getDirect().isPresent() && handle.getDirect().get().getType().equals("http")) {
            Response res = client.target(UriBuilder.fromUri(handle.getDirect().get().getUrl()))
                    .request()
                    .get();
            // TODO check status code
            return res.readEntity(InputStream.class);
        }
        else {
            return getLogFile(attemptId, handle.getFileName());
        }
    }

    public InputStream getLogFile(long attemptId, String fileName)
    {
        Response res = target("/api/logs/{id}/files/{fileName}")
            .resolveTemplate("id", attemptId)
            .resolveTemplate("fileName", fileName)
            .request()
            .headers(headers.get())
            .get();
        // TODO check status code
        return res.readEntity(InputStream.class);
    }

    public RestSessionAttempt startSessionAttempt(RestSessionAttemptRequest request)
    {
        return doPut(RestSessionAttempt.class,
                "application/json",
                request,
                target("/api/attempts"));
    }

    public void killSessionAttempt(long attemptId)
    {
        doPost(void.class,
                new HashMap<String, String>(),
                target("/api/attempts/{id}/kill")
                .resolveTemplate("id", attemptId));
    }

    public RestScheduleSummary skipSchedulesToTime(int scheduleId, Instant untilTime, Optional<Instant> runTime, boolean dryRun)
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

    public RestScheduleSummary skipSchedulesToTime(int scheduleId, LocalDateTime untilTime, Optional<Instant> runTime, boolean dryRun)
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

    public RestScheduleSummary skipSchedulesByCount(int scheduleId, Instant fromTime, int count, Optional<Instant> runTime, boolean dryRun)
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

    public List<RestSessionAttempt> backfillSchedule(int scheduleId, Instant fromTime, String attemptName, Optional<Integer> count, boolean dryRun)
    {
        return doPost(new GenericType<List<RestSessionAttempt>>() { },
                RestScheduleBackfillRequest.builder()
                    .fromTime(fromTime)
                    .dryRun(dryRun)
                    .attemptName(attemptName)
                    .count(count)
                    .build(),
                target("/api/schedules/{id}/backfill")
                .resolveTemplate("id", scheduleId));
    }

    public Map<String, Object> getVersion()
    {
        return doGet(new GenericType<Map<String, Object>>() {},
                target("/api/version"));
    }

    private WebTarget target(String path)
    {
        return client.target(UriBuilder.fromUri(endpoint + path));
    }

    private <T> T doGet(GenericType<T> type, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .get(type);
    }

    private <T> T doGet(Class<T> type, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .get(type);
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
        target.request("application/json")
            .headers(headers.get())
            .delete();
    }

    private <T> T doDelete(Class<T> type, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers.get())
            .delete(type);
    }
}
