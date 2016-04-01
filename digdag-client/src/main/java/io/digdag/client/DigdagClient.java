package io.digdag.client;

import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.time.Instant;
import java.time.LocalDateTime;
import java.security.Key;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.databind.InjectableValues;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.MacProvider;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.*;

public class DigdagClient
{
    public static class Builder
    {
        private String host;
        private int port;
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

        public Builder apiKeyHeaderBuilder(Optional<RestApiKey> apiKey)
        {
            return apiKeyHeaderBuilder(apiKey.orNull());
        }

        public Builder apiKeyHeaderBuilder(RestApiKey apiKey)
        {
            if (apiKey == null) {
                headerBuilder = null;
            }
            else {
                headerBuilder = (orig) -> buildApiKeyHeader(apiKey, orig);
            }
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
        this.endpoint = "http://" + builder.host + ":" + builder.port;

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

    public static Map<String, String> buildApiKeyHeader(RestApiKey apiKey, Map<String, String> map)
    {
        map.put("Authorization", buildApiKeyHeader(apiKey));
        return map;
    }

    public static String buildApiKeyHeader(RestApiKey apiKey)
    {
        Instant now = Instant.now();

        String sharedKey =
            Jwts.builder()
            .setSubject(apiKey.getIdString())
            .setExpiration(Date.from(now.plusSeconds(300)))
            .setHeaderParam("knd", "ps1")
            .setIssuedAt(Date.from(now))
            .signWith(SignatureAlgorithm.HS512, apiKey.getSecret())
            .compact();

        return "Bearer " + sharedKey;
    }

    public Config newConfig()
    {
        return cf.create();
    }

    public RestRepository getRepository(String name)
    {
        return doGet(RestRepository.class,
                target("/api/repository")
                .queryParam("name", name));
    }

    public RestRepository getRepository(String name, String revision)
    {
        return doGet(RestRepository.class,
                target("/api/repository")
                .queryParam("name", name)
                .queryParam("revision", revision));
    }

    public List<RestRepository> getRepositories()
    {
        return doGet(new GenericType<List<RestRepository>>() { },
                target("/api/repositories"));
    }

    public RestRepository getRepository(int repoId)
    {
        return doGet(RestRepository.class,
                target("/api/repositories/{id}")
                .resolveTemplate("id", repoId));
    }


    public List<RestRevision> getRevisions(int repoId, Optional<Integer> lastId)
    {
        return doGet(new GenericType<List<RestRevision>>() { },
                target("/api/repository/{id}/revisions")
                .resolveTemplate("id", repoId)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestWorkflowDefinition> getWorkflowDefinitions(int repoId)
    {
        return doGet(new GenericType<List<RestWorkflowDefinition>>() { },
                target("/api/repositories/{id}/workflows")
                .resolveTemplate("id", repoId));
    }

    public List<RestWorkflowDefinition> getWorkflowDefinitions(int repoId, String revision)
    {
        return doGet(new GenericType<List<RestWorkflowDefinition>>() { },
                target("/api/repositories/{id}/workflows")
                .resolveTemplate("id", repoId)
                .queryParam("revision", revision));
    }

    public RestWorkflowDefinition getWorkflowDefinition(int repoId, String name)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/repositories/{id}/workflow")
                .resolveTemplate("id", repoId)
                .queryParam("name", name));
    }

    public RestWorkflowDefinition getWorkflowDefinition(int repoId, String name, String revision)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/repositories/{id}/workflow")
                .resolveTemplate("id", repoId)
                .queryParam("name", name)
                .queryParam("revision", revision));
    }

    public RestWorkflowDefinition getWorkflowDefinition(String repoName, String name)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/workflow")
                .queryParam("name", name)
                .queryParam("repository", repoName));
    }

    public RestWorkflowDefinition getWorkflowDefinition(String repoName, String name, String revision)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/workflow")
                .queryParam("name", name)
                .queryParam("repository", repoName)
                .queryParam("revision", revision));
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
                .queryParam("mode", mode.toString()));
    }

    public RestRepository putRepositoryRevision(String repoName, String revision, File body)
        throws IOException
    {
        return doPut(RestRepository.class,
                "application/gzip",
                body,
                target("/api/repositories")
                .queryParam("repository", repoName)
                .queryParam("revision", revision));
    }

    // TODO getArchive with streaming
    public InputStream getRepositoryArchive(int repoId, String revision)
    {
        Response res = target("/api/repositories/{id}/archive")
            .resolveTemplate("id", repoId)
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

    public List<RestSessionAttempt> getSessionAttempts(boolean includeRetried, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() { },
                target("/api/attempts")
                .queryParam("include_retried", includeRetried)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSessionAttempt> getSessionAttempts(String repoName, boolean includeRetried, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() { },
                target("/api/attempts")
                .queryParam("repository", repoName)
                .queryParam("include_retried", includeRetried)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSessionAttempt> getSessionAttempts(String repoName, String workflowName, boolean includeRetried, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSessionAttempt>>() { },
                target("/api/attempts")
                .queryParam("repository", repoName)
                .queryParam("workflow", workflowName)
                .queryParam("include_retried", includeRetried)
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
                .queryParam("task_name", taskName));
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

    public List<RestSessionAttempt> backfillSchedule(int scheduleId, Instant fromTime, String attemptName, boolean dryRun)
    {
        return doPost(new GenericType<List<RestSessionAttempt>>() { },
                RestScheduleBackfillRequest.builder()
                    .fromTime(fromTime)
                    .dryRun(dryRun)
                    .attemptName(attemptName)
                    .build(),
                target("/api/schedules/{id}/backfill")
                .resolveTemplate("id", scheduleId));
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
}
