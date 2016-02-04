package io.digdag.client;

import java.util.List;
import java.util.Date;
import java.util.HashMap;
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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.common.base.Optional;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import com.fasterxml.jackson.databind.InjectableValues;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.*;

public class DigdagClient
{
    public static class Builder
    {
        private String host;
        private int port;

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

        public DigdagClient build()
        {
            return new DigdagClient(this);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final String endpoint;
    private final MultivaluedMap<String, Object> headers;

    private final Client client;
    private final ConfigFactory cf;

    private DigdagClient(Builder builder)
    {
        this.endpoint = "http://" + builder.host + ":" + builder.port;

        this.headers = new MultivaluedHashMap<>();

        Injector injector = buildInjector();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JacksonTimeModule());

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

    private static Injector buildInjector()
    {
        return Guice.createInjector(
            (binder) -> {
                binder.bind(ConfigFactory.class);
            },
            new ObjectMapperModule()
                .registerModule(new GuavaModule())
                .registerModule(new JacksonTimeModule())
            );
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

    public RestRepository getRepository(int repoId, String revision)
    {
        return doGet(RestRepository.class,
                target("/api/repository/{id}")
                .resolveTemplate("id", repoId)
                .queryParam("revision", revision));
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

    public RestRepository putRepositoryRevision(String repoName, String revision, File body)
        throws IOException
    {
        return doPut(RestRepository.class,
                "application/x-gzip",
                body,  // TODO does this work?
                target("/api/repositories")
                .queryParam("repository", repoName)
                .queryParam("revision", revision));
    }

    // TODO getArchive with streaming
    public InputStream getRepositoryArchive(int repoId, String revision)
    {
        Response res = target("/api/repositories/{id]}/archive")
            .resolveTemplate("id", repoId)
            .queryParam("revision", revision)
            .request()
            .headers(headers)
            .get();
        return res.readEntity(InputStream.class);  // TODO does this work?
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

    public List<RestSession> getSessions(boolean includeRetried, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSession>>() { },
                target("/api/sessions")
                .queryParam("include_retried", includeRetried)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSession> getSessions(String repoName, boolean includeRetried, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSession>>() { },
                target("/api/sessions")
                .queryParam("repository", repoName)
                .queryParam("include_retried", includeRetried)
                .queryParam("last_id", lastId.orNull()));
    }

    public List<RestSession> getSessions(String repoName, String workflowName, boolean includeRetried, Optional<Long> lastId)
    {
        return doGet(new GenericType<List<RestSession>>() { },
                target("/api/sessions")
                .queryParam("repository", repoName)
                .queryParam("workflow", workflowName)
                .queryParam("include_retried", includeRetried)
                .queryParam("last_id", lastId.orNull()));
    }

    public RestSession getSession(long sessionId)
    {
        return doGet(RestSession.class,
                target("/api/sessions/{id}")
                .resolveTemplate("id", sessionId));
    }

    public List<RestTask> getTasks(long sessionId)
    {
        return doGet(new GenericType<List<RestTask>>() { },
                target("/api/sessions/{id}/tasks")
                .resolveTemplate("id", sessionId));
    }


    public RestSession startSession(RestSessionRequest request)
    {
        return doPut(RestSession.class,
                "application/json",
                request,
                target("/api/sessions"));
    }

    public void killSession(long sessionId)
    {
        doPost(void.class,
                new HashMap<String, String>(),
                target("/api/sessions/{id}/kill")
                .resolveTemplate("id", sessionId));
    }

    //public List<RestWorkflowDefinition> getWorkflowDefinitions()
    //{
    //    return doGet(new GenericType<List<RestWorkflowDefinition>>() { },
    //            target("/api/workflows"));
    //}

    public RestWorkflowDefinition getWorkflowDefinition(long workflowId)
    {
        return doGet(RestWorkflowDefinition.class,
                target("/api/workflows/{id}")
                .resolveTemplate("id", workflowId));
    }

    public RestScheduleSummary skipSchedulesToTime(long scheduleId, Date date, Optional<Date> runTime, boolean dryRun)
    {
        return doPost(RestScheduleSummary.class,
                RestScheduleSkipRequest.builder()
                    .nextTime(date.getTime() / 1000)
                    .nextRunTime(runTime.transform(d -> d.getTime() / 1000))
                    .dryRun(dryRun)
                    .build(),
                target("/api/schedules/{id}/skip")
                .resolveTemplate("id", scheduleId));
    }

    public RestScheduleSummary skipSchedulesByCount(long scheduleId, Date fromTime, int count, Optional<Date> runTime, boolean dryRun)
    {
        return doPost(RestScheduleSummary.class,
                RestScheduleSkipRequest.builder()
                    .fromTime(fromTime.getTime() / 1000)
                    .count(count)
                    .nextRunTime(runTime.transform(d -> d.getTime() / 1000))
                    .dryRun(dryRun)
                    .build(),
                target("/api/schedules/{id}/skip")
                .resolveTemplate("id", scheduleId));
    }

    public List<RestSession> backfillSchedule(long scheduleId, Date fromTime, String attemptName, boolean dryRun)
    {
        return doPost(new GenericType<List<RestSession>>() { },
                RestScheduleBackfillRequest.builder()
                    .fromTime(fromTime.getTime() / 1000)
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
            .headers(headers)
            .get(type);
    }

    private <T> T doGet(Class<T> type, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers)
            .get(type);
    }

    private <T> T doPut(Class<T> type, String contentType, Object body, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers)
            .put(Entity.entity(body, contentType), type);
    }

    private <T> T doPost(GenericType<T> type, Object body, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers)
            .post(Entity.entity(body, "application/json"), type);
    }

    private <T> T doPost(Class<T> type, Object body, WebTarget target)
    {
        return target.request("application/json")
            .headers(headers)
            .post(Entity.entity(body, "application/json"), type);
    }
}
