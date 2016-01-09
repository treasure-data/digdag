package io.digdag.client;

import java.util.List;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import com.google.inject.Guice;
import org.immutables.value.Value;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
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
    private final Client client;
    private final MultivaluedMap<String, Object> headers;

    private DigdagClient(Builder builder)
    {
        this.endpoint = "http://" + builder.host + ":" + builder.port;
        this.client = new ResteasyClientBuilder()
            .register(new JacksonJsonProvider(buildObjectMapper()))
            .build();
        this.headers = new MultivaluedHashMap<>();
    }

    private static ObjectMapper buildObjectMapper()
    {
        // use Guice here to make @JacksonInject work which is used at io.digdag.spi.config.Config.<init>
        return Guice.createInjector(
            new ObjectMapperModule()
                .registerModule(new GuavaModule())
                //.registerModule(new JodaModule())
            )
            .getInstance(ObjectMapper.class);
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

    public List<RestWorkflow> getWorkflows(int repoId)
    {
        return doGet(new GenericType<List<RestWorkflow>>() { },
                target("/api/repositories/{id}/workflows")
                .resolveTemplate("id", repoId));
    }

    public List<RestWorkflow> getWorkflows(int repoId, String revision)
    {
        return doGet(new GenericType<List<RestWorkflow>>() { },
                target("/api/repositories/{id}/workflows")
                .resolveTemplate("id", repoId)
                .queryParam("revision", revision));
    }

    // TODO getArchive with streaming
    // TODO putArchive with streaming

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

    public List<RestSession> getSessions()
    {
        return doGet(new GenericType<List<RestSession>>() { },
                target("/api/sessions"));
    }

    public List<RestSession> getSessions(String repoName)
    {
        return doGet(new GenericType<List<RestSession>>() { },
                target("/api/sessions")
                .queryParam("repository", repoName));
    }

    public List<RestSession> getSessions(String repoName, String workflowName)
    {
        return doGet(new GenericType<List<RestSession>>() { },
                target("/api/sessions")
                .queryParam("repository", repoName)
                .queryParam("workflow", workflowName));
    }

    public RestSession getSession(long sessionId)
    {
        return doGet(RestSession.class,
                target("/api/session/{id}")
                .resolveTemplate("id", sessionId));
    }

    public List<RestTask> getTasks(long sessionId)
    {
        return doGet(new GenericType<List<RestTask>>() { },
                target("/api/sessions/{id}/tasks")
                .resolveTemplate("id", sessionId));
    }

    // TODO startTask

    public List<RestWorkflow> getWorkflows()
    {
        return doGet(new GenericType<List<RestWorkflow>>() { },
                target("/api/workflows"));
    }

    public RestWorkflow getWorkflow(int workflowId)
    {
        return doGet(RestWorkflow.class,
                target("/api/workflows/{id}")
                .resolveTemplate("id", workflowId));
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
}
