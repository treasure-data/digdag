package io.digdag.client.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.Test;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ModelCompatibilityTest
{
    private ObjectMapper mapper = DigdagClient.objectMapper();

    @Test
    public void testSessionAttemptIndex()
            throws Exception
    {
        RestSessionAttempt attempt = RestSessionAttempt.builder()
            .id(Id.of("1"))
            .index(3)
            .project(IdAndName.of(Id.of("1"), "p"))
            .workflow(NameOptionalId.of("w", Optional.of(Id.of("1"))))
            .sessionId(Id.of("1"))
            .sessionUuid(UUID.randomUUID())
            .sessionTime(OffsetDateTime.now(ZoneId.of("UTC")))
            .retryAttemptName(Optional.absent())
            .done(false)
            .success(false)
            .cancelRequested(false)
            .params(newConfig())
            .createdAt(Instant.now())
            .finishedAt(Optional.absent())
            .build();
        assertThat(attempt.getIndex(), is(3));
        String oldVersion = removeField(mapper.writeValueAsString(attempt), "index");
        mapper.readValue(oldVersion, RestSessionAttempt.class);
    }

    @Test
    public void testRestRevisionDefaultUserInfo()
            throws Exception
    {
        RestRevision rev = RestRevision.builder()
            .revision("rev1")
            .createdAt(Instant.now())
            .archiveType("none")
            .archiveMd5(Optional.absent())
            .userInfo(newConfig())
            .build();
        String oldVersion = removeField(mapper.writeValueAsString(rev), "userInfo");
        mapper.readValue(oldVersion, RestRevision.class);
    }

    private String removeField(String json, String field)
            throws IOException
    {
        ObjectNode node = (ObjectNode) mapper.readTree(json);
        assertThat(node.remove(field) == null, is(false));
        return mapper.writeValueAsString(node);
    }

    @Test
    public void testSessionAttemptStatusSuccess()
            throws Exception
    {
        RestSessionAttempt attempt = RestSessionAttempt.builder()
            .id(Id.of("1"))
            .index(3)
            .project(IdAndName.of(Id.of("1"), "p"))
            .workflow(NameOptionalId.of("w", Optional.of(Id.of("1"))))
            .sessionId(Id.of("1"))
            .sessionUuid(UUID.randomUUID())
            .sessionTime(OffsetDateTime.now(ZoneId.of("UTC")))
            .retryAttemptName(Optional.absent())
            .done(false)
            .success(true)
            .cancelRequested(false)
            .params(newConfig())
            .createdAt(Instant.now())
            .finishedAt(Optional.absent())
            .build();
        assertThat(attempt.getStatus(), is("success"));
    }

    @Test
    public void testSessionAttemptStatusError()
            throws Exception
    {
        RestSessionAttempt attempt = RestSessionAttempt.builder()
            .id(Id.of("1"))
            .index(3)
            .project(IdAndName.of(Id.of("1"), "p"))
            .workflow(NameOptionalId.of("w", Optional.of(Id.of("1"))))
            .sessionId(Id.of("1"))
            .sessionUuid(UUID.randomUUID())
            .sessionTime(OffsetDateTime.now(ZoneId.of("UTC")))
            .retryAttemptName(Optional.absent())
            .done(true)
            .success(false)
            .cancelRequested(false)
            .params(newConfig())
            .createdAt(Instant.now())
            .finishedAt(Optional.absent())
            .build();
        assertThat(attempt.getStatus(), is("error"));
    }

    @Test
    public void testSessionAttemptStatusKilled()
            throws Exception
    {
        RestSessionAttempt attempt = RestSessionAttempt.builder()
            .id(Id.of("1"))
            .index(3)
            .project(IdAndName.of(Id.of("1"), "p"))
            .workflow(NameOptionalId.of("w", Optional.of(Id.of("1"))))
            .sessionId(Id.of("1"))
            .sessionUuid(UUID.randomUUID())
            .sessionTime(OffsetDateTime.now(ZoneId.of("UTC")))
            .retryAttemptName(Optional.absent())
            .done(true)
            .success(false)
            .cancelRequested(true)
            .params(newConfig())
            .createdAt(Instant.now())
            .finishedAt(Optional.absent())
            .build();
        assertThat(attempt.getStatus(), is("killed"));
    }

    @Test
    public void testSessionAttemptStatusRunning()
            throws Exception
    {
        RestSessionAttempt attempt = RestSessionAttempt.builder()
            .id(Id.of("1"))
            .index(3)
            .project(IdAndName.of(Id.of("1"), "p"))
            .workflow(NameOptionalId.of("w", Optional.of(Id.of("1"))))
            .sessionId(Id.of("1"))
            .sessionUuid(UUID.randomUUID())
            .sessionTime(OffsetDateTime.now(ZoneId.of("UTC")))
            .retryAttemptName(Optional.absent())
            .done(false)
            .success(false)
            .cancelRequested(false)
            .params(newConfig())
            .createdAt(Instant.now())
            .finishedAt(Optional.absent())
            .build();
        assertThat(attempt.getStatus(), is("running"));
    }
}
