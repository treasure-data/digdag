package io.digdag.client.api;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import java.io.IOException;
import java.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ModelCompatibilityTest
{
    private ObjectMapper mapper = DigdagClient.objectMapper();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

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

    private String removeField(String json, String field)
            throws IOException
    {
        ObjectNode node = (ObjectNode) mapper.readTree(json);
        assertThat(node.remove(field) == null, is(false));
        return mapper.writeValueAsString(node);
    }
}
