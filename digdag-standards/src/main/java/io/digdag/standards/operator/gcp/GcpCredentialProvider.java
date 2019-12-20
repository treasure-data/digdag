package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.SecretNotFoundException;
import io.digdag.spi.TaskExecutionException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;

class GcpCredentialProvider
{
    private final ObjectMapper objectMapper;
    private static final String IAM_SCOPE = "https://www.googleapis.com/auth/iam";

    @Inject
    GcpCredentialProvider(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    GcpCredential credential(SecretProvider secrets)
    {
        try
        {
            String credential = secrets.getSecret("gcp.credential");
            return ImmutableGcpCredential.builder()
                    .projectId(credentialProjectId(credential))
                    .credential(googleCredential(credential))
                    .build();
        }
        catch(SecretNotFoundException e)
        {
            return ImmutableGcpCredential.builder()
                    .credential(googleCredential())
                    .build();
        }
    }

    private Optional<String> credentialProjectId(String credential)
    {
        JsonNode node;
        try {
            node = objectMapper.readTree(credential);
        }
        catch (IOException e) {
            throw new TaskExecutionException("Unable to parse 'gcp.credential' secret", e);
        }
        JsonNode projectId = node.get("project_id");
        if (projectId == null || !projectId.isTextual()) {
            return Optional.absent();
        }
        return Optional.of(projectId.asText());
    }

    private GoogleCredential googleCredential(String credential)
    {
        try {
            return GoogleCredential.fromStream(new ByteArrayInputStream(credential.getBytes(UTF_8)));
        }
        catch (IOException e) {
            throw new TaskExecutionException(e);
        }
    }

    private GoogleCredential googleCredential()
    {
        GoogleCredential credential;
        try {
            credential = GoogleCredential.getApplicationDefault().createScoped(Collections.singleton(IAM_SCOPE));
            if (credential == null) {
              throw new TaskExecutionException("Could not obtain gcp credential.");
            }
        }
        catch (IOException e) {
            throw new TaskExecutionException(e);
        }
        return credential;
    }
}
