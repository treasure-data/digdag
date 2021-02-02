package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.ServiceOptions;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

class GcpCredentialProvider
{
    private final ObjectMapper objectMapper;

    @Inject
    GcpCredentialProvider(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    GcpCredential credential(SecretProvider secrets)
    {
        Optional<String> credentialOpt = secrets.getSecretOptional("gcp.credential");
         ImmutableGcpCredential.Builder builder = ImmutableGcpCredential.builder();
         if (credentialOpt.isPresent()) {
           String credential = credentialOpt.get();
           builder.projectId(credentialProjectId(credential))
             .credential(googleCredential(credential));
         }
         else {
           try {
             builder.projectId(ServiceOptions.getDefaultProjectId())
               .credential(GoogleCredential.getApplicationDefault());
           }
           catch (IOException e) {
             throw new TaskExecutionException(
               "Could not get google cloud credential: need gcp.credential secret or Application Default Credentials",
               e
             );
           }
         }
         return builder.build();
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
}
