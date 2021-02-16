package io.digdag.standards.operator.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) // these tests shouldn't be executed concurrently.
@RunWith(MockitoJUnitRunner.class)
public class GcpCredentialProviderTest
{
    private static final ObjectMapper OBJECT_MAPPER = DigdagClient.objectMapper();

    @Mock SecretProvider secrets;

    @Test
    public void testCredentialWithServiceAccountKey() throws Exception
    {
        String cred = Resources.toString(getClass().getResource("dummy-service-account-key.json"), UTF_8);
        when(secrets.getSecretOptional("gcp.credential")).thenReturn(Optional.of(cred));

        GcpCredentialProvider provider = new GcpCredentialProvider(OBJECT_MAPPER);
        GoogleCredential credential = provider.credential(secrets).credential();

        assertEquals("dummy-506@dummy-project.iam.gserviceaccount.com", credential.getServiceAccountId());
    }

    @Test
    public void testCredentialWithADC() throws Exception
    {
        when(secrets.getSecretOptional("gcp.credential")).thenReturn(Optional.absent());
        Path dummyKeyPath = Paths.get(getClass().getResource("dummy-service-account-key.json").toURI());
        Files.copy(dummyKeyPath, Paths.get(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")));

        GcpCredentialProvider provider = new GcpCredentialProvider(OBJECT_MAPPER);
        GoogleCredential credential = provider.credential(secrets).credential();

        assertEquals("dummy-506@dummy-project.iam.gserviceaccount.com", credential.getServiceAccountId());
    }

    @Test(expected = TaskExecutionException.class)
    public void testCredentialFailure() throws Exception
    {
        when(secrets.getSecretOptional("gcp.credential")).thenReturn(Optional.absent());
        File dummyKeyFile = new File(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
        dummyKeyFile.delete();

        GcpCredentialProvider provider = new GcpCredentialProvider(OBJECT_MAPPER);
        GoogleCredential credential = provider.credential(secrets).credential();

        assertEquals("dummy-506@dummy-project.iam.gserviceaccount.com", credential.getServiceAccountId());
    }
}
