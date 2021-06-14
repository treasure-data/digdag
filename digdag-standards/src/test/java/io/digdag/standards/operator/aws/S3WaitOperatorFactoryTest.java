package io.digdag.standards.operator.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretNotFoundException;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.digdag.client.config.ConfigUtils.configFactory;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3WaitOperatorFactoryTest
{
    private static final int MAX_POLL_INTERVAL = 300;

    private static final String BUCKET = "test.bucket";
    private static final String KEY = "a/test/key";

    private static final String ACCESS_KEY_ID = "test-access-key-id";
    private static final String SECRET_ACCESS_KEY = "test-secret-access-key";
    private static final String CONTENT_TYPE = "text/plain";
    private static final long CONTENT_LENGTH = 4711;
    private static final Map<String, String> USER_METADATA = ImmutableMap.of(
            "test-name-1", "test-value-1",
            "test-name-2", "test-value-2");

    private static final String REGION = "us-west-2";

    private static final AmazonS3Exception NOT_FOUND_EXCEPTION;

    private static final String SSE_C_KEY = Base64.getEncoder().encodeToString("test-sse-c-key".getBytes(UTF_8));
    private static final String SSE_C_KEY_MD5 = Base64.getEncoder().encodeToString("test-sse-c-key-md5".getBytes(UTF_8));
    private static final String SSE_C_KEY_ALGORITHM = Base64.getEncoder().encodeToString("test-sse-c-key-algorithm".getBytes(UTF_8));
    private static final String VERSION_ID = "test-version-id";

    static {
        NOT_FOUND_EXCEPTION = new AmazonS3Exception("Not Found");
        NOT_FOUND_EXCEPTION.setStatusCode(404);
    }

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock TaskRequest taskRequest;
    @Mock SecretProvider secretProvider;
    @Mock SecretProvider awsSecrets;
    @Mock SecretProvider s3Secrets;

    @Mock AmazonS3Client s3Client;
    @Mock S3WaitOperatorFactory.AmazonS3ClientFactory s3ClientFactory;

    @Captor ArgumentCaptor<AWSCredentials> credentialsCaptor;
    @Captor ArgumentCaptor<ClientConfiguration> clientConfigurationCaptor;
    @Captor ArgumentCaptor<GetObjectMetadataRequest> objectMetadataRequestCaptor;
    @Captor ArgumentCaptor<S3ClientOptions> s3ClientOptionsCaptor;

    private final Map<String, String> environment = new HashMap<>();

    private S3WaitOperatorFactory factory;

    private Path projectPath;

    @Before
    public void setUp()
            throws Exception
    {
        when(taskRequest.getLastStateParams()).thenReturn(newConfig());

        when(s3Secrets.getSecret(anyString())).then(i -> {
            throw new SecretNotFoundException(i.getArgumentAt(0, String.class));
        });
        when(awsSecrets.getSecret(anyString())).then(i -> {
            throw new SecretNotFoundException(i.getArgumentAt(0, String.class));
        });
        when(s3Secrets.getSecretOptional(anyString())).thenReturn(Optional.absent());
        when(awsSecrets.getSecretOptional(anyString())).thenReturn(Optional.absent());

        when(secretProvider.getSecrets("aws")).thenReturn(awsSecrets);
        when(awsSecrets.getSecrets("s3")).thenReturn(s3Secrets);
        when(s3Secrets.getSecretOptional("access_key_id")).thenReturn(Optional.of(ACCESS_KEY_ID));
        when(s3Secrets.getSecretOptional("secret_access_key")).thenReturn(Optional.of(SECRET_ACCESS_KEY));

        when(s3ClientFactory.create(any(AWSCredentials.class), any(ClientConfiguration.class))).thenReturn(s3Client);
        projectPath = temporaryFolder.newFolder().toPath();
        factory = new S3WaitOperatorFactory(s3ClientFactory, environment);
    }

    @Test
    public void testDefaults()
            throws Exception
    {
        Config config = newConfig();

        config.set("_command", BUCKET + "/" + KEY);

        when(taskRequest.getConfig()).thenReturn(config);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(CONTENT_TYPE);
        objectMetadata.setContentLength(CONTENT_LENGTH);
        objectMetadata.setUserMetadata(USER_METADATA);

        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));

        when(s3Client.getObjectMetadata(objectMetadataRequestCaptor.capture())).thenReturn(objectMetadata);

        TaskResult taskResult = operator.run();

        verify(s3ClientFactory).create(credentialsCaptor.capture(), clientConfigurationCaptor.capture());

        ClientConfiguration clientConfiguration = clientConfigurationCaptor.getValue();
        assertThat(clientConfiguration.getProxyHost(), is(nullValue()));
        assertThat(clientConfiguration.getProxyPort(), is(-1));
        assertThat(clientConfiguration.getProxyUsername(), is(nullValue()));
        assertThat(clientConfiguration.getProxyPassword(), is(nullValue()));

        verify(s3Client).setS3ClientOptions(s3ClientOptionsCaptor.capture());
        S3ClientOptions s3ClientOptions = s3ClientOptionsCaptor.getValue();
        assertThat(s3ClientOptions.isPathStyleAccess(), is(false));

        AWSCredentials credentials = credentialsCaptor.getValue();
        assertThat(credentials.getAWSAccessKeyId(), is(ACCESS_KEY_ID));
        assertThat(credentials.getAWSSecretKey(), is(SECRET_ACCESS_KEY));

        GetObjectMetadataRequest objectMetadataRequest = objectMetadataRequestCaptor.getValue();
        assertThat(objectMetadataRequest.getKey(), is(KEY));
        assertThat(objectMetadataRequest.getBucketName(), is(BUCKET));
        assertThat(objectMetadataRequest.getSSECustomerKey(), is(nullValue()));

        Config expectedStoreParams = newConfig();
        expectedStoreParams
                .getNestedOrSetEmpty("s3")
                .getNestedOrSetEmpty("last_object")
                .set("metadata", objectMetadata.getRawMetadata())
                .set("user_metadata", objectMetadata.getUserMetadata());

        assertThat(taskResult.getStoreParams(), is(expectedStoreParams));
    }

    @Test
    public void testExponentialBackoff()
            throws Exception
    {
        Config config = newConfig();
        config.set("_command", BUCKET + "/" + KEY);

        when(taskRequest.getConfig()).thenReturn(config);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(CONTENT_TYPE);
        objectMetadata.setContentLength(CONTENT_LENGTH);
        objectMetadata.setUserMetadata(USER_METADATA);

        when(s3Client.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenThrow(NOT_FOUND_EXCEPTION);

        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));

        List<Integer> retryIntervals = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            try {
                operator.run();
                fail();
            }
            catch (TaskExecutionException e) {
                assertThat(e.isError(), is(false));
                assertThat(e.getRetryInterval().isPresent(), is(true));
                retryIntervals.add(e.getRetryInterval().get());
                Config lastStateParams = e.getStateParams(configFactory).get();
                when(taskRequest.getLastStateParams()).thenReturn(lastStateParams);
            }
        }

        for (int i = 1; i < retryIntervals.size(); i++) {
            int prevInterval = retryIntervals.get(i - 1);
            int interval = retryIntervals.get(i);
            assertThat(interval, is(Math.min(MAX_POLL_INTERVAL, prevInterval * 2)));
        }

        assertThat(retryIntervals.get(retryIntervals.size() - 1), is(MAX_POLL_INTERVAL));
    }

    @Test
    public void testCustomRegionAndPathStyleAccess()
            throws Exception
    {
        Config config = newConfig();
        config.set("path_style_access", true);
        config.set("_command", BUCKET + "/" + KEY);
        when(taskRequest.getConfig()).thenReturn(config);

        when(s3Secrets.getSecretOptional("region")).thenReturn(Optional.of(REGION));

        when(s3Client.getObjectMetadata(objectMetadataRequestCaptor.capture())).thenThrow(NOT_FOUND_EXCEPTION);

        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));

        try {
            operator.run();
            fail();
        }
        catch (TaskExecutionException ignore) {
        }

        verify(s3Client).setS3ClientOptions(s3ClientOptionsCaptor.capture());
        S3ClientOptions s3ClientOptions = s3ClientOptionsCaptor.getValue();
        assertThat(s3ClientOptions.isPathStyleAccess(), is(true));

        verify(s3Client).setRegion(RegionUtils.getRegion(REGION));
    }

    @Test
    public void testVersionId()
            throws Exception
    {
        Config config = newConfig();
        config.set("_command", BUCKET + "/" + KEY);
        config.set("version_id", VERSION_ID);
        when(taskRequest.getConfig()).thenReturn(config);

        when(s3Secrets.getSecretOptional("region")).thenReturn(Optional.of(REGION));

        when(s3Client.getObjectMetadata(objectMetadataRequestCaptor.capture())).thenThrow(NOT_FOUND_EXCEPTION);

        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));

        try {
            operator.run();
            fail();
        }
        catch (TaskExecutionException ignore) {
        }

        GetObjectMetadataRequest objectMetadataRequest = objectMetadataRequestCaptor.getValue();

        assertThat(objectMetadataRequest.getVersionId(), is(VERSION_ID));
    }

    @Test
    public void testSSEC()
            throws Exception
    {
        Config config = newConfig();
        config.set("_command", BUCKET + "/" + KEY);
        when(taskRequest.getConfig()).thenReturn(config);

        when(s3Secrets.getSecretOptional("sse_c_key")).thenReturn(Optional.of(SSE_C_KEY));

        when(s3Client.getObjectMetadata(objectMetadataRequestCaptor.capture())).thenThrow(NOT_FOUND_EXCEPTION);

        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));

        try {
            operator.run();
            fail();
        }
        catch (TaskExecutionException ignore) {
        }

        GetObjectMetadataRequest objectMetadataRequest = objectMetadataRequestCaptor.getValue();
        assertThat(objectMetadataRequest.getSSECustomerKey().getKey(), is(SSE_C_KEY));
        assertThat(objectMetadataRequest.getSSECustomerKey().getAlgorithm(), is("AES256"));
        assertThat(objectMetadataRequest.getSSECustomerKey().getMd5(), is(nullValue()));
    }

    @Test
    public void testSSECWithAlgorithmAndMd5()
            throws Exception
    {
        Config config = newConfig();
        config.set("_command", BUCKET + "/" + KEY);
        when(taskRequest.getConfig()).thenReturn(config);

        when(s3Secrets.getSecretOptional("sse_c_key")).thenReturn(Optional.of(SSE_C_KEY));
        when(s3Secrets.getSecretOptional("sse_c_key_algorithm")).thenReturn(Optional.of(SSE_C_KEY_ALGORITHM));
        when(s3Secrets.getSecretOptional("sse_c_key_md5")).thenReturn(Optional.of(SSE_C_KEY_MD5));

        when(s3Client.getObjectMetadata(objectMetadataRequestCaptor.capture())).thenThrow(NOT_FOUND_EXCEPTION);

        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));

        try {
            operator.run();
            fail();
        }
        catch (TaskExecutionException ignore) {
        }

        GetObjectMetadataRequest objectMetadataRequest = objectMetadataRequestCaptor.getValue();
        assertThat(objectMetadataRequest.getSSECustomerKey().getKey(), is(SSE_C_KEY));
        assertThat(objectMetadataRequest.getSSECustomerKey().getAlgorithm(), is(SSE_C_KEY_ALGORITHM));
        assertThat(objectMetadataRequest.getSSECustomerKey().getMd5(), is(SSE_C_KEY_MD5));
    }

    @Test
    public void testProxy()
            throws Exception
    {
        Config config = newConfig();
        config.set("_command", BUCKET + "/" + KEY);
        when(taskRequest.getConfig()).thenReturn(config);

        environment.put("http_proxy", "http://foo:bar@1.2.3.4:4711");

        when(s3Client.getObjectMetadata(objectMetadataRequestCaptor.capture())).thenThrow(NOT_FOUND_EXCEPTION);
        Operator operator = factory.newOperator(newContext(projectPath, taskRequest));
        try {
            operator.run();
            fail();
        }
        catch (TaskExecutionException ignore) {
        }

        verify(s3ClientFactory).create(any(AWSCredentials.class), clientConfigurationCaptor.capture());

        ClientConfiguration clientConfiguration = clientConfigurationCaptor.getValue();
        assertThat(clientConfiguration.getProxyHost(), is("1.2.3.4"));
        assertThat(clientConfiguration.getProxyPort(), is(4711));
        assertThat(clientConfiguration.getProxyUsername(), is("foo"));
        assertThat(clientConfiguration.getProxyPassword(), is("bar"));
    }

    private OperatorContext newContext(final Path projectPath, final TaskRequest taskRequest)
    {
        return new OperatorContext()
        {
            @Override
            public Path getProjectPath()
            {
                return projectPath;
            }

            @Override
            public TaskRequest getTaskRequest()
            {
                return taskRequest;
            }

            @Override
            public PrivilegedVariables getPrivilegedVariables()
            {
                return null;
            }

            @Override
            public SecretProvider getSecrets()
            {
                return secretProvider;
            }
        };
    }
}
