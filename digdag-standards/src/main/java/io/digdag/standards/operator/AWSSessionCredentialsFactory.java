package io.digdag.standards.operator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.DynamoDBv2Actions;
import com.amazonaws.auth.policy.actions.ElasticMapReduceActions;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.digdag.standards.operator.AWSSessionCredentialsFactory.Mode.READ;

public class AWSSessionCredentialsFactory
{
    private static final int DEFAULT_DURATION_SECONDS = 3 * 3600;
    private static final String DEFAULT_SESSION_NAME = "digdag-operator-session";
    private static final String URI_S3_PREFIX = "s3://";
    private static final String URI_DYNAMODB_PREFIX = "dynamodb://";
    private static final String URI_EMR_PREFIX = "emr://";
    private final String accessKeyId;
    private final String secretAccessKey;
    private final List<AcceptableUri> acceptableUris;
    private String roleArn;
    private String sessionName = DEFAULT_SESSION_NAME;
    private int durationSeconds = DEFAULT_DURATION_SECONDS;

    public enum Mode
    {
        READ, WRITE
    }

    public static class AcceptableUri
    {
        private final Mode mode;
        private final String uri;

        public AcceptableUri(Mode mode, String uri)
        {
            this.mode = mode;
            this.uri = uri;
        }
    }

    public AWSSessionCredentialsFactory(String accessKeyId, String secretAccessKey, List<AcceptableUri> acceptableUris)
    {
        checkNotNull(accessKeyId);
        checkNotNull(secretAccessKey);
        checkNotNull(acceptableUris);

        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.acceptableUris = acceptableUris;
    }

    public AWSSessionCredentialsFactory withRoleArn(String roleArn)
    {
        checkNotNull(roleArn);
        this.roleArn = roleArn;
        return this;
    }

    public AWSSessionCredentialsFactory withRoleSessionName(String roleSessionName)
    {
        checkNotNull(roleSessionName);
        this.sessionName = roleSessionName;
        return this;
    }

    public AWSSessionCredentialsFactory WithDurationSeconds(int durationSeconds)
    {
        this.durationSeconds = durationSeconds;
        return this;
    }

    public BasicSessionCredentials get()
    {
        AWSCredentials baseCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

        List<Statement> statements = new ArrayList<>();
        acceptableUris.forEach(acceptableUri -> {
                    Mode mode = acceptableUri.mode;
                    String uri = acceptableUri.uri;
                    if (uri.startsWith(URI_S3_PREFIX)) {
                        String s3BucketAndKeyStr = uri.substring(URI_S3_PREFIX.length());
                        String[] s3BucketAndKey = s3BucketAndKeyStr.split("/", 2);
                        statements.add(new Statement(Statement.Effect.Allow)
                                .withActions(S3Actions.ListObjects)
                                .withResources(new Resource("arn:aws:s3:::" + s3BucketAndKey[0])));
                        switch (mode) {
                            case READ:
                                statements.add(new Statement(Statement.Effect.Allow)
                                        .withActions(S3Actions.GetObject)
                                        .withResources(new Resource("arn:aws:s3:::" + s3BucketAndKeyStr + "*")));
                                break;
                            case WRITE:
                                statements.add(new Statement(Statement.Effect.Allow)
                                        .withActions(S3Actions.PutObject)
                                        .withResources(new Resource("arn:aws:s3:::" + s3BucketAndKeyStr + "*")));
                                break;
                        }
                    }
                    else if (uri.startsWith(URI_DYNAMODB_PREFIX)) {
                        String table = uri.substring(URI_DYNAMODB_PREFIX.length());
                        statements.add(new Statement(Statement.Effect.Allow)
                                .withActions(DynamoDBv2Actions.DescribeTable)
                                .withResources(new Resource(String.format("arn:aws:dynamodb:*:*:table/%s", table))));
                        switch (mode) {
                            case READ:
                                statements.add(new Statement(Statement.Effect.Allow)
                                        .withActions(DynamoDBv2Actions.Scan)
                                        .withResources(new Resource(String.format("arn:aws:dynamodb:*:*:table/%s", table))));
                                break;
                            case WRITE:
                                break;
                        }
                    }
                    else if (uri.startsWith(URI_EMR_PREFIX)) {
                        String cluster = uri.substring(URI_EMR_PREFIX.length());
                        // TODO: Grant minimum actions
                        statements.add(new Statement(Statement.Effect.Allow)
                                        .withActions(ElasticMapReduceActions.AllElasticMapReduceActions)
                                        .withResources(new Resource(String.format("arn:aws:elasticmapreduce:*:*:cluster/%s", cluster))));
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected `uri`. uri=" + uri);
                    }
                }
        );
        Policy policy = new Policy();
        policy.setStatements(statements);

        Credentials credentials;

        if (roleArn != null && !roleArn.isEmpty()) {
            // use STS to assume role
            AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(baseCredentials);
            AssumeRoleResult assumeResult = stsClient.assumeRole(new AssumeRoleRequest()
                    .withRoleArn(roleArn)
                    .withDurationSeconds(durationSeconds)
                    .withRoleSessionName(sessionName)
                    .withPolicy(policy.toJson()));

            credentials = assumeResult.getCredentials();
        }
        else {
            // Maybe we'd better add an option command later like `without_federated_token`
            AWSSecurityTokenServiceClient stsClient =
                    new AWSSecurityTokenServiceClient(baseCredentials);

            GetFederationTokenRequest federationTokenRequest = new GetFederationTokenRequest()
                    .withDurationSeconds(durationSeconds)
                    .withName(sessionName)
                    .withPolicy(policy.toJson());

            GetFederationTokenResult federationTokenResult =
                    stsClient.getFederationToken(federationTokenRequest);

            credentials = federationTokenResult.getCredentials();
        }

        return new BasicSessionCredentials(
                credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(),
                credentials.getSessionToken());
    }
}
