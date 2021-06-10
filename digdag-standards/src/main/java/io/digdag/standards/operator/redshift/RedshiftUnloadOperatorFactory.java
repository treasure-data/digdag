package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.aws.AWSSessionCredentialsFactory.AcceptableUri;
import io.digdag.standards.operator.aws.AWSSessionCredentialsFactory.Mode;

import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RedshiftUnloadOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "redshift_unload";
    private final Config systemConfig;
    private final TemplateEngine templateEngine;

    @Inject
    public RedshiftUnloadOperatorFactory(Config systemConfig, TemplateEngine templateEngine)
    {
        this.systemConfig = systemConfig;
        this.templateEngine = templateEngine;
    }

    @Override
    public String getType()
    {
        return OPERATOR_TYPE;
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new RedshiftUnloadOperator(systemConfig, context, templateEngine);
    }

    @VisibleForTesting
    static class RedshiftUnloadOperator
        extends BaseRedshiftLoadOperator<RedshiftConnection.UnloadConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        @VisibleForTesting
        RedshiftUnloadOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine)
        {
            super(systemConfig, context, templateEngine);
        }

        @Override
        protected String type()
        {
            return OPERATOR_TYPE;
        }

        @Override
        protected List<String> nestedConfigKeys()
        {
            return ImmutableList.of("redshift_unload", "redshift");
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig()
        {
            return context.getSecrets().getSecrets("aws.redshift");
        }

        @Override
        protected List<SecretProvider> additionalSecretProvidersForCredentials(SecretProvider awsSecrets)
        {
            return ImmutableList.of(awsSecrets.getSecrets("redshift_unload"));
        }

        @Override
        protected List<AcceptableUri> buildAcceptableUriForSessionCredentials(Config config, AWSCredentials baseCredential)
        {
            String to = config.get("to", String.class);

            ImmutableList.Builder<AcceptableUri> builder = ImmutableList.builder();
            builder.add(new AcceptableUri(Mode.WRITE, to));

            return builder.build();
        }

        @Override
        protected RedshiftConnection.UnloadConfig createStatementConfig(Config params, AWSSessionCredentials sessionCredentials, String queryId)
        {
            return createUnloadConfig(params, sessionCredentials, queryId);
        }

        @Override
        protected String buildSQLStatement(RedshiftConnection connection, RedshiftConnection.UnloadConfig statementConfig, boolean maskCredential)
        {
            return connection.buildUnloadStatement(statementConfig, maskCredential);
        }

        @Override
        protected void beforeConnect(AWSCredentials credentials, RedshiftConnection.UnloadConfig unloadConfig)
        {
            clearDest(credentials, unloadConfig);
        }

        @VisibleForTesting
        RedshiftConnection.UnloadConfig createUnloadConfig(Config config, AWSSessionCredentials sessionCredentials, String queryId)
        {
            RedshiftConnection.UnloadConfig uc = new RedshiftConnection.UnloadConfig();
            uc.configure(
                    unloadConfig -> {
                        unloadConfig.accessKeyId = sessionCredentials.getAWSAccessKeyId();
                        unloadConfig.secretAccessKey = sessionCredentials.getAWSSecretKey();
                        if (sessionCredentials.getSessionToken() != null) {
                            unloadConfig.sessionToken = Optional.of(sessionCredentials.getSessionToken());
                        }

                        unloadConfig.query = config.get("query", String.class);
                        unloadConfig.to = config.get("to", String.class);
                        unloadConfig.manifest = config.getOptional("manifest", Boolean.class);
                        unloadConfig.encrypted = config.getOptional("encrypted", Boolean.class);
                        unloadConfig.allowoverwrite = config.getOptional("allowoverwrite", Boolean.class);
                        unloadConfig.delimiter = config.getOptional("delimiter", String.class);
                        unloadConfig.fixedwidth = config.getOptional("fixedwidth", String.class);
                        unloadConfig.gzip = config.getOptional("gzip", Boolean.class);
                        unloadConfig.bzip2 = config.getOptional("bzip2", Boolean.class);
                        unloadConfig.nullAs = config.getOptional("null_as", String.class);
                        unloadConfig.escape = config.getOptional("escape", Boolean.class);
                        unloadConfig.addquotes = config.getOptional("addquotes", Boolean.class);
                        unloadConfig.parallel = config.getOptional("parallel", String.class);

                        unloadConfig.setupWithPrefixDir(queryId);
                    }
            );
            return uc;
        }

        private void clearDest(AWSCredentials credentials, RedshiftConnection.UnloadConfig unloadConfig)
        {
            try {
                RetryExecutor.retryExecutor() .run(() -> {
                    AmazonS3Client s3Client = new AmazonS3Client(credentials);
                    ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(unloadConfig.s3Bucket).withPrefix(unloadConfig.s3Prefix);
                    ListObjectsV2Result result;
                    // This operation shouldn't be skipped since remaining files created by other operation can cause duplicated data
                    do {
                        result = s3Client.listObjectsV2(req);

                        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                            s3Client.deleteObject(unloadConfig.s3Bucket, objectSummary.getKey());
                        }
                        req.setContinuationToken(result.getNextContinuationToken());
                    } while (result.isTruncated());
                });
            }
            catch (RetryExecutor.RetryGiveupException e) {
                throw ThrowablesUtil.propagate(e);
            }
        }
    }
}
