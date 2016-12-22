package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.AWSSessionCredentialsFactory;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.standards.operator.jdbc.NoTransactionHelper;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.util.DurationParam;
import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static io.digdag.standards.operator.AWSSessionCredentialsFactory.*;

public class RedshiftUnloadOperatorFactory
        implements OperatorFactory
{
    private static final String POLL_INTERVAL = "pollInterval";
    private static final int INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 1200;

    private static final String OPERATOR_TYPE = "redshift_unload";
    private final TemplateEngine templateEngine;

    private static final String QUERY_ID = "queryId";

    @Inject
    public RedshiftUnloadOperatorFactory(TemplateEngine templateEngine)
    {
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
        return new RedshiftUnloadOperator(context, templateEngine);
    }

    @VisibleForTesting
    static class RedshiftUnloadOperator
        extends AbstractJdbcJobOperator<RedshiftConnectionConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        @VisibleForTesting
        RedshiftUnloadOperator(OperatorContext context, TemplateEngine templateEngine)
        {
            super(context, templateEngine);
        }

        @Override
        protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return RedshiftConnectionConfig.configure(secrets, params);
        }

        @Override
        protected RedshiftConnection connect(RedshiftConnectionConfig connectionConfig)
        {
            return RedshiftConnection.open(connectionConfig);
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

        @VisibleForTesting
        AWSCredentials createBaseCredential(SecretProvider secretProvider)
        {
            SecretProvider awsSecrets = secretProvider.getSecrets("aws");
            SecretProvider redshiftSecrets = awsSecrets.getSecrets("redshift");
            SecretProvider redshiftUnloadSecrets = awsSecrets.getSecrets("redshift_unload");

            String keyOfAccess = "access-key-id";
            String accessKeyId =
                    redshiftUnloadSecrets.getSecretOptional(keyOfAccess)
                    .or(redshiftSecrets.getSecretOptional(keyOfAccess))
                    .or(() -> awsSecrets.getSecret(keyOfAccess));

            String keyOfSecret = "secret-access-key";
            String secretAccessKey =
                    redshiftUnloadSecrets.getSecretOptional(keyOfSecret)
                    .or(redshiftSecrets.getSecretOptional(keyOfSecret))
                    .or(() -> awsSecrets.getSecret(keyOfSecret));

            return new BasicAWSCredentials(accessKeyId, secretAccessKey);
        }

        private AWSSessionCredentials createSessionCredentials(Config config, AWSCredentials baseCredential)
        {
            String from = config.get("to", String.class);
            Optional<Boolean> manifest = config.getOptional("manifest", Boolean.class);

            ImmutableList.Builder<AWSSessionCredentialsFactory.AcceptableUri> builder = ImmutableList.builder();
            builder.add(new AcceptableUri(Mode.WRITE, from));
            if (manifest.or(false)) {
                String head = "s3://";
                if (!from.startsWith(head)) {
                    throw new ConfigException("Invalid manifest file uri: " + from);
                }
                AmazonS3Client s3Client = new AmazonS3Client(baseCredential);
                String[] bucketAndKey = from.substring(head.length()).split("/", 2);
                if (bucketAndKey.length < 2 || bucketAndKey[1].isEmpty())
                    throw new ConfigException("Invalid manifest file uri: " + from);
                try {
                    RetryExecutor.retryExecutor().run(() -> {
                                Map<String, Object> value = null;
                                try (InputStream in = s3Client.getObject(bucketAndKey[0], bucketAndKey[1]).getObjectContent()) {
                                    ObjectMapper objectMapper = new ObjectMapper();
                                    value = objectMapper.readValue(in, new TypeReference<Map<String, Object>>() {});
                                }
                                catch (IOException e) {
                                    Throwables.propagate(e);
                                }
                                List<Map<String, String>> entries = (List<Map<String, String>>) value.get("entries");
                                entries.forEach(file ->
                                        builder.add(new AcceptableUri(Mode.WRITE, file.get("url"))));
                            }
                    );
                }
                catch (RetryExecutor.RetryGiveupException e) {
                    throw new TaskExecutionException(
                            "Failed to fetch a manifest file: " + from, buildExceptionErrorConfig(e));
                }
            }

            AWSSessionCredentialsFactory sessionCredentialsFactory =
                    new AWSSessionCredentialsFactory(
                            baseCredential.getAWSAccessKeyId(),
                            baseCredential.getAWSSecretKey(),
                            builder.build());
            return sessionCredentialsFactory.get();
        }

        @VisibleForTesting
        RedshiftConnection.UnloadConfig createUnloadConfig(Config config, AWSCredentials baseCredential)
        {
            AWSSessionCredentials sessionCredentials = createSessionCredentials(config, baseCredential);

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
                    }
            );
            return uc;
        }

        private void clearDest(AWSCredentials credentials, RedshiftConnection.UnloadConfig unloadConfig)
        {
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
        }

        @Override
        protected TaskResult run(Config params, Config state, RedshiftConnectionConfig connectionConfig)
        {
            boolean strictTransaction = strictTransaction(params);

            String statusTableName;
            DurationParam statusTableCleanupDuration;
            if (strictTransaction) {
                statusTableName = params.get("status_table", String.class, "__digdag_status");
                statusTableCleanupDuration = params.get("status_table_cleanup", DurationParam.class,
                        DurationParam.of(Duration.ofHours(24)));
            }
            else {
                statusTableName = null;
                statusTableCleanupDuration = null;
            }

            UUID queryId;
            // generate query id
            if (!state.has(QUERY_ID)) {
                // this is the first execution of this task
                logger.debug("Generating query id for a new {} task", type());
                queryId = UUID.randomUUID();
                state.set(QUERY_ID, queryId);
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
            queryId = state.get(QUERY_ID, UUID.class);

            AWSCredentials baseCredentials = createBaseCredential(context.getSecrets());
            RedshiftConnection.UnloadConfig unloadConfig = createUnloadConfig(params, baseCredentials);
            unloadConfig.setupWithPrefixDir(queryId.toString());

            try {
                RetryExecutor.retryExecutor() .run(() -> clearDest(baseCredentials, unloadConfig));
            }
            catch (RetryExecutor.RetryGiveupException e) {
                Throwables.propagate(e);
            }

            try (RedshiftConnection connection = connect(connectionConfig)) {
                String query = connection.buildUnloadStatement(unloadConfig);

                Exception statementError = connection.validateStatement(query);
                if (statementError != null) {
                    unloadConfig.accessKeyId = "********";
                    unloadConfig.secretAccessKey = "********";
                    String queryForLogging = connection.buildUnloadStatement(unloadConfig);
                    throw new ConfigException("Given query is invalid: " + queryForLogging, statementError);
                }

                TransactionHelper txHelper;
                if (strictTransaction) {
                    txHelper = connection.getStrictTransactionHelper(statusTableName,
                            statusTableCleanupDuration.getDuration());
                }
                else {
                    txHelper = new NoTransactionHelper();
                }

                txHelper.prepare(queryId);

                boolean executed = txHelper.lockedTransaction(queryId, () -> {
                    connection.executeUpdate(query);
                });

                if (!executed) {
                    logger.debug("Query is already completed according to status table. Skipping statement execution.");
                }

                try {
                    txHelper.cleanup();
                }
                catch (Exception ex) {
                    logger.warn("Error during cleaning up status table. Ignoring.", ex);
                }

                return TaskResult.defaultBuilder(request).build();
            }
            catch (LockConflictException ex) {
                int pollingInterval = state.get(POLL_INTERVAL, Integer.class, INITIAL_POLL_INTERVAL);
                // Set next interval for exponential backoff
                state.set(POLL_INTERVAL, Math.min(pollingInterval * 2, MAX_POLL_INTERVAL));
                throw TaskExecutionException.ofNextPolling(pollingInterval, ConfigElement.copyOf(state));
            }
            catch (DatabaseException ex) {
                // expected error that should suppress stacktrace by default
                String message = String.format("%s [%s]", ex.getMessage(), ex.getCause().getMessage());
                throw new TaskExecutionException(message, buildExceptionErrorConfig(ex));
            }
        }
    }
}
