package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
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
import io.digdag.spi.*;
import io.digdag.standards.operator.AWSSessionCredentialsFactory;
import io.digdag.standards.operator.AWSSessionCredentialsFactory.AcceptableUri;
import io.digdag.standards.operator.jdbc.*;
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

public class RedshiftLoadOperatorFactory
        implements OperatorFactory
{
    private static final String POLL_INTERVAL = "pollInterval";
    private static final int INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 1200;

    private static final String OPERATOR_TYPE = "redshift_load";
    private final TemplateEngine templateEngine;

    private static final String QUERY_ID = "queryId";

    @Inject
    public RedshiftLoadOperatorFactory(TemplateEngine templateEngine)
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
        return new RedshiftLoadOperator(context, templateEngine);
    }

    @VisibleForTesting
    static class RedshiftLoadOperator
        extends AbstractJdbcJobOperator<RedshiftConnectionConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        @VisibleForTesting
        RedshiftLoadOperator(OperatorContext context, TemplateEngine templateEngine)
        {
            super(context, templateEngine);
        }

        /* TODO: This method name should be connectionConfig() or something? */
        @Override
        protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return RedshiftConnectionConfig.configure(secrets, params);
        }

        /* TODO: This method should be in XxxxConnectionConfig ? */
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
            return ImmutableList.of("redshift_load", "redshift");
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig()
        {
            return context.getSecrets().getSecrets("aws.redshift");
        }

        protected AWSCredentials createBaseCredential(SecretProvider secretProvider)
        {
            SecretProvider awsSecrets = secretProvider.getSecrets("aws");
            SecretProvider redshiftSecrets = awsSecrets.getSecrets("redshift");
            SecretProvider redshiftLoadSecrets = awsSecrets.getSecrets("redshift_load");

            String keyOfAccess = "access-key-id";
            String accessKeyId =
                    redshiftLoadSecrets.getSecretOptional(keyOfAccess)
                    .or(redshiftSecrets.getSecretOptional(keyOfAccess))
                    .or(() -> awsSecrets.getSecret(keyOfAccess));

            String keyOfSecret = "secret-access-key";
            String secretAccessKey =
                    redshiftLoadSecrets.getSecretOptional(keyOfSecret)
                    .or(redshiftSecrets.getSecretOptional(keyOfSecret))
                    .or(() -> awsSecrets.getSecret(keyOfSecret));

            return new BasicAWSCredentials(accessKeyId, secretAccessKey);
        }

        private AWSSessionCredentials createSessionCredentials(Config config, AWSCredentials baseCredential)
        {
            String from = config.get("from", String.class);
            Optional<String> json = config.getOptional("json", String.class);
            Optional<String> avro = config.getOptional("avro", String.class);
            Optional<Boolean> manifest = config.getOptional("manifest", Boolean.class);

            ImmutableList.Builder<AcceptableUri> builder = ImmutableList.builder();
            builder.add(new AcceptableUri(AWSSessionCredentialsFactory.Mode.READ, from));
            if (json.isPresent()) {
                String uri = json.get();
                if (!uri.equalsIgnoreCase("auto")) {
                    builder.add(new AcceptableUri(AWSSessionCredentialsFactory.Mode.READ, uri));
                }
            }
            if (avro.isPresent()) {
                String uri = avro.get();
                if (!uri.equalsIgnoreCase("auto")) {
                    builder.add(new AcceptableUri(AWSSessionCredentialsFactory.Mode.READ, uri));
                }
            }
            if (manifest.or(false)) {
                String head = "s3://";
                if (!from.startsWith(head)) {
                    throw new ConfigException("Invalid manifest file uri: " + from);
                }
                AmazonS3Client s3Client = new AmazonS3Client(baseCredential);
                String[] bucketAndKey = from.substring(head.length()).split("/", 2);
                if (bucketAndKey.length < 2 || bucketAndKey[1].isEmpty()) {
                    throw new ConfigException("Invalid manifest file uri: " + from);
                }
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
                                        builder.add(new AcceptableUri(AWSSessionCredentialsFactory.Mode.READ, file.get("url"))));
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
        RedshiftConnection.CopyConfig createCopyConfig(Config config, AWSCredentials baseCredential)
        {
            AWSSessionCredentials sessionCredentials = createSessionCredentials(config, baseCredential);

            RedshiftConnection.CopyConfig cc = new RedshiftConnection.CopyConfig();
            cc.configure(
                    copyConfig -> {
                        copyConfig.accessKeyId = sessionCredentials.getAWSAccessKeyId();
                        copyConfig.secretAccessKey = sessionCredentials.getAWSSecretKey();
                        if (sessionCredentials.getSessionToken() != null) {
                            copyConfig.sessionToken = Optional.of(sessionCredentials.getSessionToken());
                        }

                        copyConfig.table = config.get("table", String.class);
                        copyConfig.columnList = config.getOptional("column_list", String.class);
                        copyConfig.from = config.get("from", String.class);
                        copyConfig.readratio = config.getOptional("readratio", Integer.class);
                        copyConfig.manifest = config.getOptional("manifest", Boolean.class);
                        copyConfig.encrypted = config.getOptional("encrypted", Boolean.class);
                        copyConfig.region = config.getOptional("region", String.class);

                        copyConfig.csv = config.getOptional("csv", String.class);
                        copyConfig.delimiter = config.getOptional("delimiter", String.class);
                        copyConfig.fixedwidth = config.getOptional("fixedwidth", String.class);
                        copyConfig.json = config.getOptional("json", String.class);
                        copyConfig.avro = config.getOptional("avro", String.class);
                        copyConfig.gzip = config.getOptional("gzip", Boolean.class);
                        copyConfig.bzip2 = config.getOptional("bzip2", Boolean.class);
                        copyConfig.lzop = config.getOptional("lzop", Boolean.class);

                        copyConfig.acceptanydate = config.getOptional("acceptanydate", Boolean.class);
                        copyConfig.acceptinvchars = config.getOptional("acceptinvchars", String.class);
                        copyConfig.blanksasnull = config.getOptional("blanksasnull", Boolean.class);
                        copyConfig.dateformat = config.getOptional("dateformat", String.class);
                        copyConfig.emptyasnull = config.getOptional("emptyasnull", Boolean.class);
                        copyConfig.encoding = config.getOptional("encoding", String.class);
                        copyConfig.escape = config.getOptional("escape", Boolean.class);
                        copyConfig.explicitIds = config.getOptional("explicit_ids", Boolean.class);
                        copyConfig.fillrecord = config.getOptional("fillrecord", Boolean.class);
                        copyConfig.ignoreblanklines = config.getOptional("ignoreblanklines", Boolean.class);
                        copyConfig.ignoreheader = config.getOptional("ignoreheader", Integer.class);
                        copyConfig.nullAs = config.getOptional("null_as", String.class);
                        copyConfig.removequotes = config.getOptional("removequotes", Boolean.class);
                        copyConfig.roundec = config.getOptional("roundec", Boolean.class);
                        copyConfig.timeformat = config.getOptional("timeformat", String.class);
                        copyConfig.trimblanks = config.getOptional("trimblanks", Boolean.class);
                        copyConfig.truncatecolumns = config.getOptional("truncatecolumns", Boolean.class);
                        copyConfig.comprows = config.getOptional("comprows", Integer.class);
                        copyConfig.compupdate = config.getOptional("compupdate", String.class);
                        copyConfig.maxerror = config.getOptional("maxerror", Integer.class);
                        copyConfig.noload = config.getOptional("noload", Boolean.class);
                        copyConfig.statupdate = config.getOptional("statupdate", String.class);
                    }
            );
            return cc;
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
            RedshiftConnection.CopyConfig copyConfig = createCopyConfig(params, baseCredentials);

            try (RedshiftConnection connection = connect(connectionConfig)) {
                String query = connection.buildCopyStatement(copyConfig);

                Exception statementError = connection.validateStatement(query);
                if (statementError != null) {
                    copyConfig.accessKeyId = "********";
                    copyConfig.secretAccessKey = "********";
                    String queryForLogging = connection.buildCopyStatement(copyConfig);
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
