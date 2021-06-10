package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.aws.AWSSessionCredentialsFactory;
import io.digdag.standards.operator.aws.AWSSessionCredentialsFactory.AcceptableUri;
import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class RedshiftLoadOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "redshift_load";
    private final TemplateEngine templateEngine;
    private final Config systemConfig;

    @Inject
    public RedshiftLoadOperatorFactory(Config systemConfig, TemplateEngine templateEngine)
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
        return new RedshiftLoadOperator(systemConfig, context, templateEngine);
    }

    @VisibleForTesting
    static class RedshiftLoadOperator
        extends BaseRedshiftLoadOperator<RedshiftConnection.CopyConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        @VisibleForTesting
        RedshiftLoadOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine)
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
            return ImmutableList.of("redshift_load", "redshift");
        }

        @Override
        protected List<SecretProvider> additionalSecretProvidersForCredentials(SecretProvider awsSecrets)
        {
            return ImmutableList.of(awsSecrets.getSecrets("redshift_load"));
        }

        @Override
        protected List<AcceptableUri> buildAcceptableUriForSessionCredentials(Config config, AWSCredentials baseCredential)
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
                                    throw ThrowablesUtil.propagate(e);
                                }
                                @SuppressWarnings("unchecked")
                                List<Map<String, String>> entries = (List<Map<String, String>>) value.get("entries");
                                entries.forEach(file ->
                                        builder.add(new AcceptableUri(AWSSessionCredentialsFactory.Mode.READ, file.get("url"))));
                            }
                    );
                }
                catch (RetryExecutor.RetryGiveupException e) {
                    throw new TaskExecutionException(
                            "Failed to fetch a manifest file: " + from, e);
                }
            }
            return builder.build();
        }

        @VisibleForTesting
        RedshiftConnection.CopyConfig createCopyConfig(Config config, AWSSessionCredentials sessionCredentials)
        {
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
        protected RedshiftConnection.CopyConfig createStatementConfig(Config params, AWSSessionCredentials sessionCredentials, String queryId)
        {
            return createCopyConfig(params, sessionCredentials);
        }

        @Override
        protected String buildSQLStatement(RedshiftConnection connection, RedshiftConnection.CopyConfig statementConfig, boolean maskCredential)
        {
            return connection.buildCopyStatement(statementConfig, maskCredential);
        }

        @Override
        protected void beforeConnect(AWSCredentials credentials, RedshiftConnection.CopyConfig statemenetConfig)
        {
            // Do nothing
        }
    }
}
