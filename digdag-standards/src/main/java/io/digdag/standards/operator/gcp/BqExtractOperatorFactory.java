package io.digdag.standards.operator.gcp;

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;

import static io.digdag.standards.operator.gcp.Bq.tableReference;

class BqExtractOperatorFactory
        implements OperatorFactory
{
    private final BqClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    public BqExtractOperatorFactory(
            BqClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }

    public String getType()
    {
        return "bq_extract";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new BqExtractOperator(context);
    }

    private class BqExtractOperator
            extends BaseBqJobOperator
    {
        BqExtractOperator(OperatorContext context)
        {
            super(context, clientFactory, credentialProvider);
        }

        @Override
        protected JobConfiguration jobConfiguration(String projectId)
        {
            JobConfigurationExtract cfg = new JobConfigurationExtract();

            try {
                cfg.setDestinationUris(params.getList("destination", String.class));
            }
            catch (ConfigException ignore) {
                cfg.setDestinationUri(params.get("destination", String.class));
            }

            Optional<DatasetReference> defaultDataset = params.getOptional("dataset", String.class)
                    .transform(Bq::datasetReference);
            String sourceTable = params.get("_command", String.class);
            cfg.setSourceTable(tableReference(projectId, defaultDataset, sourceTable));

            params.getOptional("print_header", boolean.class).transform(cfg::setPrintHeader);
            params.getOptional("field_delimiter", String.class).transform(cfg::setFieldDelimiter);
            params.getOptional("destination_format", String.class).transform(cfg::setDestinationFormat);
            params.getOptional("compression", String.class).transform(cfg::setCompression);

            return new JobConfiguration()
                    .setExtract(cfg);
        }
    }
}
