package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static io.digdag.standards.operator.td.TdOperatorFactory.buildResetStoreParams;
import static io.digdag.standards.operator.td.TdOperatorFactory.buildStoreParams;
import static io.digdag.standards.operator.td.TdOperatorFactory.downloadJobResult;

public class TdRunOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdRunOperatorFactory.class);
    private final Map<String, String> env;
    private final Config systemConfig;
    private final BaseTDClientFactory clientFactory;

    @Inject
    public TdRunOperatorFactory(@Environment Map<String, String> env, Config systemConfig, BaseTDClientFactory clientFactory)
    {
        this.env = env;
        this.systemConfig = systemConfig;
        this.clientFactory = clientFactory;
    }

    public String getType()
    {
        return "td_run";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new TdRunOperator(context, clientFactory);
    }

    private class TdRunOperator
            extends BaseTdJobOperator
    {
        private final Config params;
        private final JsonNode command;
        private final Instant sessionTime;
        private final Optional<String> downloadFile;
        private final boolean storeLastResults;
        private final boolean preview;

        private TdRunOperator(OperatorContext context, BaseTDClientFactory clientFactory)
        {
            super(context, env, systemConfig, clientFactory);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.command = params.get("_command", JsonNode.class);
            this.sessionTime = params.get("session_time", Instant.class);
            this.downloadFile = params.getOptional("download_file", String.class);
            // TODO store_last_results should be io.digdag.standards.operator.jdbc.StoreLastResultsOption
            // instead of boolean to be consistent with pg> and redshift> operators but not implemented yet.
            this.storeLastResults = params.get("store_last_results", boolean.class, false);
            this.preview = params.get("preview", boolean.class, false);
        }

        @Override
        protected String startJob(TDOperator op, String domainKey)
        {
            switch (command.getNodeType()) {
                case NUMBER:
                    return startById(op, domainKey, command.longValue());
                case STRING:
                    return startByName(op, domainKey, command.textValue());
                default:
                    throw new ConfigException("Invalid saved query reference: " + command);
            }
        }

        private String startById(TDOperator op, String domainKey, long id)
        {
            TDSavedQueryStartRequest req = TDSavedQueryStartRequest.builder()
                    .name("")
                    .id(id)
                    .scheduledTime(Date.from(sessionTime))
                    .domainKey(domainKey)
                    .build();

            String jobId = op.submitNewJobWithRetry(client -> client.startSavedQuery(req));
            logger.info("Started a saved query id={} with time={}, job id={}", id, sessionTime, jobId);
            return jobId;
        }

        private String startByName(TDOperator op, String domainKey, String name)
        {
            TDSavedQueryStartRequest req = TDSavedQueryStartRequest.builder()
                    .name(name)
                    .scheduledTime(Date.from(sessionTime))
                    .domainKey(domainKey)
                    .build();

            String jobId = op.submitNewJobWithRetry(client -> client.startSavedQuery(req));
            logger.info("Started a saved query name={} with time={}, job id={}", name, sessionTime, jobId);
            return jobId;
        }

        @Override
        protected TaskResult processJobResult(TDOperator op, TDJobOperator job)
        {
            downloadJobResult(job, workspace, downloadFile, state, retryInterval);

            if (preview) {
                TdOperatorFactory.downloadPreviewRows(job, "job id " + job.getJobId(), state, retryInterval);
            }

            Config storeParams = buildStoreParams(request.getConfig().getFactory(), job, storeLastResults, state, retryInterval);

            return TaskResult.defaultBuilder(request)
                    .resetStoreParams(buildResetStoreParams(storeLastResults))
                    .storeParams(storeParams)
                    .build();
        }
    }
}
