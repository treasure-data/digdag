package io.digdag.standards.operator.td;

import java.util.List;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.standards.operator.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;

public class TdDdlOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdDdlOperatorFactory.class);

    @Inject
    public TdDdlOperatorFactory()
    { }

    public String getType()
    {
        return "td_ddl";
    }

    @Override
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new TdDdlOperator(archivePath, request);
    }

    private class TdDdlOperator
            extends BaseOperator
    {
        public TdDdlOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().getNestedOrGetEmpty("td")
                .deepCopy()
                .setAll(request.getConfig());

            List<String> deleteList = params.getListOrEmpty("drop_table", String.class);
            List<String> createList = params.getListOrEmpty("create_table", String.class);
            List<String> emptyList = params.getListOrEmpty("empty_table", String.class);

            try (TDOperation op = TDOperation.fromConfig(params)) {
                for (String t : Iterables.concat(deleteList, emptyList)) {
                    logger.info("Deleting TD table {}.{}", op.getDatabase(), t);
                    op.ensureTableDeleted(t);
                }
                for (String t : Iterables.concat(createList, emptyList)) {
                    logger.info("Creating TD table {}.{}", op.getDatabase(), t);
                    op.ensureTableDeleted(t);
                }
            }

            return TaskResult.empty(request.getConfig().getFactory());
        }
    }
}
