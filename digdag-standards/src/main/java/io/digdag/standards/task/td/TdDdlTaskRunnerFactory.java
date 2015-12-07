package io.digdag.standards.task.td;

import java.util.List;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskRunner;
import io.digdag.spi.TaskRunnerFactory;
import io.digdag.standards.task.BaseTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigException;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;

public class TdDdlTaskRunnerFactory
        implements TaskRunnerFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdDdlTaskRunnerFactory.class);

    @Inject
    public TdDdlTaskRunnerFactory()
    { }

    public String getType()
    {
        return "td_ddl";
    }

    @Override
    public TaskRunner newTaskExecutor(TaskRequest request)
    {
        return new TdDdlTaskRunner(request);
    }

    private class TdDdlTaskRunner
            extends BaseTaskRunner
    {
        public TdDdlTaskRunner(TaskRequest request)
        {
            super(request);
        }

        @Override
        public Config runTask()
        {
            Config config = request.getConfig();

            List<String> deleteList = config.getListOrEmpty("drop_table", String.class);
            List<String> createList = config.getListOrEmpty("create_table", String.class);
            List<String> emptyList = config.getListOrEmpty("empty_table", String.class);

            try (TDOperation op = TDOperation.fromConfig(config)) {
                for (String t : Iterables.concat(deleteList, emptyList)) {
                    logger.info("Deleting TD table {}.{}", op.getDatabase(), t);
                    op.ensureTableDeleted(t);
                }
                for (String t : Iterables.concat(createList, emptyList)) {
                    logger.info("Creating TD table {}.{}", op.getDatabase(), t);
                    op.ensureTableDeleted(t);
                }
            }
            return request.getConfig().getFactory().create();
        }
    }
}
