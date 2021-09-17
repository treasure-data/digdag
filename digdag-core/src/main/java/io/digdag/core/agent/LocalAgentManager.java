package io.digdag.core.agent;

import java.util.function.Supplier;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.BackgroundExecutor;
import io.digdag.core.ErrorReporter;
import io.digdag.core.database.TransactionManager;
import io.digdag.spi.AccountRouting;
import io.digdag.spi.AccountRoutingFactory;
import io.digdag.spi.metrics.DigdagMetrics;

public class LocalAgentManager
        implements BackgroundExecutor
{
    private final Supplier<MultiThreadAgent> agentFactory;
    private volatile Thread thread;
    private volatile MultiThreadAgent agent;

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    private DigdagMetrics metrics;

    @Inject
    public LocalAgentManager(
            AgentConfig config,
            AgentId agentId,
            TaskServerApi taskServer,
            OperatorManager operatorManager,
            TransactionManager transactionManager,
            AccountRoutingFactory acrouteFactory)
    {
        if (config.getEnabled()) {
            this.agentFactory =
                    () -> new MultiThreadAgent(config, agentId, taskServer, operatorManager, transactionManager, errorReporter, metrics, acrouteFactory);
        }
        else {
            this.agentFactory = null;
        }
    }

    @PostConstruct
    public synchronized void start()
    {
        if (agentFactory != null && thread == null) {
            agent = agentFactory.get();
            Thread thread = new ThreadFactoryBuilder()
                .setDaemon(false)  // tasks taken from the queue should be certainly processed or callbacked to the server
                .setNameFormat("local-agent-%d")
                .build()
                .newThread(agent);
            thread.start();
            this.thread = thread;
        }
    }

    @PreDestroy
    public synchronized void shutdown()
        throws InterruptedException
    {
        if (thread != null) {
            agent.shutdown(Optional.absent());  // TODO should this value configurable? or should it be always forever and wait until thread interruption?
            thread.join();
            thread = null;
        }
    }

    @Override
    public void eagerShutdown()
            throws InterruptedException
    {
        shutdown();
    }
}
