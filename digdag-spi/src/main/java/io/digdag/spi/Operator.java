package io.digdag.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Operator
{
    Logger logger = LoggerFactory.getLogger(Operator.class);

    TaskResult run();

    default void cleanup(TaskRequest request)
    {
        logger.debug("cleanup is called: attempt_id={}, task_id={}", request.getAttemptId(), request.getTaskId());
    }
}
