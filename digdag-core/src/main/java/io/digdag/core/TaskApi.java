package io.digdag.core;

import com.google.common.base.*;
import com.google.common.collect.*;

public interface TaskApi
{
    void taskFinished(long taskId,
            ConfigSource stateParams,
            ConfigSource subtaskConfig,
            Optional<ConfigSource> error,
            Optional<Integer> retryInterval,
            Optional<ConfigSource> carryParams,
            Optional<TaskReport> report);
}
