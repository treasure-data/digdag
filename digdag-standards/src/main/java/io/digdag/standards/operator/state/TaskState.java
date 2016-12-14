package io.digdag.standards.operator.state;

import com.google.common.base.Supplier;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public interface TaskState
{
    /**
     * The raw root task state {@link Config}. The {@link Config} returned by {@link #params()} is some subtree of this root state.
     * This root state should be used when throwing a polling {@link io.digdag.spi.TaskExecutionException}.
     */
    Config root();

    /**
     * Get the raw params {@link Config} for this {@link TaskState}. This should be used when mutating (nested) task state but <b>not</b> when
     * throwing a polling {@link io.digdag.spi.TaskExecutionException}.
     */
    Config params();

    /**
     * Get a nested {@link TaskState}.
     */
    default TaskState nestedState(String key)
    {
        return ImmutableTaskState.builder()
                .root(root())
                .params(params().getNestedOrSetEmpty(key))
                .build();
    }

    /**
     * Create a new {@link TaskState} based on the raw {@link Config} root task state.
     */
    static TaskState of(Config root)
    {
        return ImmutableTaskState.builder()
                .root(root)
                .params(root)
                .build();
    }

    /**
     * Create a new {@link TaskState} for a {@link TaskRequest}.
     */
    static TaskState of(TaskRequest request)
    {
        return of(request.getLastStateParams().deepCopy());
    }

    /**
     * Create a new {@link TaskExecutionException} with using the root task state.
     */
    default TaskExecutionException pollingTaskExecutionException(int interval)
    {
        return TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(root()));
    }

    /**
     * Store a constant value (once) in the {@link TaskState}. If the value is not yet present in
     * the {@link TaskState}, a value is retrieved from the supplier, inserted into the state and
     * a polling {@link TaskExecutionException} is thrown to immediately persist it. Note that the
     * supplier might be called more than once in case of system failure.
     *
     * @param key The key of the constant value.
     * @param type The type of the constant value.
     * @param supplier A supplier that will be called to get a value to store in the state if not yet present.
     * @param <T> The type of the constant value.
     * @return The constant value.
     */
    default <T> T constant(String key, Class<T> type, Supplier<T> supplier)
    {
        T value = root().get(key, type, null);
        if (value != null) {
            return value;
        }
        value = supplier.get();
        root().set(key, value);
        throw pollingTaskExecutionException(0);
    }
}
