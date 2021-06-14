package io.digdag.standards.operator.state;

import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.DurationInterval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Duration;
import java.util.function.Function;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PollingRetryExecutorTest
{
    private static final String STATE_KEY = "foobar";

    private static final ConfigFactory CF = new ConfigFactory(DigdagClient.objectMapper());

    @Mock Operation<Integer> operation;

    @Test
    public void testRunSuccess()
            throws Exception
    {
        TaskState state = TaskState.of(CF.create());

        Function<TaskState, PollingRetryExecutor> executor =
                s -> pollingRetryExecutor(s, STATE_KEY)
                        .retryIf(MyException.class, MyException::isRetry);

        // Successfully run the operator once
        {
            when(operation.perform(any(TaskState.class))).thenReturn(4711);
            int result = executor.apply(state).run(operation);
            verify(operation, times(1)).perform(any(TaskState.class));
            assertThat(result, is(4711));
        }

        assertThat(state.root().getKeys().size(), is(1));
        assertThat(state.root().has(STATE_KEY), is(true));

        // Successfully run the operator again and verify that the new return value is returned
        {
            when(operation.perform(any(TaskState.class))).thenReturn(4712);
            int result = executor.apply(state).run(operation);
            verify(operation, times(2)).perform(any(TaskState.class));
            assertThat(result, is(4712));
        }

        assertThat(state.root().getKeys().size(), is(1));
        assertThat(state.root().has(STATE_KEY), is(true));
    }

    @Test
    public void testRunOnceRetry()
            throws Exception
    {
        TaskState state = TaskState.of(CF.create());

        DurationInterval retryInterval = DurationInterval.of(
                Duration.ofSeconds(3),
                Duration.ofSeconds(20));

        Function<TaskState, PollingRetryExecutor> executor =
                s -> pollingRetryExecutor(s, STATE_KEY)
                        .withRetryInterval(retryInterval)
                        .retryIf(MyException.class, MyException::isRetry);

        int[] expectedRetryIntervals = {3, 6, 12, 20, 20};

        // Fail a few times
        doThrow(new MyException(true)).when(operation).perform(any(TaskState.class));
        for (int expectedRetryInterval : expectedRetryIntervals) {
            try {
                executor.apply(state).runOnce(Integer.class, operation);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getRetryInterval(), is(Optional.of(expectedRetryInterval)));
            }

            assertThat(state.root().getKeys().size(), is(1));
            assertThat(state.root().has(STATE_KEY), is(true));
        }
        verify(operation, times(5)).perform(any(TaskState.class));

        // And then succeed
        doReturn(4711).when(operation).perform(any(TaskState.class));
        for (int i = 0; i < 3; i++) {
            int result = executor.apply(state).runOnce(Integer.class, operation);
            assertThat(result, is(4711));

            assertThat(state.root().getKeys().size(), is(1));
            assertThat(state.root().has(STATE_KEY), is(true));
        }

        verify(operation, times(6)).perform(any(TaskState.class));
    }

    @Test
    public void testRetryDefault()
            throws Exception
    {
        TaskState state = TaskState.of(CF.create());

        MyException cause = new MyException(false);

        doThrow(cause).when(operation).perform(any(TaskState.class));

        try {
            pollingRetryExecutor(state, STATE_KEY)
                    .runOnce(Integer.class, operation);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval().isPresent(), is(true));
        }
    }

    @Test
    public void testNoRetry()
            throws Exception
    {
        TaskState state = TaskState.of(CF.create());

        // Match exception type but fail condition
        {
            MyException cause = new MyException(false);
            doThrow(cause).when(operation).perform(any(TaskState.class));
            try {
                pollingRetryExecutor(state, STATE_KEY)
                        .retryIf(MyException.class, MyException::isRetry)
                        .runOnce(Integer.class, operation);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getCause(), is(cause));
            }
        }

        // Do not match exception type
        {
            Exception cause = new Exception();
            doThrow(cause).when(operation).perform(any(TaskState.class));
            try {
                pollingRetryExecutor(state, STATE_KEY)
                        .retryIf(MyException.class, MyException::isRetry)
                        .runOnce(Integer.class, operation);
            }
            catch (TaskExecutionException e) {
                assertThat(e.getCause(), is(cause));
            }
        }
    }

    static class MyException
            extends Exception
    {
        private final boolean retry;

        MyException(boolean retry)
        {
            super("retry = " + retry);
            this.retry = retry;
        }

        boolean isRetry()
        {
            return retry;
        }
    }
}