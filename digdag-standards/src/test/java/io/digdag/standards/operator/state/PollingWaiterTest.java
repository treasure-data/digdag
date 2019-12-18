package io.digdag.standards.operator.state;

import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.DurationInterval;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;
import static org.hamcrest.MatcherAssert.assertThat;

public class PollingWaiterTest
{
    private static Logger logger = LoggerFactory.getLogger(PollingWaiterTest.class);

    private static final ConfigFactory CF = new ConfigFactory(DigdagClient.objectMapper());
    private static final DurationInterval POLL_INTERVAL = DurationInterval.of(Duration.ofSeconds(5), Duration.ofSeconds(300));

    @Test
    public void testNotimeout()
    {
        Integer expected = 99;
        Integer answer = null;
        List<Optional<Integer>> reterns = Arrays.asList(Optional.absent(), Optional.absent(), Optional.of(expected));
        TaskState state = TaskState.of(CF.create());
        for (Optional<Integer> ret: reterns) {
            logger.debug("state:{}", state);
            try {
                answer = pollingWaiter(state, "EXISTS")
                        .withPollInterval(POLL_INTERVAL)
                        .withWaitMessage("Return value does not exist")
                        .await( pollstate  -> { return ret;} );
            }
            catch (TaskExecutionException te) {
                logger.debug("TaskExecutionException interval:{}", te.getRetryInterval());
                if (te.getRetryInterval().isPresent()) {
                    try {
                        Thread.sleep(te.getRetryInterval().get() * 1000);
                    }
                    catch (InterruptedException ie) {

                    }
                }
            }
        }
        assertThat("Get answer finally", answer != null);
        assertThat("Correct answer ", answer.intValue() == expected.intValue());
    }

    @Test(expected = PollingTimeoutException.class)
    public void testTimeout()
    {
        Integer expected = 99;
        Integer answer = null;
        List<Integer> intervalList = new ArrayList<>();
        List<Optional<Integer>> reterns = Arrays.asList(Optional.absent(), Optional.absent(), Optional.of(expected));
        TaskState state = TaskState.of(CF.create());
        for (Optional<Integer> ret: reterns) {
            logger.debug("state:{}", state);
            try {
                answer = pollingWaiter(state, "EXISTS")
                        .withTimeout(Optional.of(Duration.ofSeconds(10)))
                        .withPollInterval(POLL_INTERVAL)
                        .withWaitMessage("Return value does not exist")
                        .await( pollstate  -> { return ret;} );
            }
            catch (TaskExecutionException te) {
                logger.debug("TaskExecutionException interval:{}", te.getRetryInterval());
                if (te.getRetryInterval().isPresent()) {
                    intervalList.add(te.getRetryInterval().get());
                    try {
                        Thread.sleep(te.getRetryInterval().get() * 1000);
                    }
                    catch (InterruptedException ie) {

                    }
                }
            }
        }
    }

    @Test
    public void testInterval()
    {
        Integer answer = null;
        List<Integer> intervalList = new ArrayList<>();
        TaskState state = TaskState.of(CF.create());
        PollingTimeoutException pollException = null;
        for (int loop = 0; loop < 10 && pollException == null; loop++){
            try {
                answer = pollingWaiter(state, "EXISTS")
                        .withTimeout(Optional.of(Duration.ofSeconds(10)))
                        .withPollInterval(POLL_INTERVAL)
                        .withWaitMessage("Return value does not exist")
                        .await(pollstate -> {
                            return Optional.absent();
                        });
            } catch (TaskExecutionException te) {
                logger.debug("TaskExecutionException interval:{}", te.getRetryInterval());
                if (te.getRetryInterval().isPresent()) {
                    intervalList.add(te.getRetryInterval().get());
                    try {
                        Thread.sleep(te.getRetryInterval().get() * 1000);
                    } catch (InterruptedException ie) {

                    }
                }
            } catch (PollingTimeoutException pe) {
                pollException = pe;
            }
        }
        assertThat( "PollingTimeoutException must be caught", pollException != null);
    }
}
