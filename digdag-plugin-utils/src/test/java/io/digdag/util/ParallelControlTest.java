package io.digdag.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class ParallelControlTest
{
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ConfigFactory configFactory = new ConfigFactory(new ObjectMapper());

    private Config newConfig()
    {
        return configFactory.create();
    }

    @Test
    public void testParallelControl()
    {
        {
            ParallelControl pc = ParallelControl.of(newConfig());
            assertThat(pc.isParallel(), is(false));
            assertThat(pc.getParallelLimit(), is(0));
        }
        {
            ParallelControl pc = ParallelControl.of(newConfig().set("_parallel", true));
            assertThat(pc.isParallel(), is(true));
            assertThat(pc.getParallelLimit(), is(0));
        }

        {
            ParallelControl pc = ParallelControl.of(newConfig().set("_parallel", "false"));
            assertThat(pc.isParallel(), is(false));
            assertThat(pc.getParallelLimit(), is(0));
        }

        {
            ParallelControl pc = ParallelControl.of(newConfig().set("_parallel", "true"));
            assertThat(pc.isParallel(), is(true));
            assertThat(pc.getParallelLimit(), is(0));
        }

        {
            // with limit
            Config config = newConfig().setNested("_parallel", newConfig().set("limit", 3));
            ParallelControl pc = ParallelControl.of(config);
            assertThat(pc.isParallel(), is(true));
            assertThat(pc.getParallelLimit(), is(3));
        }

        {
            Config src = newConfig().set("_parallel", false);
            Config dst = newConfig();
            ParallelControl.of(src).copyIfNeeded(dst);
            assertThat(dst.has("_parallel"), is(false));
        }

        {
            Config src = newConfig().set("_parallel", true);
            Config dst = newConfig();
            ParallelControl.of(src).copyIfNeeded(dst);
            assertThat(dst.get("_parallel", boolean.class), is(true));
        }

        {
            Config src = newConfig().set("_parallel", newConfig().set("limit", 3));
            Config dst = newConfig();
            ParallelControl.of(src).copyIfNeeded(dst);
            assertThat(dst.getNested("_parallel").get("limit", int.class), is(3));
        }

    }

    @Test
    public void testParallelInvalidFormat()
    {

        Config test1 = configFactory.create().set("_parallel", "tru");
        assertException(() -> ParallelControl.of(test1), ConfigException.class, "Expected 'true' or 'false' for key '_parallel' but got");

        Config test2 = configFactory.create().setNested("_parallel", newConfig().set("limit", "test"));
        assertException(() -> ParallelControl.of(test2), ConfigException.class, "Expected integer (int) type for key 'limit' but got");
    }

    private void assertException(Runnable func, Class<? extends Exception> expected, String message)
    {
        try {
            func.run();
            fail();
        }
        catch (Exception ex) {
            assertThat(ex, instanceOf(expected));
            assertThat(ex.getMessage(), containsString(message));
        }
    }
}
