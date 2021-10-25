package io.digdag.core.acroute;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AccountRouting;
import io.digdag.spi.AccountRouting.ModuleType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DefaultAccountRoutingFactoryTest
{
    private final ConfigFactory configFactory = new ConfigFactory(new ObjectMapper());

    @Test
    public void newAccountRoutingTest1()
    {
        Config systemConfig = configFactory.create()
                .set("executor.account_routing.enabled", "true")
                .set("executor.account_routing.include", "1,2,3,4")
                .set("agent.account_routing.enabled", "true")
                .set("agent.account_routing.exclude", "11,12,13,14");
        {
            AccountRouting ac = DefaultAccountRoutingFactory.fromConfig(systemConfig, Optional.of(ModuleType.EXECUTOR.toString()));
            assertTrue("Account routing must be enabled", ac.enabled());
            assertEquals("site_id in (1,2,3,4)", ac.getFilterSQL());
        }

        {
            AccountRouting ac = DefaultAccountRoutingFactory.fromConfig(systemConfig, Optional.of(ModuleType.AGENT.toString()));
            assertTrue("Account routing must be enabled", ac.enabled());
            assertEquals("site_id not in (11,12,13,14)", ac.getFilterSQL());
        }

        {
            AccountRouting ac = DefaultAccountRoutingFactory.fromConfig(systemConfig, Optional.of(ModuleType.SCHEDULER.toString()));
            assertFalse("Account routing must be disabled", ac.enabled());
            assertFalse("Filter SQL must be empty", ac.getFilterSQLOpt().isPresent());
        }

    }

    @Test
    public void configValidation()
    {
        {
            // Either 'include' or 'exclude' must be set
            Config systemConfig = configFactory.create()
                    .set("agent.account_routing.enabled", "true");
            assertThrows(ConfigException.class, ()-> DefaultAccountRoutingFactory.fromConfig(systemConfig, Optional.of(ModuleType.AGENT.toString())));
        }
        {
            // Set both 'include' and 'exclude' are not allowed
            Config systemConfig = configFactory.create()
                    .set("agent.account_routing.enabled", "true")
                    .set("agent.account_routing.include", "1,2,3")
                    .set("agent.account_routing.exclude", "11,12,13");
            assertThrows(ConfigException.class, ()-> DefaultAccountRoutingFactory.fromConfig(systemConfig, Optional.of(ModuleType.AGENT.toString())));
        }
    }
}
