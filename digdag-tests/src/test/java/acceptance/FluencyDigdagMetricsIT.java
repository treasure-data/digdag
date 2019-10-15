package acceptance;

import com.google.inject.Scopes;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.crypto.SecretCryptoProvider;
import io.digdag.core.database.DatabaseSecretStoreManager;
import io.digdag.server.ac.DefaultAccessController;
import io.digdag.server.metrics.DigdagMetricsConfig;
import io.digdag.server.metrics.DigdagMetricsModule;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.ac.AccessController;
import io.github.yoyama.micrometer.FluencyMeterRegistry;
import io.github.yoyama.micrometer.FluencyRegistryConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.komamitsu.fluency.Fluency;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FluencyDigdagMetricsIT
{
    List<Object[]> emitList = new ArrayList<>();
    Fluency fluency = mock(Fluency.class);
    FluencyRegistryConfig regConfig = FluencyRegistryConfig.apply("testtest", "digdag", Duration.ofSeconds(1), true);

    CommandExecutor cmdExecutor = mock(CommandExecutor.class);
    DigdagMetricsModule metricsModule;

    @Before
    public void setUp() throws IOException
    {
        doAnswer(new Answer<Void>()
        {
            @Override
            public Void answer(InvocationOnMock invocation) {
                emitList.add(invocation.getArguments());
                return null;
            }
        })
        .when(fluency)
        .emit(any(String.class), any(Long.class), any(Map.class));

        FluencyMeterRegistry meter = FluencyMeterRegistry.apply(regConfig, HierarchicalNameMapper.DEFAULT, Clock.SYSTEM, fluency);

        Config c = emptyConfig();
        c.set("metrics.enable", "fluency");
        DigdagMetricsConfig metricsConfig = new DigdagMetricsConfig(c);

        metricsModule = spy(new DigdagMetricsModule(metricsConfig));
        when(metricsModule.createFluencyMeterRegistry(any())).thenReturn(meter);
    }

    Config emptyConfig()
    {
        return ConfigElement.empty().toConfig(new ConfigFactory(DigdagClient.objectMapper()));
    }

    DigdagEmbed setEmbed()
    {
        return new DigdagEmbed.Bootstrap()
                .withExtensionLoader(false)
                .addModules((binder) -> {
                    binder.bind(CommandExecutor.class).toInstance(cmdExecutor);
                    binder.bind(SecretCrypto.class).toProvider(SecretCryptoProvider.class).in(Scopes.SINGLETON);
                    binder.bind(SecretStoreManager.class).to(DatabaseSecretStoreManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessController.class).to(DefaultAccessController.class);
                })
                .withWorkflowExecutor(true)
                .withScheduleExecutor(false)
                .withLocalAgent(true)
                .overrideModulesWith(metricsModule)
                .initialize();
    }

    @Test
    public void sendMetricsToFluency() throws Exception
    {
        try (DigdagEmbed digdag = setEmbed())
        {
            digdag.getLocalSite().runUntilAllDone();
            Thread.sleep(10000);
            assertTrue("Num of sending metrics > 0", emitList.size() > 0);
            assertEquals("tag is testtest", "testtest", emitList.get(0)[0].toString());
        }
    }
}
