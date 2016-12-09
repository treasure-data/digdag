package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.client.config.Config;
import io.digdag.core.DigdagEmbed;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretAccessList;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class OperatorSecretFilterTest
{
    static class SecretAccessOperatorFactory
            implements OperatorFactory
    {
        @Inject
        public SecretAccessOperatorFactory()
        { }

        @Override
        public String getType()
        {
            return "secret_access";
        }

        @Override
        public SecretAccessList getSecretAccessList()
        {
            return () -> ImmutableSet.of("statically_declared_key");
        }

        @Override
        public Operator newOperator(OperatorContext context)
        {
            return new SecretAccessOperator(context);
        }
    }

    static class SecretAccessOperator
            implements Operator
    {
        private final OperatorContext context;

        public SecretAccessOperator(OperatorContext context)
        {
            this.context = context;
        }

        @Override
        public boolean testUserSecretAccess(String key)
        {
            return key.equals("user_key");
        }

        @Override
        public TaskResult run()
        {
            String get = context.getTaskRequest().getConfig().get("get", String.class);
            String got = context.getSecrets().getSecret(get);
            return TaskResult.defaultBuilder(context.getTaskRequest())
                .storeParams(newConfig().set("got", got))
                .build();
        }
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private DigdagEmbed embed;
    private OperatorManager operatorManager;

    @Before
    public void setUp()
            throws Exception
    {
        this.embed = setupEmbed((bootstrap) -> bootstrap.addModules((binder) -> {
                    Multibinder.newSetBinder(binder, OperatorFactory.class)
                        .addBinding().to(SecretAccessOperatorFactory.class).in(Scopes.SINGLETON);
                })
                .overrideModulesWith((binder) -> {
                    SecretStore secrets = (projectId, scope, key) -> Optional.of(key + ".value");
                    binder.bind(SecretStoreManager.class).toInstance(siteId -> secrets);
                })
            );
        this.operatorManager = embed.getInjector().getInstance(OperatorManager.class);
    }

    @After
    public void shutdown()
            throws Exception
    {
        embed.close();
    }

    @Test
    public void verifyPredeclaredAccessAllowed()
    {
        Config config = newConfig().set("get", "statically_declared_key");
        Config stored = run("secret_access", config);
        assertThat(stored.get("got", String.class), is("statically_declared_key.value"));
    }

    @Test
    public void verifyNonPredeclaredAccessRejected()
    {
        exception.expect(SecretAccessFilteredException.class);
        exception.expectMessage(containsString("kkk"));

        Config config = newConfig().set("get", "kkk");
        run("secret_access", config);
    }

    @Test
    public void verifyUserGrantedAccessAllowed()
    {
        Config config = newConfig().set("get", "user_key").set("_secret", newConfig().set("user_key", true));
        Config stored = run("secret_access", config);
        assertThat(stored.get("got", String.class), is("user_key.value"));
    }

    @Test
    public void verifyUserGrantedButOperatorFilteredAccessRejected()
    {
        exception.expect(SecretAccessFilteredException.class);
        exception.expectMessage(containsString("kkk"));

        Config config = newConfig().set("get", "kkk").set("_secret", newConfig().set("kkk", true));
        run("secret_access", config);
    }

    private Config run(String operatorType, Config config)
    {
        TaskResult result = operatorManager.callExecutor(
                Paths.get(""),
                operatorType,
                newTaskRequest().withConfig(config));
        return result.getStoreParams();
    }
}
