package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
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
import io.digdag.spi.TaskExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import io.digdag.client.config.ConfigUtils;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class OperatorManagerExceptionTest
{
    static class CustomErrorOperatorFactory
            implements OperatorFactory
    {
        @Inject
        public CustomErrorOperatorFactory()
        { }

        @Override
        public String getType()
        {
            return "custom_error";
        }

        @Override
        public Operator newOperator(OperatorContext context)
        {
            return new CustomErrorOperator(context);
        }
    }

    static class CustomErrorOperator
            implements Operator
    {
        private final OperatorContext context;

        public CustomErrorOperator(OperatorContext context)
        {
            this.context = context;
        }

        @Override
        public TaskResult run()
        {
            throw EXCEPTIONS.get(context.getTaskRequest().getConfig().get("_command", String.class));
        }
    }

    private static class CustomRuntimeException extends RuntimeException
    {
        CustomRuntimeException(String message)
        {
            super(message);
        }

        CustomRuntimeException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    private final static Map<String, RuntimeException> EXCEPTIONS = ImmutableMap.<String, RuntimeException>builder()
        .put("runtime", new RuntimeException("foobar"))
        .put("custom", new CustomRuntimeException("foobar"))
        .put("custom_nested", new CustomRuntimeException(null, new RuntimeException("nested")))
        .put("wrapped_runtime", new TaskExecutionException(new RuntimeException("foobar")))
        .put("wrapped_custom", new TaskExecutionException(new CustomRuntimeException("foobar")))
        .put("wrapped_null_message", new TaskExecutionException(new CustomRuntimeException(null, new RuntimeException("cause"))))
        .put("wrapped_custom_message", new TaskExecutionException("custom!!", new RuntimeException("foo")))
        .build();

    private DigdagEmbed embed;
    private OperatorManager operatorManager;

    @Before
    public void setUp()
            throws Exception
    {
        this.embed = setupEmbed((bootstrap) -> bootstrap.addModules((binder) -> {
                    Multibinder.newSetBinder(binder, OperatorFactory.class)
                        .addBinding().to(CustomErrorOperatorFactory.class).in(Scopes.SINGLETON);
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
    public void verifyException()
    {
        expectRuntimeException("runtime", RuntimeException.class, "foobar (runtime)");
        expectRuntimeException("custom", CustomRuntimeException.class, "foobar (custom runtime)");
        expectRuntimeException("custom_nested", CustomRuntimeException.class, "CustomRuntimeException (custom runtime)\n> nested (runtime)");
        expectExecutionException("wrapped_runtime", "foobar (runtime)");
        expectExecutionException("wrapped_custom", "foobar (custom runtime)");
        expectExecutionException("wrapped_null_message", "cause (custom runtime)");
        expectExecutionException("wrapped_custom_message", "custom!!");
    }

    private void expectExecutionException(String name, String expectedMessage)
    {
        Config config = newConfig().set("_command", name);
        try {
            operatorManager.callExecutor(Paths.get(""), "custom_error", newTaskRequest().withConfig(config));
            fail("expected TaskExecutionException");
        }
        catch (RuntimeException ex) {
            assertThat(ex, instanceOf(TaskExecutionException.class));
            Config error = ((TaskExecutionException) ex).getError(ConfigUtils.configFactory).or(newConfig());
            assertThat(error.get("message", String.class, null), is(expectedMessage));
        }
    }

    private void expectRuntimeException(String name, Class<?> expectedClass, String expectedMessage)
    {
        Config config = newConfig().set("_command", name);
        try {
            operatorManager.callExecutor(Paths.get(""), "custom_error", newTaskRequest().withConfig(config));
            fail("expected RuntimeException");
        }
        catch (RuntimeException ex) {
            assertThat((Object) ex.getClass(), is((Object) expectedClass));
            assertThat(OperatorManager.formatExceptionMessage(ex), is(expectedMessage));
        }
    }
}
