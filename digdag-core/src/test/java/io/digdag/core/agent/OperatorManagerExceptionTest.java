package io.digdag.core.agent;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.workflow.WorkflowTestingUtils;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Map;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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

    private static DigdagEmbed embed;

    private OperatorManager operatorManager;

    @BeforeClass
    public static void shutdown()
            throws Exception
    {
        embed = WorkflowTestingUtils.setupEmbed((bootstrap) -> bootstrap.addModules((binder) -> {
                    Multibinder.newSetBinder(binder, OperatorFactory.class)
                            .addBinding().to(CustomErrorOperatorFactory.class).in(Scopes.SINGLETON);
                })
        );
    }

    @AfterClass
    public static void destroyDigdagEmbed()
            throws Exception
    {
        embed.close();
    }

    @Before
    public void setUp()
            throws Exception
    {
        this.operatorManager = embed.getInjector().getInstance(OperatorManager.class);
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
