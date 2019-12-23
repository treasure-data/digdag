package io.digdag.core.agent;


import io.digdag.client.config.Config;
import io.digdag.spi.TemplateException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.client.config.ConfigUtils.newConfig;

public class ConfigEvalEngineTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private ConfigEvalEngine engine = new ConfigEvalEngine();

    private Config params()
    {
        return newConfig()
            .set("timezone", "UTC");
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertThat(
                engine.eval(loadYamlResource("/io/digdag/core/agent/eval/basic.dig"), params()),
                is(loadYamlResource("/io/digdag/core/agent/eval/basic_expected.dig")));
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertThat(
                engine.eval(loadYamlResource("/io/digdag/core/agent/eval/literal.dig"), params()),
                is(loadYamlResource("/io/digdag/core/agent/eval/literal_expected.dig")));
    }

    @Test
    public void undefinedVariable()
            throws Exception
    {
        exception.expect(TemplateException.class);
        exception.expectMessage(containsString("ReferenceError"));
        exception.expectMessage(containsString("no_such_var"));
        engine.eval(newConfig().set("key", "${no_such_var}"), params());
    }

    @Test
    public void undefinedField()
            throws Exception
    {
        assertThat(
            engine.eval(newConfig().set("key", "${timezone.no_such_field}"), params())
            .get("key", String.class),
            is(""));
    }

    @Test
    public void undefinedFieldAccess()
            throws Exception
    {
        exception.expect(TemplateException.class);
        exception.expectMessage(containsString("TypeError"));
        exception.expectMessage(containsString("invalid_access"));
        engine.eval(newConfig().set("key", "${timezone.no_such_field.invalid_access}"), params());
    }

    @Test
    public void testMoment()
            throws Exception
    {
        assertThat(
                engine.eval(loadYamlResource("/io/digdag/core/agent/eval/moment.dig"), params()),
                is(loadYamlResource("/io/digdag/core/agent/eval/moment_expected.dig")));
    }

    @Test
    public void testMomentTimeZone()
            throws Exception
    {
        assertThat(
                engine.eval(loadYamlResource("/io/digdag/core/agent/eval/moment.dig"), params().set("timezone", "Asia/Tokyo")),
                is(loadYamlResource("/io/digdag/core/agent/eval/moment_expected_jst.dig")));
    }

    @Test
    public void testMomentDstTimeZone()
            throws Exception
    {
        assertThat(
                engine.eval(loadYamlResource("/io/digdag/core/agent/eval/moment.dig"), params().set("timezone", "America/Los_Angeles")),
                is(loadYamlResource("/io/digdag/core/agent/eval/moment_expected_pst_pdt.dig")));
    }

    @Test
    public void momentNowIsAvailable()
            throws Exception
    {
        assertThat(
                engine.eval(newConfig().set("key", "${moment().format()}"), params()).get("key", String.class),
                not(is("")));
    }

    @Test
    public void testRequireInvokdetemplate()
    {
        assertThat(engine.requireInvokedTemplate(""), is(false));
        assertThat(engine.requireInvokedTemplate("\n"), is(false));
        assertThat(engine.requireInvokedTemplate("Digdag Notification\naa${hoge}bb"), is(true));
    }
}
