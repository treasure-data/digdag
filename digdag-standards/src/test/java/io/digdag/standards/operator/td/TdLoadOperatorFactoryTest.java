package io.digdag.standards.operator.td;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.standards.operator.td.TdOperatorTestingUtils.newOperatorFactory;


public class TdLoadOperatorFactoryTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path tempPath;
    private TdLoadOperatorFactory factory;

    @Before
    public void createInstance()
    {
        this.tempPath = folder.getRoot().toPath();
        this.factory = newOperatorFactory(TdLoadOperatorFactory.class);
    }

    private Operator newOperatorWithConfig(Config config)
    {
        return factory.newOperator(newContext(tempPath, newTaskRequest().withConfig(config)));
    }

    @Test
    public void testConfigNoParam()
    {
        expectedException.expect(ConfigException.class);
        expectedException.expectMessage("No parameter is set");
        newOperatorWithConfig(newConfig());
    }

    @Test
    public void testConfigBothConfigAndName()
    {
        expectedException.expect(ConfigException.class);
        expectedException.expectMessage("The parameters config and name cannot both be set");
        newOperatorWithConfig(newConfig().set("config", newConfig().set("aa", 1)).set("name", "aaa"));
    }

    @Test
    public void testConfigBothCommandAndName()
    {
        expectedException.expect(ConfigException.class);
        expectedException.expectMessage("Only the command or one of the config and name params may be set");
        newOperatorWithConfig(newConfig().set("_command", "aaa.yml").set("name", "bbb.yml"));
    }

    @Test
    public void testConfigCommandOnly()
    {
        newOperatorWithConfig(newConfig().set("_command", "aaa"));
    }

}
