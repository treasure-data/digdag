package io.digdag.standards.operator;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.omg.CORBA.ExceptionList;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.util.ArrayList;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.workflow.WorkflowTestingUtils;
import io.digdag.spi.TaskResult;
import io.digdag.standards.command.MockNonBlockingCommandExecutor;
import io.digdag.standards.operator.ShOperatorFactory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class ShOperatorFactoryTest
{
    private static DigdagEmbed embed;

    @BeforeClass
    public static void createDigdagEmbed()
    {
        embed = WorkflowTestingUtils.setupEmbed();
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ObjectMapper mapper;
    private MockNonBlockingCommandExecutor executor;
    private Path tempPath;

    @Before
    public void createInstance1()
    {
        this.mapper = embed.getInjector().getInstance(ObjectMapper.class);
        this.executor = new MockNonBlockingCommandExecutor(mapper);
        this.tempPath = folder.getRoot().toPath();
        //System.setProperty("os.name","windows");

    }

    @Test
    public void testGetShellAndScriptnameOnWin()
    throws Exception
    {
        System.setProperty("os.name","windows");

        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory operatorFactory = new ShOperatorFactory(executor);
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));


        Method method_isWindowsPlatform = ShOperatorFactory.ShOperator.class.getDeclaredMethod("isWindowsPlatform");
        method_isWindowsPlatform.setAccessible(true);
        assertTrue((boolean)method_isWindowsPlatform.invoke(operator));

        Method method_getDefaultShell = ShOperatorFactory.ShOperator.class.getDeclaredMethod("getDefaultShell");
        method_getDefaultShell.setAccessible(true);
        assertTrue("powershell.exe" == (String)method_getDefaultShell.invoke(operator));

        Method method_getScriptName = ShOperatorFactory.ShOperator.class.getDeclaredMethod("getScriptName");
        method_getScriptName.setAccessible(true);
        assertTrue("runner.ps1" == (String)method_getScriptName.invoke(operator));

        System.clearProperty("os.name");
    }


    @Test
    public void testGetShellAndScriptnameOnLinux()
    throws Exception
    {
        System.setProperty("os.name","linux");

        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory operatorFactory = new ShOperatorFactory(executor);
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));

        Method method = ShOperatorFactory.ShOperator.class.getDeclaredMethod("isWindowsPlatform");
        method.setAccessible(true);
        assertFalse((boolean)method.invoke(operator));

        Method method_getDefaultShell = ShOperatorFactory.ShOperator.class.getDeclaredMethod("getDefaultShell");
        method_getDefaultShell.setAccessible(true);
        assertTrue("/bin/sh" == (String)method_getDefaultShell.invoke(operator));

        Method method_getScriptName = ShOperatorFactory.ShOperator.class.getDeclaredMethod("getScriptName");
        method_getScriptName.setAccessible(true);
        assertTrue("runner.sh" == (String)method_getScriptName.invoke(operator));

        System.clearProperty("os.name");
    }


    @Test
    public void testSupportLegacyPowershellParm()
    throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory operatorFactory = new ShOperatorFactory(executor);
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));

        Method method = ShOperatorFactory.ShOperator.class.getDeclaredMethod("supportLegacyPowershellParm", List.class);
        method.setAccessible(true);

        List<String> legacy_powershell_parm = new ArrayList<String>(){{add("powershell.exe");add("-");}};
        List<String> powershell_parm =  (List)method.invoke(operator, legacy_powershell_parm);
        assertTrue(1 == powershell_parm.size());
        assertTrue("powershell.exe" == powershell_parm.get(0).toLowerCase());
    
    }


}


