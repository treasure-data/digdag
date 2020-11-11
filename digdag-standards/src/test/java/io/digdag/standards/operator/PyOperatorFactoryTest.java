package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import io.digdag.standards.operator.PyOperatorFactory.PyOperator;

public class PyOperatorFactoryTest
{
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private Path tempPath;
	private PyOperatorFactory factory;
	private Config config;
	private ObjectMapper objectMapper = DigdagClient.objectMapper();
	private CommandExecutor exec;
	private OperatorContext operatorContext;
	private TaskRequest taskRequest;

	@Before
	public void setUp()
	{
		tempPath = folder.getRoot().toPath();

		exec = mock(CommandExecutor.class);
		factory = new PyOperatorFactory(exec, objectMapper);
		config = new ConfigFactory(objectMapper).create();
		operatorContext = mock(OperatorContext.class);
		taskRequest = mock(TaskRequest.class);
		doReturn(tempPath).when(operatorContext).getProjectPath();
		doReturn(taskRequest).when(operatorContext).getTaskRequest();
		doReturn(config).when(taskRequest).getConfig();
	}

	@Test
	public void testGetErrorReason() throws IOException
	{
		CommandStatus commandStatus = mock(CommandStatus.class);
		CommandContext commandContext = mock(CommandContext.class);
		PyOperator py = (PyOperator)factory.newOperator(operatorContext);
		String outputJsonContent = Resources.toString(Resources.getResource("io/digdag/standards/operator/py/output.json"), UTF_8);
		Path ioDir = tempPath.resolve("iodir");
		Files.createDirectories(ioDir);
		Files.write(ioDir.resolve("output.json"), outputJsonContent.getBytes(UTF_8));

		doReturn(137).when(commandStatus).getStatusCode();
		doReturn(Optional.of("Test error message")).when(commandStatus).getErrorMessage();
		doReturn(tempPath).when(commandContext).getLocalProjectPath();
		doReturn("iodir").when(commandStatus).getIoDirectory();
		String reason = py.getErrorReason(commandStatus, commandContext);
		System.out.println(reason);
		assertThat(reason, containsString("Error messages from CommandExecutor: Test error message"));
		assertThat(reason, containsString("Error messages from python: name 'printaa' is not defined (NameError)"));
		assertThat(reason, containsString("from NameError: name 'printaa' is not defined"));
	}

}
