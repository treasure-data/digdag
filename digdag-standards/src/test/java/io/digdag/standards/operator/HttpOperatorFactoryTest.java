package io.digdag.standards.operator;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static org.junit.Assert.fail;

public class HttpOperatorFactoryTest
{
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(0);

	private Path tempPath;
	private HttpOperatorFactory factory;
	private Config config;

	@Before
	public void createInstance()
	{
		this.tempPath = folder.getRoot().toPath();
		this.factory = newOperatorFactory(HttpOperatorFactory.class);

		Config config = newConfig();
		config.set("_command", "http://localhost:" + wireMockRule.port() + "/api/foobar");
		config.set("timeout", 3);
		config.set("retry", false);

		this.config = config;
	}


	@Test
	public void respondSlowlyThanTimeout() throws IOException {
		stubFor(get("/api/foobar")
				.willReturn(aResponse()
						.withStatus(200)
						.withFixedDelay(5000)));

		Operator op = factory.newOperator(newContext(
				tempPath,
				newTaskRequest().withConfig(config)));
		try {
			op.run();
			fail("should be thrown Exception.");
		} catch (TaskExecutionException ignore) {
		}
	}

	@Test
	public void respondQuicklyThanTimeout() throws IOException {
		stubFor(get("/api/foobar")
				.willReturn(aResponse()
						.withStatus(200)
						.withFixedDelay(1000)));

		Operator op = factory.newOperator(newContext(
				tempPath,
				newTaskRequest().withConfig(config)));
		try {
			op.run();
		} catch (TaskExecutionException ignore) {
			fail("should be success.");
		}
	}
}
