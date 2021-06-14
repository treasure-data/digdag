package acceptance;

import avro.shaded.com.google.common.base.Joiner;
import avro.shaded.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class ConfigIT
{

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void verifyPrecedenceWithDefaultConfigFile()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params.dig");

        Path home = folder.newFolder().toPath().resolve("home");
        Path configFile = home.resolve(".config").resolve("digdag").resolve("config");
        Files.createDirectories(configFile.getParent());

        TestUtils.fakeHome(home.toString(), () -> {
            Files.write(configFile, asList(
                    "params.param1=defaultConfigValue1",
                    "params.param2=defaultConfigValue2",
                    "params.param3=defaultConfigValue3",
                    "params.param4=defaultConfigValue4"
            ));
            Map<String, String> env = ImmutableMap.of(
                    "DIGDAG_CONFIG", Joiner.on('\n').join(
                            "params.param2=envConfigValue2",
                            "params.param3=envConfigValue3",
                            "params.param4=envConfigValue4"
                    )
            );
            TestUtils.withSystemProperties(ImmutableMap.of(
                    "params.param3", "systemPropValue3",
                    "params.param4", "systemPropValue4"), () -> {
                main(env,
                        "run",
                        "-o", folder.newFolder().getAbsolutePath(),
                        "--project", projectDir.toString(),
                        "-p", "param4=commandLinePropValue4",
                        "params.dig");
            });
        });
        assertThat(Files.readAllLines(projectDir.resolve("param1.out")), contains("defaultConfigValue1"));
        assertThat(Files.readAllLines(projectDir.resolve("param2.out")), contains("envConfigValue2"));
        assertThat(Files.readAllLines(projectDir.resolve("param3.out")), contains("systemPropValue3"));
        assertThat(Files.readAllLines(projectDir.resolve("param4.out")), contains("commandLinePropValue4"));
    }

    @Test
    public void verifyPrecedenceWithExplicitConfigFile()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params.dig");

        Path home = folder.newFolder().toPath().resolve("home");
        Path defaultConfigFile = home.resolve(".config").resolve("digdag").resolve("config");
        Files.createDirectories(defaultConfigFile.getParent());

        Path explicitConfig = folder.newFolder().toPath().resolve("explicit-config");
        Files.write(explicitConfig, asList(
                "params.param3=explicitConfigValue3",
                "params.param4=explicitConfigValue4"
        ));

        TestUtils.fakeHome(home.toString(), () -> {
            Files.write(defaultConfigFile, asList(
                    "params.param1=defaultConfigValue1",
                    "params.param2=defaultConfigValue2",
                    "params.param3=defaultConfigValue3",
                    "params.param4=defaultConfigValue4"
            ));
            Map<String, String> env = ImmutableMap.of(
                    "DIGDAG_CONFIG", Joiner.on('\n').join(
                            "params.param1=envConfigValue1",
                            "params.param2=envConfigValue2",
                            "params.param3=envConfigValue3",
                            "params.param4=envConfigValue4"
                    )
            );
            TestUtils.withSystemProperties(ImmutableMap.of(
                    "params.param2", "systemPropValue2",
                    "params.param3", "systemPropValue3",
                    "params.param4", "systemPropValue4"), () -> {
                main(env,
                        "run",
                        "-o", folder.newFolder().getAbsolutePath(),
                        "--config", explicitConfig.toString(),
                        "--project", projectDir.toString(),
                        "-p", "param4=commandLinePropValue4",
                        "params.dig");
            });
        });
        assertThat(Files.readAllLines(projectDir.resolve("param1.out")), contains("envConfigValue1"));
        assertThat(Files.readAllLines(projectDir.resolve("param2.out")), contains("systemPropValue2"));
        assertThat(Files.readAllLines(projectDir.resolve("param3.out")), contains("explicitConfigValue3"));
        assertThat(Files.readAllLines(projectDir.resolve("param4.out")), contains("commandLinePropValue4"));
    }
}
