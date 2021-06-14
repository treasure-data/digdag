package io.digdag.core.log;

import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.AgentId;
import io.digdag.core.config.PropertyUtils;
import io.digdag.spi.LogFilePrefix;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.digdag.core.log.LocalFileLogServerFactory.LocalFileLogServer.LocalFileDirectTaskLogger;

public class LocalFileLogServerFactoryTest
{
    private static final ConfigFactory CONFIG_FACTORY = new ConfigFactory(DigdagClient.objectMapper());

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    Path taskLogPath;
    Properties props;
    Config systemConfig;
    LocalFileLogServerFactory logServerFactory;
    LocalFileLogServerFactory.LocalFileLogServer localServer;

    LogFilePrefix prefix = LogFilePrefix.builder()
            .createdAt(Instant.now())
            .retryAttemptName(Optional.absent())
            .projectId(1)
            .sessionTime(Instant.now())
            .siteId(1)
            .timeZone(ZoneId.systemDefault())
            .workflowName("test1")
            .build();
    LocalFileDirectTaskLogger taskLogger;

    @Before
    public void setUp() throws IOException
    {
        taskLogPath = tempFolder.newFolder("task_logs").toPath();
    }

    private void setUpTaskLogger(Optional<String> size)
    {
        props = new Properties();
        props.setProperty("log-server.local.path", taskLogPath.toString());
        if (size.isPresent()) {
            props.setProperty("log-server.local.split_size", size.get());
        }
        else {
        }
        systemConfig = PropertyUtils.toConfigElement(props).toConfig(CONFIG_FACTORY);
        System.out.println(systemConfig);
        logServerFactory = new LocalFileLogServerFactory(systemConfig, AgentId.of("agentA"));
        localServer = (LocalFileLogServerFactory.LocalFileLogServer) logServerFactory.getLogServer();
        taskLogger = localServer.newDirectTaskLogger(prefix, "+task1");
    }

    @Test
    public void checkSplitSize()
    {
        setUpTaskLogger(Optional.of("100"));
        String msg = repeatedString("a", 51);
        for (int i = 0; i < 100; i++) {
            taskLogger.log(msg.getBytes(UTF_8), 0, msg.length());
        }
        taskLogger.close();
        for (File f : taskLogPath.toFile().listFiles()) {
            for (File f2: f.listFiles()) {
                File[] taskLogs = f2.listFiles();
                for (File f3: taskLogs) {
                    System.out.println(f3);
                }
                assertThat("log file should be splitted", f2.listFiles().length > 1, is(true));
            }
        }
    }

    @Test
    public void checkSplitSizeIsZero()
    {
        setUpTaskLogger(Optional.of("0"));

        String msg = repeatedString("a", 51);
        for (int i = 0; i < 100; i++) {
            taskLogger.log(msg.getBytes(UTF_8), 0, msg.length());
        }
        taskLogger.close();
        for (File f : taskLogPath.toFile().listFiles()) {
            for (File f2: f.listFiles()) {
                File[] taskLogs = f2.listFiles();
                for (File f3: taskLogs) {
                    System.out.println(f3);
                }
                assertThat("log file should be a file", f2.listFiles().length, is(1));
            }
        }
    }

    @Test
    public void checkNoSplitSize()
    {
        setUpTaskLogger(Optional.absent());

        String msg = repeatedString("a", 51);
        for (int i = 0; i < 100; i++) {
            taskLogger.log(msg.getBytes(UTF_8), 0, msg.length());
        }
        taskLogger.close();
        for (File f : taskLogPath.toFile().listFiles()) {
            for (File f2: f.listFiles()) {
                File[] taskLogs = f2.listFiles();
                for (File f3: taskLogs) {
                    System.out.println(f3);
                }
                assertThat("log file should be a file", f2.listFiles().length, is(1));
            }
        }
    }

    private String repeatedString(String v, int num)
    {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < num; i++) {
            b.append(v);
        }
        return b.toString();
    }
}
