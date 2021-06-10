package acceptance;

import io.digdag.commons.guava.ThrowablesUtil;
import org.junit.Rule;
import org.junit.Test;
import io.digdag.core.database.DataSourceProvider;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Statement;

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import static utils.TestUtils.expect;
import static utils.TestUtils.main;

public class ServerJmxIT
{
    public static final Pattern JMX_PORT_PATTERN = Pattern.compile("\\s*JMX agent started on port (\\d+)\\s*");

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .inProcess(false)
            .configuration(
                    "server.jmx.port=0",
                    "metrics.enable=jmx",
                    "database.leakDetectionThreshold=60000",
                    "database.maximumPoolSize=3")
            .build();

    private static JMXConnector connectJmx(TemporaryDigdagServer server)
        throws IOException
    {
        Matcher matcher = JMX_PORT_PATTERN.matcher(server.outUtf8());
        assertThat(matcher.find(), is(true));
        int port = Integer.parseInt(matcher.group(1));

        try {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + port + "/jmxrmi");
            return JMXConnectorFactory.connect(url, null);
        }
        catch (MalformedURLException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    @Test
    public void verifyJmx()
            throws Exception
    {
        try (JMXConnector con = connectJmx(server)) {
            MBeanServerConnection beans = con.getMBeanServerConnection();

            Object uptime = beans.getAttribute(ObjectName.getInstance("java.lang", "type", "Runtime"), "Uptime");
            assertThat(uptime, instanceOf(Long.class));

            Object enqueueCount = beans.getAttribute(ObjectName.getInstance("io.digdag.core.workflow", "name", "TaskQueueDispatcher"), "EnqueueCount");
            assertThat(enqueueCount, is(0L));
        }
    }

    @Test
    public void verifyHikariCP()
            throws Exception
    {
        //This test requires remote database
        assumeThat(server.isRemoteDatabase(), is(true));
        try (JMXConnector con = connectJmx(server)) {
            MBeanServerConnection beans = con.getMBeanServerConnection();

            Object leakDetectionThreshold = beans.getAttribute(ObjectName.getInstance("com.zaxxer.hikari", "type", "PoolConfig (HikariPool-1)"), "LeakDetectionThreshold");
            assertThat(leakDetectionThreshold, is(60000L));

            Object numConnection = beans.getAttribute(ObjectName.getInstance("com.zaxxer.hikari", "type", "Pool (HikariPool-1)"), "TotalConnections");
            assertTrue((int)numConnection >= 0);
        }
    }

    @Test
    public void verifyDigdagMetrics()
            throws Exception
    {
        //To get specific metrics, we need to use it.
        CommandStatus status = main("projects",
                "-e", server.endpoint());
        try (JMXConnector con = connectJmx(server)) {
            MBeanServerConnection beans = con.getMBeanServerConnection();

            //Category AGENT
            Object numMaxAcquire = beans.getAttribute(ObjectName.getInstance("io.digdag.agent", "name", "agent_mtag_NumMaxAcquire"), "Count");
            assertTrue((long)numMaxAcquire >= 0);

            //Category API
            Object getProjectsByName = beans.getAttribute(ObjectName.getInstance("io.digdag.api", "name", "api_getProjects"), "Count");
            assertTrue((long)getProjectsByName >= 0);

            //Category DB
            Object findTasksByState = beans.getAttribute(ObjectName.getInstance("io.digdag.db", "name", "db_dssm_findRootTasksByStates"), "Count");
            assertTrue((long)findTasksByState >= 0);

            //Category EXECUTOR
            Object executor_LoopCount = beans.getAttribute(ObjectName.getInstance("io.digdag.executor", "name", "executor_loopCount"), "Count");
            assertTrue((long)executor_LoopCount >= 0);
        }
    }


    @Test
    public void verifyUncaughtErrorCount()
            throws Exception
    {
        //This test requires remote database
        assumeThat(server.isRemoteDatabase(), is(true));

        try (JMXConnector con = connectJmx(server)) {
            MBeanServerConnection beans = con.getMBeanServerConnection();

            Object uncaughtErrorCount = beans.getAttribute(ObjectName.getInstance("io.digdag.core", "name", "ErrorReporter"), "UncaughtErrorCount");
            assertThat(uncaughtErrorCount, is(0));

            // oops, tasks table is broken!?
            try (DataSourceProvider dsp = new DataSourceProvider(server.getRemoteTestDatabaseConfig())) {
                Statement stmt = dsp.get().getConnection().createStatement();
                stmt.execute("drop table tasks cascade");
            }

            // should increment uncaught exception count
            expect(Duration.ofMinutes(5), () -> {
                int count = (int) beans.getAttribute(ObjectName.getInstance("io.digdag.core", "name", "ErrorReporter"), "UncaughtErrorCount");
                return count > 0;
            });
        }
    }
}
