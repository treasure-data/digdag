package acceptance;

import org.junit.Rule;
import org.junit.Test;
import utils.TemporaryDigdagServer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class ServerJmxIT
{
    public static final Pattern JMX_PORT_PATTERN = Pattern.compile("\\s*JMX agent started on port (\\d+)\\s*");

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .inProcess(false)
            .configuration(
                    "server.jmx.port=0")
            .build();

    @Test
    public void scheduleStartTime()
            throws Exception
    {
        Matcher matcher = JMX_PORT_PATTERN.matcher(server.outUtf8());
        assertThat(matcher.find(), is(true));
        int port = Integer.parseInt(matcher.group(1));

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + port + "/jmxrmi");
        try (JMXConnector con = JMXConnectorFactory.connect(url, null)) {
            MBeanServerConnection beans = con.getMBeanServerConnection();

            Object uptime = beans.getAttribute(ObjectName.getInstance("java.lang", "type", "Runtime"), "Uptime");
            assertThat(uptime, instanceOf(Long.class));

            Object enqueueCount = beans.getAttribute(ObjectName.getInstance("io.digdag.core.workflow", "name", "TaskQueueDispatcher"), "EnqueueCount");
            assertThat(enqueueCount, is(0L));
        }
    }
}
