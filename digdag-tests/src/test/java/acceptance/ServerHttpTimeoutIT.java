package acceptance;

import org.junit.Rule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.TemporaryDigdagServer;

import java.time.Duration;
import java.net.Socket;
import java.net.InetSocketAddress;

import static java.nio.charset.StandardCharsets.UTF_8;

import static utils.TestUtils.expect;

public class ServerHttpTimeoutIT
{
    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .inProcess(false)
            .configuration(
                    "server.http.no-request-timeout=2",
                    "server.http.request-parse-timeout=2",
                    "server.http.io-idle-timeout=11"
                    )
            .build();

    private Socket socket;

    @Before
    public void setUp()
            throws Exception
    {
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(30*1000);
        socket.connect(new InetSocketAddress(server.host(), server.port()));
    }

    @After
    public void close()
            throws Exception
    {
        socket.close();
    }

    @Test
    public void verifyNoRequestTimeout()
            throws Exception
    {
        expect(Duration.ofSeconds(10), () -> socket.getInputStream().read() == -1);
    }

    @Test
    public void verifyRequestParseTimeout()
            throws Exception
    {
        socket.getOutputStream().write("GET /api/version HTTP/1.1\r\n".getBytes(UTF_8));
        expect(Duration.ofSeconds(10), () -> socket.getInputStream().read() == -1);
    }

    @Test
    public void verifyRequestBodyTimeout()
            throws Exception
    {
        socket.getOutputStream().write("POST /api/schedules/1/skip HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\r\n".getBytes(UTF_8));
        expect(Duration.ofSeconds(20), () -> socket.getInputStream().read() == -1);
    }
}
