package acceptance;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import redis.clients.jedis.Jedis;

import static org.junit.Assume.assumeTrue;

public class BaseParamRedisIT
{
    protected static final String REDIS_HOST = System.getenv("REDIS_HOST");
    protected Jedis redisClient;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp()
    {
        assumeTrue(REDIS_HOST != null && !REDIS_HOST.isEmpty());

        this.redisClient = new Jedis(REDIS_HOST);
        redisClient.connect();
        redisClient.flushDB();
    }

    @After
    public void tearDown()
    {
        if (this.redisClient != null) {
            redisClient.flushDB();
            this.redisClient.close();
        }
    }
}
