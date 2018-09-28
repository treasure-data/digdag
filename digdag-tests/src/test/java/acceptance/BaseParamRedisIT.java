package acceptance;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import redis.clients.jedis.Jedis;

import static org.junit.Assume.assumeTrue;

public class BaseParamRedisIT
{
    protected static final String DIGDAG_TEST_REDIS = System.getenv("DIGDAG_TEST_REDIS");
    protected Jedis redisClient;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp()
    {
        assumeTrue(DIGDAG_TEST_REDIS != null && !DIGDAG_TEST_REDIS.isEmpty());

        this.redisClient = new Jedis(DIGDAG_TEST_REDIS);
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
