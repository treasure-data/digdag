package acceptance.td;

import com.amazonaws.util.Base64;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public class Secrets
{
    static final String TD_API_KEY = System.getenv().getOrDefault("TD_API_KEY", "");
    static final byte[] ENCRYPTION_KEY_BYTES = new byte[128 / 8];
    {
        ThreadLocalRandom.current().nextBytes(ENCRYPTION_KEY_BYTES);
    }

    static final String ENCRYPTION_KEY = Base64.encodeAsString(ENCRYPTION_KEY_BYTES);

    static Collection<String> secretsServerConfiguration()
    {
        return ImmutableList.of(
                "digdag.secret-encryption-key = " + ENCRYPTION_KEY,
                "secret-access-policy.operators.td.secrets = [\"td.*\"]",
                "secret-access-policy.operators.td_ddl.secrets = [\"td.*\"]",
                "secret-access-policy.operators.td_for_each.secrets = [\"td.*\"]",
                "secret-access-policy.operators.td_load.secrets = [\"td.*\"]",
                "secret-access-policy.operators.td_table_export.secrets = [\"td.*\"]",
                "secret-access-policy.operators.td_wait.secrets = [\"td.*\"]",
                "secret-access-policy.operators.td_wait_table.secrets = [\"td.*\"]"
        );
    }
}
