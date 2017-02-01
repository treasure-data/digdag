package acceptance.td;

import com.google.common.collect.ImmutableList;

import java.util.Base64;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public class Secrets
{
    static final String TD_API_KEY;

    static final String TD_SECRETS_ENABLED_PROP_KEY = "io.digdag.standards.td.secrets.enabled";

    static {
        // Do not pick up apikey from ~/.td/td.conf during local test runs
        System.setProperty(TD_SECRETS_ENABLED_PROP_KEY, "false");

        TD_API_KEY = System.getenv().getOrDefault("TD_API_KEY", "");
    }

    static final byte[] ENCRYPTION_KEY_BYTES = new byte[128 / 8];
    {
        ThreadLocalRandom.current().nextBytes(ENCRYPTION_KEY_BYTES);
    }

    static final String ENCRYPTION_KEY = Base64.getEncoder().encodeToString(ENCRYPTION_KEY_BYTES);

    static Collection<String> secretsServerConfiguration()
    {
        return ImmutableList.of(
                "digdag.secret-encryption-key = " + ENCRYPTION_KEY,
                "secret-access-policy.enabled = true",
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
