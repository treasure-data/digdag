package io.digdag.core.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.database.TransactionManager;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class DefaultSecretProvider
        implements SecretProvider
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSecretProvider.class);

    interface OperatorSecretFilter
    {
        boolean test(String key, boolean userGranted);
    }

    private final SecretAccessContext context;
    private final Config mounts;
    private final SecretStore secretStore;
    private final TransactionManager tm;

    DefaultSecretProvider(SecretAccessContext context, Config mounts, SecretStore secretStore,
            TransactionManager tm)
    {
        this.context = context;
        this.mounts = mounts;
        this.secretStore = secretStore;
        this.tm = tm;
    }

    @Override
    public Optional<String> getSecretOptional(String key)
    {
        // Sanity check key
        String errorMessage = "Illegal key: '" + key + "'";
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key), errorMessage);
        Preconditions.checkArgument(key.indexOf('*') == -1, errorMessage);

        String remountedKey = remount(key);

        return fetchSecret(remountedKey);
    }

    private String remount(String key)
    {
        List<String> segments = Splitter.on('.').splitToList(key);
        segments.forEach(segment -> Preconditions.checkArgument(!Strings.isNullOrEmpty(segment)));
        JsonNode scope = mounts.getInternalObjectNode();

        int i = 0;

        while (true) {
            String segment = segments.get(i);
            JsonNode node = scope.get(segment);
            if (node == null) {
                // Key falls outside the override scope. No remount.
                return key;
            }
            if (node.isObject()) {
                // Dig deeper
                i++;
                if (i >= segments.size()) {
                    // Key ended before we reached a leaf. No override.
                    return key;
                }
                scope = node;
            }
            else if (node.isTextual()) {
                // Reached a path-overriding grant leaf.
                List<String> remainder = segments.subList(i + 1, segments.size());
                List<String> base = Splitter.on('.').splitToList(node.asText());
                String remounted = FluentIterable
                        .from(base)
                        .append(remainder)
                        .join(Joiner.on('.'));
                return remounted;
            }
            else if (node.isBoolean() && node.asBoolean()) {
                // Reached a grant leaf.
                logger.warn("Granting access to secrets in '_secrets' is deprecated. All secrets are accessible by default.");
                return key;
            }
            else {
                throw new ConfigException("Illegal value in _secrets: " + node);
            }
        }
    }

    private Optional<String> fetchSecret(String key)
    {
        return tm.begin(() -> {
            Optional<String> projectSecret = secretStore.getSecret(context.projectId(), SecretScopes.PROJECT, key);

            if (projectSecret.isPresent()) {
                return projectSecret;
            }

            return secretStore.getSecret(context.projectId(), SecretScopes.PROJECT_DEFAULT, key);
        });
    }
}
