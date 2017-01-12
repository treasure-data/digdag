package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.util.UserSecretTemplate;

public class Secrets
{
    public static ObjectNode resolveSecrets(ObjectNode node, SecretProvider secrets)
    {
        ObjectNode newNode = node.objectNode();
        node.fields().forEachRemaining(entry -> newNode.set(
                UserSecretTemplate.of(entry.getKey()).format(secrets),
                resolveSecrets(entry.getValue(), secrets)));
        return newNode;
    }

    public static ArrayNode resolveSecrets(ArrayNode node, SecretProvider secrets)
    {
        ArrayNode newNode = node.arrayNode();
        node.elements().forEachRemaining(element -> newNode.add(resolveSecrets(element, secrets)));
        return newNode;
    }

    public static TextNode resolveSecrets(TextNode node, SecretProvider secrets)
    {
        return new TextNode(UserSecretTemplate.of(node.textValue()).format(secrets));
    }

    @SuppressWarnings("unchecked")
    public static <T extends JsonNode> T resolveSecrets(T node, SecretProvider secrets)
    {
        if (node.isObject()) {
            return (T) resolveSecrets((ObjectNode) node, secrets);
        }
        else if (node.isArray()) {
            return (T) resolveSecrets((ArrayNode) node, secrets);
        }
        else if (node.isTextual()) {
            return (T) resolveSecrets((TextNode) node, secrets);
        }
        else {
            return node;
        }
    }

    public static Config resolveSecrets(Config config, SecretProvider secrets)
    {
        return new ConfigFactory(DigdagClient.objectMapper())
                .create(resolveSecrets(config.getInternalObjectNode(), secrets));
    }
}
