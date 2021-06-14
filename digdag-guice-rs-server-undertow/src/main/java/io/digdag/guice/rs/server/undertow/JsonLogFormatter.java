package io.digdag.guice.rs.server.undertow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.digdag.commons.ThrowablesUtil;
import io.digdag.guice.rs.server.ThreadLocalAccessLogs;

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ExchangeAttributeParser;
import io.undertow.attribute.ExchangeAttributes;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonLogFormatter
{
    private static final String DEFAULT_PATTERN = "json{time:%{time,yyyy-MM-dd'T'HH:mm:ss.SSSZ} host:%h forwardedfor:%{i,X-Forwarded-For} method:%m uri:%U%q protocol:%H status:%s size:%B referer:%{i,Referer} ua:%{i,User-Agent} vhost:%{i,Host} reqtime:%T}";

    private JsonLogFormatter()
    { }

    public static boolean isJsonPattern(String pattern)
    {
        return pattern.equals("json") || (pattern.startsWith("json{") && pattern.endsWith("}"));
    }

    public static ExchangeAttribute buildExchangeAttribute(String pattern, ClassLoader classLoader)
    {
        if (pattern.equals("json")) {
            pattern = DEFAULT_PATTERN;
        }

        String body = pattern.substring("json{".length(), pattern.length() - "}".length());

        Map<String, ExchangeAttribute> map = new LinkedHashMap<>();
        ExchangeAttributeParser parser = ExchangeAttributes.parser(classLoader);

        String[] kvs = body.split(" ");
        for (String kv : kvs) {
            String[] fragments = kv.split(":", 2);
            if (fragments.length != 2) {
                throw new IllegalArgumentException("JSON access log pattern includes an invalid fragment: " + kv);
            }
            String key = fragments[0];
            String value = fragments[1];

            map.put(key, parser.parse(value));
        }

        return new JsonExchangeAttribute(map);
    }

    private static class JsonExchangeAttribute
            implements ExchangeAttribute
    {
        private final Map<String, ExchangeAttribute> map;
        private final JsonFactory jsonFactory = new JsonFactory();

        public JsonExchangeAttribute(Map<String, ExchangeAttribute> map)
        {
            this.map = map;
        }

        @Override
        public String readAttribute(HttpServerExchange exchange)
        {
            StringWriter writer = new StringWriter();
            try (JsonGenerator gen = jsonFactory.createGenerator(writer)) {
                gen.writeStartObject();
                for (Map.Entry<String, ExchangeAttribute> pair : map.entrySet()) {
                    gen.writeFieldName(pair.getKey());
                    gen.writeString(pair.getValue().readAttribute(exchange));
                }

                Map<String, String> appAttributes = ThreadLocalAccessLogs.resetAttributes();
                for (Map.Entry<String, String> pair : appAttributes.entrySet()) {
                    gen.writeFieldName(pair.getKey());
                    gen.writeString(pair.getValue());
                }

                gen.writeEndObject();
            }
            catch (IOException ex) {
                throw ThrowablesUtil.propagate(ex);
            }
            return writer.toString();
        }

        @Override
        public void writeAttribute(HttpServerExchange exchange, String newValue)
            throws ReadOnlyAttributeException
        {
            throw new ReadOnlyAttributeException();
        }
    }
}
