package io.digdag.client.api;

import java.util.Locale;
import java.time.ZoneId;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.DateTimeException;
import java.time.zone.ZoneRulesException;
import java.io.IOException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;

public class JacksonTimeModule
    extends SimpleModule
{
    public JacksonTimeModule()
    {
        super();
        addSerializer(Instant.class, new InstantSerializer());
        addDeserializer(Instant.class, new InstantDeserializer());
        addSerializer(OffsetDateTime.class, new OffsetDateTimeSerializer());
        addDeserializer(OffsetDateTime.class, new OffsetDateTimeDeserializer());
        addSerializer(ZoneId.class, new ZoneIdSerializer());
        addDeserializer(ZoneId.class, new ZoneIdDeserializer());
    }

    public static class InstantSerializer
            extends JsonSerializer<Instant>
    {
        private final DateTimeFormatter formatter;

        public InstantSerializer()
        {
            this.formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)
                .withZone(ZoneId.of("UTC"));
        }

        @Override
        public void serialize(Instant value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeString(formatter.format(value));
        }
    }

    public static class InstantDeserializer
            extends FromStringDeserializer<Instant>
    {
        public InstantDeserializer()
        {
            super(Instant.class);
        }

        @Override
        protected Instant _deserialize(String value, DeserializationContext context)
                throws JsonMappingException
        {
            try {
                return Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value));
            }
            catch (DateTimeParseException ex) {
                throw new JsonMappingException("Invalid ISO time format: " + value, ex);
            }
        }
    }

    public static class OffsetDateTimeSerializer
            extends JsonSerializer<OffsetDateTime>
    {
        private final DateTimeFormatter formatter;

        public OffsetDateTimeSerializer()
        {
            this.formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", Locale.ENGLISH);
        }

        @Override
        public void serialize(OffsetDateTime value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeString(formatter.format(value));
        }
    }

    public static class OffsetDateTimeDeserializer
            extends FromStringDeserializer<OffsetDateTime>
    {

        public OffsetDateTimeDeserializer()
        {
            super(OffsetDateTime.class);
        }

        @Override
        protected OffsetDateTime _deserialize(String value, DeserializationContext context)
                throws JsonMappingException
        {
            try {
                return OffsetDateTime.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(value));
            }
            catch (DateTimeParseException ex) {
                throw new JsonMappingException("Invalid ISO time format: %s" + value, ex);
            }
        }
    }

    public static class ZoneIdSerializer
            extends JsonSerializer<ZoneId>
    {
        @Override
        public void serialize(ZoneId value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeString(value.getId());
        }
    }

    public static class ZoneIdDeserializer
            extends FromStringDeserializer<ZoneId>
    {
        public ZoneIdDeserializer()
        {
            super(ZoneId.class);
        }

        @Override
        protected ZoneId _deserialize(String value, DeserializationContext context)
                throws JsonMappingException
        {
            try {
                return ZoneId.of(value);
            }
            catch (DateTimeException ex) {
                throw new JsonMappingException("Unknown time zone name: " + value, ex);
            }
        }
    }
}
