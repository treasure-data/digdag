package utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.api.JacksonTimeModule;
import org.hamcrest.Matchers;
import org.immutables.value.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertThat;

@Value.Immutable
public abstract class CommandStatus
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new GuavaModule());
        MAPPER.registerModule(new JacksonTimeModule());
        MAPPER.setInjectableValues(new InjectableValues.Std()
                .addValue(ObjectMapper.class, MAPPER));
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public abstract int code();

    public abstract byte[] out();

    public abstract byte[] err();

    public String out(Charset charset)
    {
        return charset.decode(ByteBuffer.wrap(out())).toString();
    }

    public String err(Charset charset)
    {
        return charset.decode(ByteBuffer.wrap(err())).toString();
    }

    public String outUtf8()
    {
        return out(StandardCharsets.UTF_8);
    }

    public String errUtf8()
    {
        return err(StandardCharsets.UTF_8);
    }

    public static CommandStatus of(int code, byte[] out, byte[] err)
    {
        return ImmutableCommandStatus.builder()
                .code(code)
                .out(out)
                .err(err)
                .build();
    }

    public CommandStatus assertSuccess() {
        assertThat(errUtf8(), code(), Matchers.is(0));
        return this;
    }

    public <T> T outJson(Class<T> type)
            throws IOException
    {
        return MAPPER.readValue(outUtf8(), type);
    }

    public <T> T outJson(TypeReference<T> type)
            throws IOException
    {
        return MAPPER.readValue(outUtf8(), type);
    }
}
