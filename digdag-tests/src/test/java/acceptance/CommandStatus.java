package acceptance;

import org.immutables.value.Value;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Value.Immutable
public abstract class CommandStatus
{
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
}
