package acceptance;

import com.google.common.base.Objects;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.immutables.value.Value;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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

    public static Matcher<CommandStatus> success()
    {
        return new BaseMatcher<CommandStatus>()
        {
            @Override
            public boolean matches(Object item)
            {
                return item instanceof CommandStatus &&
                        ((CommandStatus) item).code() == 0;
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendText("command status with code 0");
            }
        };
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("err", errUtf8())
                .add("out", outUtf8())
                .add("code", code())
                .toString();
    }

    public CommandStatus assertSuccess() {
        assertThat(errUtf8() + '\n' + outUtf8(), this, is(success()));
        return this;
    }

}
