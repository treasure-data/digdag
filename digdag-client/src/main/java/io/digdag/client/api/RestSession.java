package io.digdag.client.api;

import java.util.Date;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.spi.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSession.class)
@JsonDeserialize(as = ImmutableRestSession.class)
public abstract class RestSession
{
    public abstract long getId();

    public abstract String getName();

    // TODO
    //public abstract IdName getRepository();

    // TODO
    //public abstract String getRevision();

    public abstract Config getParams();

    public abstract Date getCreatedAt();

    // TODO
    //public abstract IdName getWorkflow();

    public static ImmutableRestSession.Builder builder()
    {
        return ImmutableRestSession.builder();
    }
}
