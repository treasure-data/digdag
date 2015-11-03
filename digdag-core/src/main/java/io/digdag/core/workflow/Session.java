package io.digdag.core.workflow;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.config.Config;

@JsonDeserialize(as = ImmutableSession.class)
public abstract class Session
{
    public abstract String getName();

    public abstract Config getParams();

    public abstract SessionOptions getOptions();

    public static ImmutableSession.Builder sessionBuilder()
    {
        return ImmutableSession.builder();
    }
}
