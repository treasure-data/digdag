package io.digdag.core;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSession.class)
public abstract class Session
{
    public abstract String getName();

    public abstract ConfigSource getParams();

    public abstract SessionOptions getOptions();

    public static ImmutableSession.Builder sessionBuilder()
    {
        return ImmutableSession.builder();
    }
}
