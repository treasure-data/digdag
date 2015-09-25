package io.digdag.core;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableSession.class)
public abstract class SessionImpl
        extends Session
{ }
