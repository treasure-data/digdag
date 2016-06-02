package io.digdag.core.session;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.digdag.core.repository.ImmutableImplStyle;
import org.immutables.value.Value;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableStoredSession.class)
public abstract class StoredSessionImpl
        extends StoredSession
{ }
