package io.digdag.core.session;

import io.digdag.core.repository.ImmutableImplStyle;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableStoredTask.class)
public abstract class StoredTaskImpl
        extends StoredTask
{ }
