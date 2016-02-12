package io.digdag.core.queue;

import io.digdag.core.repository.ImmutableImplStyle;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableQueueSetting.class)
public abstract class QueueSettingImpl
        extends QueueSetting
{ }
