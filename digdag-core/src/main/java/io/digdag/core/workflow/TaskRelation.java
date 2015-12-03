package io.digdag.core.workflow;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.core.spi.TaskReport;
import org.immutables.value.Value;
import io.digdag.core.config.Config;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskRelation.class)
@JsonDeserialize(as = ImmutableTaskRelation.class)
public abstract class TaskRelation
{
    public abstract long getId();

    public abstract Optional<Long> getParentId();

    public abstract List<Long> getUpstreams();
}
