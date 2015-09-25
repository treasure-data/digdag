package io.digdag.core;

import java.util.List;
import java.util.Map;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableStoredWorkflow.class)
public abstract class StoredWorkflow
        extends Workflow
{
    public abstract int getId();

    public abstract int getRevisionId();
}
