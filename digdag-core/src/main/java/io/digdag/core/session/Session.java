package io.digdag.core.session;

import java.time.Instant;
import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinition;

@JsonDeserialize(as = ImmutableSession.class)
public abstract class Session
{
    public abstract int getRepositoryId();

    public abstract String getWorkflowName();

    public abstract Instant getInstant();

    public static ImmutableSession.Builder sessionBuilder()
    {
        return ImmutableSession.builder();
    }

    /*
    public static ImmutableSession.Builder sessionBuilder(
            String name,
            Config revisionDefaultParams,
            WorkflowDefinition source,
            Config overwriteParams)
    {
        return sessionBuilder(name, ZoneId.of("UTC"),
            revisionDefaultParams, source, overwriteParams);
    }

    public static ImmutableSession.Builder sessionBuilder(
            String name,
            ZoneId defaultTimeZone,
            Config revisionDefaultParams,
            WorkflowDefinition source,
            Config overwriteParams)
    {
        ZoneId timeZone = ZoneId.of(
                overwriteParams.get("timezone", String.class,
                    source.getConfig().get("timezone", String.class,
                        revisionDefaultParams.get("timezone", String.class,
                            defaultTimeZone.getId()))));

        Config params = overwriteParams.deepCopy()
            .set("timezone", timeZone.getId());
            */
        //.set("time_zone_offset", /*how to calculate using ZoneId API? needs joda-time?*/)

    /*
        return ImmutableSession.builder()
            .name(name)
            .params(params);
    }
    */
}
