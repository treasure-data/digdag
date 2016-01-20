package io.digdag.core.session;

import java.util.TimeZone;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowSource;

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

    public static ImmutableSession.Builder sessionBuilder(
            String name,
            Config revisionDefaultParams,
            WorkflowSource source,
            Config overwriteParams)
    {
        return sessionBuilder(name, TimeZone.getTimeZone("UTC"),
            revisionDefaultParams, source, overwriteParams);
    }

    public static ImmutableSession.Builder sessionBuilder(
            String name,
            TimeZone defaultTimeZone,
            Config revisionDefaultParams,
            WorkflowSource source,
            Config overwriteParams)
    {
        TimeZone timeZone = TimeZone.getTimeZone(
                overwriteParams.get("timezone", String.class,
                    source.getConfig().get("timezone", String.class,
                        revisionDefaultParams.get("timezone", String.class,
                            defaultTimeZone.getID()))));

        Config params = overwriteParams.deepCopy()
            .set("timezone", timeZone.getID());
        //.set("time_zone_offset", /*how to calculate using TimeZone API? needs joda-time?*/)

        return ImmutableSession.builder()
            .name(name)
            .params(params);
    }

    public static TimeZone getSessionTimeZone(Session s)
    {
        return TimeZone.getTimeZone(s.getParams().get("timezone", String.class));
    }
}
