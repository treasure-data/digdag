package io.digdag.core.session;

import java.util.TimeZone;
import io.digdag.spi.config.Config;
import io.digdag.core.repository.WorkflowSource;

public final class Sessions
{
    private Sessions()
    { }

    public static ImmutableSession.Builder newSession(
            String name,
            Config revisionParams, WorkflowSource source, Config overwriteParams)
    {
        return newSession(name, TimeZone.getTimeZone("UTC"),
            revisionParams, source, overwriteParams);
    }

    public static ImmutableSession.Builder newSession(
            String name, TimeZone defaultTimeZone,
            Config revisionParams, WorkflowSource source, Config overwriteParams)
    {
        TimeZone timeZone = TimeZone.getTimeZone(
                overwriteParams.get("timezone", String.class,
                    source.getConfig().get("timezone", String.class,
                        revisionParams.get("timezone", String.class,
                            defaultTimeZone.getID()))));

        Config params = overwriteParams.deepCopy()
            .set("timezone", timeZone.getID());
        //.set("time_zone_offset", /*how to calculate using TimeZone API? needs joda-time?*/)

        return Session.sessionBuilder()
            .params(params);
    }

    public static TimeZone getSessionTimeZone(Session s)
    {
        return TimeZone.getTimeZone(s.getParams().get("timezone", String.class));
    }
}
