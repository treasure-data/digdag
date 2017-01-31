package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestAgentLauncherHandle.class)
public interface RestAgentLauncherHandle
{
    Optional<RestDirectDownloadHandle> getDirect();

    // base64 md5 checksum
    String getMd5();

    List<String> getJavaOptions();

    List<String> getJavaArguments();

    static ImmutableRestAgentLauncherHandle.Builder builder()
    {
        return ImmutableRestAgentLauncherHandle.builder();
    }
}
