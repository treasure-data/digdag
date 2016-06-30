package io.digdag.spi;

import java.net.URL;
import java.net.MalformedURLException;
import org.immutables.value.Value;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

@Value.Immutable
public interface DirectUploadHandle
{
    @JsonValue
    URL getUrl();

    static DirectDownloadHandle of(String url)
    {
        try {
            return of(new URL(url));
        }
        catch (MalformedURLException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @JsonCreator
    static DirectDownloadHandle of(URL url)
    {
        return ImmutableDirectDownloadHandle.builder()
            .url(url)
            .build();
    }
}
