package utils;

import io.digdag.client.Version;
import static io.digdag.client.DigdagVersion.buildVersion;

public class LocalVersion
{
    public static LocalVersion of()
    {
        return of(buildVersion());
    }

    public static LocalVersion of(Version version)
    {
        return new LocalVersion(version, false);
    }

    private final Version version;
    private final boolean batchModeCheck;

    private LocalVersion(Version version, boolean batchModeCheck)
    {
        this.version = version;
        this.batchModeCheck = batchModeCheck;
    }

    public LocalVersion withBatchModeCheck(boolean enabled)
    {
        return new LocalVersion(version, enabled);
    }

    public Version getVersion()
    {
        return version;
    }

    public boolean isBatchModeCheck()
    {
        return batchModeCheck;
    }
}
