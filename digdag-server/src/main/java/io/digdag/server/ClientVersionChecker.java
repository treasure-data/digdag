package io.digdag.server;

import com.google.common.base.Optional;
import io.digdag.client.Version;
import io.digdag.client.config.Config;

public class ClientVersionChecker
{
    public static ClientVersionChecker fromSystemConfig(Config systemConfig)
    {
        return new ClientVersionChecker(
                systemConfig.getOptional("server.client-version-check.upgrade-recommended-if-older", String.class).transform(Version::parse),
                systemConfig.getOptional("server.client-version-check.api-incompatible-if-older", String.class).transform(Version::parse));
    }

    private final Optional<Version> upgradeRecommendedUntil;
    private final Optional<Version> apiIncompatibleUntil;

    private ClientVersionChecker(
            Optional<Version> upgradeRecommendedUntil,
            Optional<Version> apiIncompatibleUntil)
    {
        this.upgradeRecommendedUntil = upgradeRecommendedUntil;
        this.apiIncompatibleUntil = apiIncompatibleUntil;
    }

    public boolean isUpgradeRecommended(Version clientVersion)
    {
        if (!upgradeRecommendedUntil.isPresent()) {
            return false;
        }
        return clientVersion.isOlder(upgradeRecommendedUntil.get());
    }

    public boolean isApiCompatible(Version clientVersion)
    {
        if (!apiIncompatibleUntil.isPresent()) {
            return true;
        }
        boolean incompatible = clientVersion.isOlder(apiIncompatibleUntil.get());
        return !incompatible;
    }
}
