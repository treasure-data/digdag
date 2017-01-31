package io.digdag.server;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import io.digdag.client.config.Config;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkArgument;

@Value.Immutable
@JsonDeserialize(as = ImmutableAgentLauncherConfig.class)
public interface AgentLauncherConfig
{
    Optional<String> getDownloadUrl();

    Optional<String> getMd5();

    Optional<String> getLocalJarPath();

    List<String> getJavaOptions();

    List<String> getJavaArguments();

    public static ImmutableAgentLauncherConfig.Builder defaultBuilder()
    {
        return ImmutableAgentLauncherConfig.builder()
            .downloadUrl(Optional.absent())
            .md5(Optional.absent())
            .localJarPath(Optional.absent())
            .javaOptions(ImmutableList.of())
            .javaArguments(ImmutableList.of());
    }

    public static AgentLauncherConfig defaultConfig()
    {
        return defaultBuilder().build();
    }

    public static AgentLauncherConfig convertFrom(Config config)
    {
        Optional<String> md5 = config.getOptional("server.agent-launcher.md5", String.class);
        Optional<String> localJarPath = config.getOptional("server.agent-launcher.path", String.class);

        // auto-calculate md5 if path is set
        if (localJarPath.isPresent() && md5.isPresent()) {
            try {
                byte[] bin = com.google.common.io.Files.hash(
                        Paths.get(localJarPath.get()).toFile(),
                        Hashing.md5()
                        ).asBytes();
                md5 = Optional.of(Base64.getEncoder().encodeToString(bin));
            }
            catch (IOException ex) {
                throw new IllegalArgumentException("Failed to calculate md5 of server.agent-launcher.path", ex);
            }
        }

        return defaultBuilder()
            .downloadUrl(config.getOptional("server.agent-launcher.url", String.class))
            .md5(md5)
            .localJarPath(localJarPath)
            .javaOptions(config.parseList("server.agent-launcher.java-options", String.class))
            .javaArguments(config.parseList("server.agent-launcher.java-arguments", String.class))
            .build();
    }

    @Value.Check
    default void check()
    {
        if (getMd5().isPresent()) {
            byte[] bin;
            try {
                bin = Base64.getDecoder().decode(getMd5().get());
            }
            catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("server.agent-launcher.md5 must be a base64-encoded string", ex);
            }
            checkArgument(bin.length == 16, "server.agent-launcher.md5 must be a base64-encoded string of 64 bytes binary");
        }
        if (getDownloadUrl().isPresent() || getLocalJarPath().isPresent()) {
            checkArgument(getMd5().isPresent(), "server.agent-launcher.md5 must be set when download-url is set");
        }
    }
}
