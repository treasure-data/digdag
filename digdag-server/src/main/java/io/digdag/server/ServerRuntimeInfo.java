package io.digdag.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@JsonSerialize(as = ImmutableServerRuntimeInfo.class)
@JsonDeserialize(as = ImmutableServerRuntimeInfo.class)
public interface ServerRuntimeInfo
{
    @JsonProperty("local_addresses")
    List<Address> localAddresses();

    @JsonProperty("local_admin_addresses")
    List<Address> localAdminAddresses();

    static ImmutableServerRuntimeInfo.Builder builder() {
        return ImmutableServerRuntimeInfo.builder();
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableAddress.class)
    @JsonDeserialize(as = ImmutableAddress.class)
    interface Address
    {
        String host();

        int port();

        static Address of(String host, int port) {
            return ImmutableAddress.builder().host(host).port(port).build();
        }
    }
}
