package io.digdag.client.api;

import java.util.Arrays;
import java.util.Base64;
import java.nio.ByteBuffer;

public class RestApiKey
{
    public static RestApiKey of(String key)
    {
        String[] fragments = key.split("/", 2);
        if (fragments.length != 2) {
            throw new IllegalArgumentException("Invalid API key format");
        }

        byte[] idData = Base64.getUrlDecoder().decode(fragments[0]);
        if (idData.length != 8) {
            throw new IllegalArgumentException("Invalid API key format");
        }
        long id = ByteBuffer.wrap(idData).getLong();

        byte[] secret = Base64.getUrlDecoder().decode(fragments[1]);
        if (secret.length != 32) {
            throw new IllegalArgumentException("Invalid API key format");
        }

        return new RestApiKey(id, secret);
    }

    private final long id;
    private final byte[] secret;

    private RestApiKey(long id, byte[] secret)
    {
        this.id = id;
        this.secret = secret;
    }

    public long getId()
    {
        return id;
    }

    public String getIdString()
    {
        byte[] idData = new byte[8];
        ByteBuffer.wrap(idData).putLong(id);
        return Base64.getUrlEncoder().encodeToString(idData);
    }

    public byte[] getSecret()
    {
        return secret;
    }

    @Override
    public String toString()
    {
        String secretPart = Base64.getUrlEncoder().encodeToString(secret);
        String idPart = getIdString();
        return idPart + "/" + secretPart;
    }

    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof RestApiKey)) {
            return false;
        }
        return toString().equals(((RestApiKey) o).toString());
    }
}
