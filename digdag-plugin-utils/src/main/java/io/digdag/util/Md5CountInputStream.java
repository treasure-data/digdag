package io.digdag.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5CountInputStream
        extends FilterInputStream
{
    private static final MessageDigest MD5;
    private byte[] tempBuffer;

    static {
        try {
            MD5 = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static byte[] digestMd5(byte[] data)
    {
        try {
            MessageDigest md5 = (MessageDigest) MD5.clone();
            return md5.digest(data);
        }
        catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to initialize MD5 digest", ex);
        }
    }

    private final MessageDigest md5;
    private long count;

    public Md5CountInputStream(InputStream in)
    {
        super(in);
        try {
            this.md5 = (MessageDigest) MD5.clone();
        }
        catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to initialize MD5 digest", ex);
        }
        this.count = 0L;
    }

    public long getCount()
    {
        return count;
    }

    public byte[] getDigest()
    {
        return md5.digest();
    }

    @Override
    public int read() throws IOException
    {
        int c = in.read();
        if (c >= 0) {
            count += 1;
            md5.update((byte) c);
        }
        return c;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int n = in.read(b, off, len);
        if (n > 0) {
            count += n;
            md5.update(b, off, n);
        }
        return n;
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException
    {
        if (tempBuffer == null) {
            tempBuffer = new byte[32 * 1024];
        }
        return read(tempBuffer, 0, (int) Math.min(n, tempBuffer.length));
    }

    @Override
    public int available() throws IOException
    {
        return in.available();
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public void mark(int readlimit)
    { }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("mark/reset not supported");
    }
}
