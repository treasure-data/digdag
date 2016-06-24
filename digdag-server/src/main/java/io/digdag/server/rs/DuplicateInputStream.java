package io.digdag.server.rs;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public class DuplicateInputStream
        extends FilterInputStream
{
    private final OutputStream out;
    private byte[] tempBuffer;

    public DuplicateInputStream(InputStream in, OutputStream out)
    {
        super(in);
        this.out = out;
    }

    private void ensureTempBuffer()
    {
        if (tempBuffer == null) {
            tempBuffer = new byte[32 * 1024];
        }
    }

    @Override
    public int read() throws IOException
    {
        int c = in.read();
        if (c >= 0) {
            out.write(c);
        }
        return c;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int n = in.read(b, off, len);
        if (n >= 0) {
            out.write(b, off, n);
        }
        return n;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException
    {
        ensureTempBuffer();
        return read(tempBuffer, 0, (int) Math.min(n, tempBuffer.length));
    }

    @Override
    public void close() throws IOException
    {
        ensureTempBuffer();
        while (read(tempBuffer) >= 0);
        in.close();
    }

    public void abort() throws IOException
    {
        in.close();
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
