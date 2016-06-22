package io.digdag.server.rs;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

public class ReplicatedInputStream
        extends FilterInputStream
{
    public static ReplicatedInputStream replicate(InputStream in)
    {
        return new ReplicatedInputStream(in);
    }

    private static final byte[] EMPTY = new byte[0];

    private byte[] buffer = EMPTY;
    private int followerRemaining = 0;
    private int followerOffset;

    private class Follower
            extends InputStream
    {
        private boolean closed;

        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            while (true) {
                if (closed) {
                    throw new IOException("Stream Closed");
                }
                if (buffer == null) {
                    return -1;
                }
                if (followerRemaining > 0) {
                    break;
                }
                try {
                    wait();
                }
                catch (InterruptedException ex) {
                    throw new InterruptedIOException();
                }
            }

            if (followerRemaining <= len) {
                len = followerRemaining;
                System.arraycopy(buffer, followerOffset, b, off, len);
                followerRemaining = 0;
                return len;
            }
            else {
                System.arraycopy(buffer, followerOffset, b, off, len);
                followerOffset += len;
                followerRemaining -= len;
                return len;
            }
        }

        @Override
        public synchronized long skip(long n) throws IOException
        {
            if (followerRemaining <= n) {
                n = followerRemaining;
                followerRemaining = 0;
                return n;
            }
            else {
                int len = (int) n;  // n is smaller than followerRemaining which is int
                followerOffset += len;
                followerRemaining -= len;
                return len;
            }
        }

        @Override
        public int read() throws IOException
        {
            return read(new byte[1]);
        }

        @Override
        public synchronized int read(byte[] b) throws IOException
        {
            return read(b, 0, b.length);
        }

        @Override
        public synchronized int available() throws IOException
        {
            if (buffer == null) {
                return 0;
            }
            return followerRemaining;
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
        { }

        @Override
        public synchronized void close() throws IOException
        {
            closed = true;
        }
    }

    private final Follower follower;

    public ReplicatedInputStream(InputStream in)
    {
        super(in);
        this.follower = new Follower();
    }

    public InputStream getFollower()
    {
        return follower;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException
    {
        if (buffer == null) {
            // already closed
            throw new IOException("Stream Closed");
        }

        while (followerRemaining > 0) {
            try {
                wait();
            }
            catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }
        assert followerRemaining == 0;

        int n = in.read(b, off, len);
        if (n < 0) {
            buffer = null;
            return n;
        }
        else {
            ensureBufferCapacity(n);
            System.arraycopy(b, off, buffer, 0, n);
            followerRemaining = n;
            followerOffset = 0;
            return n;
        }
    }

    private void ensureBufferCapacity(int required)
    {
        if (buffer == null || buffer.length < required) {
            int len = Math.min(buffer.length, 512);
            while (len < required) {
                len *= 2;
            }
            buffer = new byte[len];
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        in.close();
    }

    @Override
    public int read() throws IOException
    {
        return read(new byte[1]);
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public synchronized long skip(long n) throws IOException
    {
        return read(new byte[(int) Math.min(n, 64*1024L)]);
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
    { }
}
