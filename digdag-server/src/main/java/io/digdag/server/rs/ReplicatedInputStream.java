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

    private volatile byte[] buffer = EMPTY;
    private volatile int followerRemaining = 0;
    private volatile int followerOffset;
    private volatile boolean followerClosed;

    private class Follower
            extends InputStream
    {
        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            synchronized (ReplicatedInputStream.this) {
                while (true) {
                    if (followerClosed) {
                        throw new IOException("Stream Closed");
                    }
                    else if (buffer == null) {
                        return -1;
                    }
                    else if (followerRemaining > 0) {
                        break;
                    }
                    else {
                        try {
                            ReplicatedInputStream.this.wait();
                        }
                        catch (InterruptedException ex) {
                            throw new InterruptedIOException();
                        }
                    }
                }

                if (followerRemaining <= len) {
                    len = followerRemaining;
                    System.arraycopy(buffer, followerOffset, b, off, len);
                    followerRemaining = 0;
                    ReplicatedInputStream.this.notifyAll();
                    return len;
                }
                else {
                    System.arraycopy(buffer, followerOffset, b, off, len);
                    followerOffset += len;
                    followerRemaining -= len;
                    ReplicatedInputStream.this.notifyAll();
                    return len;
                }
            }
        }

        @Override
        public long skip(long n) throws IOException
        {
            synchronized (ReplicatedInputStream.this) {
                if (followerRemaining <= n) {
                    n = followerRemaining;
                    followerRemaining = 0;
                    ReplicatedInputStream.this.notifyAll();
                    return n;
                }
                else {
                    int len = (int) n;  // n is smaller than followerRemaining which is int
                    followerOffset += len;
                    followerRemaining -= len;
                    ReplicatedInputStream.this.notifyAll();
                    return len;
                }
            }
        }

        @Override
        public int read() throws IOException
        {
            return read(new byte[1]);
        }

        @Override
        public int read(byte[] b) throws IOException
        {
            return read(b, 0, b.length);
        }

        @Override
        public int available() throws IOException
        {
            synchronized (ReplicatedInputStream.this) {
                if (buffer == null) {
                    return 0;
                }
                return followerRemaining;
            }
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

        @Override
        public void close() throws IOException
        {
            synchronized (ReplicatedInputStream.this) {
                followerClosed = true;
                ReplicatedInputStream.this.notifyAll();
            }
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

        if (followerClosed) {
            return in.read(b, off, len);
        }
        else {
            while (followerRemaining > 0 && !followerClosed) {
                try {
                    wait();
                }
                catch (InterruptedException ex) {
                    throw new InterruptedIOException();
                }
            }

            int n = in.read(b, off, len);
            if (n < 0) {
                buffer = null;
                notifyAll();
                return n;
            }
            else {
                ensureBufferCapacity(n);
                System.arraycopy(b, off, buffer, 0, n);
                followerRemaining = n;
                followerOffset = 0;
                notifyAll();
                return n;
            }
        }
    }

    private void ensureBufferCapacity(int required)
    {
        if (buffer == null || buffer.length < required) {
            int len = Math.max(buffer.length, 512);
            while (len < required) {
                len *= 2;
            }
            buffer = new byte[len];
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if (!followerClosed) {
            // skip all following
            byte[] trail = new byte[1024];
            while (read(trail) >= 0);
        }
        in.close();
        buffer = null;
        notifyAll();
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
    {
        throw new IOException("mark/reset not supported");
    }
}
