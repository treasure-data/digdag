package io.digdag.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class ResumableInputStreamTest
{
    static final int IO_EXCEPTION = -2;
    static final int RUNTIME_EXCEPTION = -3;

    static class TestingInputStream
            extends InputStream
    {
        private final Deque<Integer> returnValues;
        private boolean closed;

        public TestingInputStream(int... returnValues)
        {
            this.returnValues = new ArrayDeque<Integer>(returnValues.length);
            for (int c : returnValues) {
                this.returnValues.addLast(c);
            }
        }

        @Override
        public int read() throws IOException
        {
            return throwIfErrorCommand(returnValues.removeFirst());
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            return throwIfErrorCommand(returnValues.removeFirst());
        }

        @Override
        public long skip(long n) throws IOException
        {
            return throwIfErrorCommand(returnValues.removeFirst());
        }

        @Override
        public int available() throws IOException
        {
            int sum = 0;
            for (int c : returnValues) {
                if (c > 0) {
                    sum += c;
                }
            }
            return sum;
        }

        @Override
        public void close() throws IOException
        {
            closed = true;
        }

        public boolean isClosed()
        {
            return closed;
        }

        private int throwIfErrorCommand(int c) throws IOException
        {
            switch (c) {
            case RUNTIME_EXCEPTION:
                throw new RuntimeException("runtime");
            case IO_EXCEPTION:
                throw new IOException("io");
            default:
                return c;
            }
        }

        @Override
        public boolean markSupported()
        {
            // this is a dummy implementation that shouldn't matter
            return true;
        }
    }

    static class SimpleReopener
            implements ResumableInputStream.Reopener
    {
        private final InputStream next;
        private long lastOffset;
        private Exception lastClosedCause;
        private int reopenCount = 0;

        public SimpleReopener(InputStream next)
        {
            this.next = next;
        }

        @Override
        public InputStream reopen(long offset, Exception closedCause) throws IOException
        {
            if (next == null) {
                throw new RuntimeException("reopen");
            }
            this.lastOffset = offset;
            this.lastClosedCause = closedCause;
            this.reopenCount++;
            return next;
        }

        public long getLastOffset() { return lastOffset; }

        public Exception getLastClosedCause() { return lastClosedCause; }

        public int getReopenCount() { return reopenCount; }
    }

    @Test
    public void readReadsFromNext() throws Exception
    {
        ResumableInputStream in = new ResumableInputStream(new TestingInputStream(1, 2, -1, -1), new SimpleReopener(null));
        assertThat(in.read(), is(1));
        assertThat(in.read(), is(2));
        assertThat(in.read(), is(-1));
        assertThat(in.read(), is(-1));
    }

    @Test
    public void readReadsFromNextWithBytes() throws Exception
    {
        ResumableInputStream in = new ResumableInputStream(new TestingInputStream(1, 2, -1, -1), new SimpleReopener(null));
        assertThat(in.read(new byte[1]), is(1));
        assertThat(in.read(new byte[1]), is(2));
        assertThat(in.read(new byte[1]), is(-1));
        assertThat(in.read(new byte[1]), is(-1));
    }

    @Test
    public void readReadsFromNextWithBytesAndOffset() throws Exception
    {
        ResumableInputStream in = new ResumableInputStream(new TestingInputStream(1, 2, -1, -1), new SimpleReopener(null));
        assertThat(in.read(new byte[1], 0, 1), is(1));
        assertThat(in.read(new byte[1], 0, 1), is(2));
        assertThat(in.read(new byte[1], 0, 1), is(-1));
        assertThat(in.read(new byte[1], 0, 1), is(-1));
    }

    @Test
    public void skipSkipsFromNext() throws Exception
    {
        ResumableInputStream in = new ResumableInputStream(new TestingInputStream(1, 2, -1, -1), new SimpleReopener(null));
        assertThat(in.skip(1), is(1L));
        assertThat(in.skip(1), is(2L));
        assertThat(in.skip(1), is(-1L));
        assertThat(in.skip(1), is(-1L));
    }

    @Test
    public void closeClosesNext() throws Exception
    {
        TestingInputStream next = new TestingInputStream(-1);
        ResumableInputStream in = new ResumableInputStream(next, new SimpleReopener(null));
        in.close();
        assertThat(next.isClosed(), is(true));
    }

    @Test
    public void markIsNotSupported() throws Exception
    {
        TestingInputStream next = new TestingInputStream(-1);
        ResumableInputStream in = new ResumableInputStream(next, new SimpleReopener(null));
        assertThat(in.markSupported(), is(false));
    }

    @Test
    public void readResumesWithRuntimeException() throws Exception
    {
        TestingInputStream next = new TestingInputStream(
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                1,
                IO_EXCEPTION,
                2,
                -1);
        SimpleReopener reopener = new SimpleReopener(next);
        ResumableInputStream in = new ResumableInputStream(next, reopener);
        assertThat(in.read(), is(1));
        assertThat(reopener.getLastOffset(), is(0L));
        assertThat(reopener.getReopenCount(), is(5));
        assertThat(in.read(), is(2));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
        assertThat(in.read(), is(-1));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
    }

    @Test
    public void readWithBytesResumesWithRuntimeException() throws Exception
    {
        TestingInputStream next = new TestingInputStream(
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                1,
                IO_EXCEPTION,
                2,
                -1);
        SimpleReopener reopener = new SimpleReopener(next);
        ResumableInputStream in = new ResumableInputStream(next, reopener);
        assertThat(in.read(new byte[1]), is(1));
        assertThat(reopener.getLastOffset(), is(0L));
        assertThat(reopener.getLastClosedCause(), instanceOf(RuntimeException.class));
        assertThat(reopener.getReopenCount(), is(5));
        assertThat(in.read(new byte[1]), is(2));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
        assertThat(reopener.getLastClosedCause(), instanceOf(IOException.class));
        assertThat(in.read(new byte[1]), is(-1));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
    }

    @Test
    public void readWithBytesAndOffsetResumesWithRuntimeException() throws Exception
    {
        TestingInputStream next = new TestingInputStream(
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                1,
                IO_EXCEPTION,
                2,
                -1);
        SimpleReopener reopener = new SimpleReopener(next);
        ResumableInputStream in = new ResumableInputStream(next, reopener);
        assertThat(in.read(new byte[1], 0, 1), is(1));
        assertThat(reopener.getLastOffset(), is(0L));
        assertThat(reopener.getLastClosedCause(), instanceOf(RuntimeException.class));
        assertThat(reopener.getReopenCount(), is(5));
        assertThat(in.read(new byte[1], 0, 1), is(2));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
        assertThat(reopener.getLastClosedCause(), instanceOf(IOException.class));
        assertThat(in.read(new byte[1], 0, 1), is(-1));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
    }

    @Test
    public void skipResumesWithRuntimeException() throws Exception
    {
        TestingInputStream next = new TestingInputStream(
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                RUNTIME_EXCEPTION,
                1,
                IO_EXCEPTION,
                2,
                -1);
        SimpleReopener reopener = new SimpleReopener(next);
        ResumableInputStream in = new ResumableInputStream(next, reopener);
        assertThat(in.skip(1), is(1L));
        assertThat(reopener.getLastOffset(), is(0L));
        assertThat(reopener.getLastClosedCause(), instanceOf(RuntimeException.class));
        assertThat(reopener.getReopenCount(), is(5));
        assertThat(in.skip(1), is(2L));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
        assertThat(reopener.getLastClosedCause(), instanceOf(IOException.class));
        assertThat(in.skip(1), is(-1L));
        assertThat(reopener.getLastOffset(), is(1L));
        assertThat(reopener.getReopenCount(), is(6));
    }
}
