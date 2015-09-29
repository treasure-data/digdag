package io.digdag.core;

import java.util.PrimitiveIterator;
import java.util.NoSuchElementException;
import java.math.RoundingMode;
import com.google.common.math.IntMath;

public class TreeCode
{
    private TreeCode() { }

    public static long getRootCode()
    {
        return 1L;
    }

    public static Encoder encoder()
    {
        return new Encoder();
    }

    public static Decoder decoder(long x63)
    {
        return new Decoder(x63);
    }

    public static class Encoder
    {
        private int off = 63;
        private long x63 = 0;

        public void add(int t)
        {
            int x = t + 1;
            int e = IntMath.log2(x, RoundingMode.FLOOR);
            long d = x - (1L << e);
            long v = (((1 << e) - 1) << (1 + e)) | d;
            int bits = 2 * e + 1;

            if (off - bits < 6) {
                throw new IllegalStateException("TreeCode overflow");
            }
            off -= bits;
            x63 |= (v << off);
        }

        public long get()
        {
            return x63 | (63 - off);
        }
    }

    public static class Decoder
            implements PrimitiveIterator.OfInt
    {
        private int off;
        private int max;
        private final long src;

        public Decoder(long c)
        {
            this.off = 0;
            this.max = (int) (c & ((1 << 6) - 1));
            this.src = c & ~((1 << 6) - 1);
        }

        public boolean hasNext()
        {
            return max > off;
        }

        public int nextInt()
        {
            if (max <= off) {
                throw new NoSuchElementException();
            }
            long x63 = (src << off) & 0x7fffffffffffffffL;
            int e = Long.numberOfLeadingZeros(x63 ^ 0x7fffffffffffffffL) - 1;
            int d = ((int)(x63 >> (62 - e*2))) & ((1 << e) - 1);
            off += (e * 2 + 1);
            int x = (1 << e) + d;
            return x - 1;
        }
    }
}
