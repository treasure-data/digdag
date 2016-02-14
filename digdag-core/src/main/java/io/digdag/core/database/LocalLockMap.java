package io.digdag.core.database;

import java.util.Set;
import java.util.HashSet;

public class LocalLockMap
{
    private static class Block
    {
        private final Set<Integer> set = new HashSet<>();

        public synchronized boolean tryLock(int queueId, long maxTimeout)
            throws InterruptedException
        {
            Integer i = queueId;

            if (set.add(i)) {
                return true;
            }
            else {
                wait(maxTimeout);
                if (set.add(i)) {
                    return true;
                }
                return false;
            }
        }

        public synchronized void unlock(int queueId)
        {
            set.remove(queueId);
            notifyAll();
        }
    }

    private final Block[] blocks = new Block[256];

    public LocalLockMap()
    {
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new Block();
        }
    }

    public boolean tryLock(int queueId, long maxTimeout)
        throws InterruptedException
    {
        return blocks[queueId % blocks.length].tryLock(queueId, maxTimeout);
    }

    public void unlock(int queueId)
    {
        blocks[queueId].unlock(queueId);
    }
}
