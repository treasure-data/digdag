package io.digdag.util;

import java.util.function.Predicate;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class RetryExecutor
{
    public static RetryExecutor retryExecutor()
    {
        return new RetryExecutor();
    }

    public static class RetryGiveupException
            extends ExecutionException
    {
        public RetryGiveupException(String message, Exception cause)
        {
            super(cause);
        }

        public RetryGiveupException(Exception cause)
        {
            super(cause);
        }

        public Exception getCause()
        {
            return (Exception) super.getCause();
        }
    }

    public interface RetryPredicate
            extends Predicate<Exception>
    { }

    public interface RetryAction
    {
        void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
            throws RetryGiveupException;
    }

    public interface GiveupAction
    {
        void onGiveup(Exception firstException, Exception lastException)
            throws RetryGiveupException;
    }

    private final int retryLimit;
    private final int initialRetryWait;
    private final int maxRetryWait;
    private final double waitGrowRate;
    private final RetryPredicate retryPredicate;
    private final RetryAction retryAction;
    private final GiveupAction giveupAction;

    private RetryExecutor()
    {
        this(5, 1000, 30 * 60 * 1000, 3.0, null, null, null);
    }

    private RetryExecutor(int retryLimit, int initialRetryWait, int maxRetryWait, double waitGrowRate,
            RetryPredicate retryPredicate, RetryAction retryAction, GiveupAction giveupAction)
    {
        this.retryLimit = retryLimit;
        this.initialRetryWait = initialRetryWait;
        this.maxRetryWait = maxRetryWait;
        this.waitGrowRate = waitGrowRate;
        this.retryPredicate = retryPredicate;
        this.retryAction = retryAction;
        this.giveupAction = giveupAction;
    }

    public RetryExecutor withRetryLimit(int count)
    {
        return new RetryExecutor(
                count, initialRetryWait, maxRetryWait, waitGrowRate,
                retryPredicate, retryAction, giveupAction);
    }

    public RetryExecutor withInitialRetryWait(int msec)
    {
        return new RetryExecutor(
                retryLimit, msec, maxRetryWait, waitGrowRate,
                retryPredicate, retryAction, giveupAction);
    }

    public RetryExecutor withMaxRetryWait(int msec)
    {
        return new RetryExecutor(
                retryLimit, initialRetryWait, msec, waitGrowRate,
                retryPredicate, retryAction, giveupAction);
    }

    public RetryExecutor withWaitGrowRate(double rate)
    {
        return new RetryExecutor(
                retryLimit, initialRetryWait, maxRetryWait, rate,
                retryPredicate, retryAction, giveupAction);
    }

    public RetryExecutor retryIf(RetryPredicate function)
    {
        return new RetryExecutor(
                retryLimit, initialRetryWait, maxRetryWait, waitGrowRate,
                function, retryAction, giveupAction);
    }

    public RetryExecutor onRetry(RetryAction function)
    {
        return new RetryExecutor(
                retryLimit, initialRetryWait, maxRetryWait, waitGrowRate,
                retryPredicate, function, giveupAction);
    }

    public RetryExecutor onGiveup(GiveupAction function)
    {
        return new RetryExecutor(
                retryLimit, initialRetryWait, maxRetryWait, waitGrowRate,
                retryPredicate, retryAction, function);
    }

    public <T> T runInterruptible(Callable<T> op)
            throws InterruptedException, RetryGiveupException
    {
        return run(op, true);
    }

    public void runInterruptible(Runnable op)
            throws InterruptedException, RetryGiveupException
    {
        runInterruptible(() -> {
            op.run();
            return (Void) null;
        });
    }

    public <T> T run(Callable<T> op)
            throws RetryGiveupException
    {
        try {
            return run(op, false);
        } catch (InterruptedException ex) {
            throw new RetryGiveupException("Unexpected interruption", ex);
        }
    }

    public void run(Runnable op)
            throws RetryGiveupException
    {
        run(() -> {
            op.run();
            return (Void) null;
        });
    }

    private <T> T run(Callable<T> op, boolean interruptible)
            throws InterruptedException, RetryGiveupException
    {
        int retryCount = 0;

        Exception firstException = null;

        while(true) {
            try {
                return op.call();
            } catch (Exception exception) {
                if (firstException == null) {
                    firstException = exception;
                }
                if (retryCount >= retryLimit || retryPredicate == null || !retryPredicate.test(exception)) {
                    if (giveupAction != null) {
                        giveupAction.onGiveup(firstException, exception);
                    }
                    throw new RetryGiveupException(firstException);
                }

                // exponential back-off with hard limit
                int retryWait = (int) Math.min((double) maxRetryWait, initialRetryWait * Math.pow(waitGrowRate, retryCount));

                retryCount++;
                if (retryAction != null) {
                    retryAction.onRetry(exception, retryCount, retryLimit, retryWait);
                }

                try {
                    Thread.sleep(retryWait);
                } catch (InterruptedException ex) {
                    if (interruptible) {
                        throw ex;
                    }
                }
            }
        }
    }
}

