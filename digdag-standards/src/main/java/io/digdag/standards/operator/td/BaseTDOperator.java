package io.digdag.standards.operator.td;

import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpException;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.SecretProvider;
import io.digdag.util.RetryExecutor;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

import static io.digdag.util.RetryExecutor.retryExecutor;

public class BaseTDOperator
{
    private static final Logger logger = LoggerFactory.getLogger(BaseTDOperator.class);

    private static final int INITIAL_RETRY_WAIT = 500;
    private static final int MAX_RETRY_WAIT = 2000;
    private static final int MAX_RETRY_LIMIT = 3;
    public static final int AUTH_MAX_RETRY_LIMIT = 1;

    TDClient client;
    SecretProvider secrets;

    static final RetryExecutor defaultRetryExecutor = retryExecutor()
            .withInitialRetryWait(INITIAL_RETRY_WAIT)
            .withMaxRetryWait(MAX_RETRY_WAIT)
            .withRetryLimit(MAX_RETRY_LIMIT)
            .retryIf((exception) -> !isDeterministicClientException(exception));

    public void updateApikey(SecretProvider secrets)
    {
        String apikey = TDClientFactory.getApikey(secrets);
        client = client.withApiKey(apikey);
    }

    public RetryExecutor authenticatinRetryExecutor() {
        return retryExecutor()
                .withInitialRetryWait(INITIAL_RETRY_WAIT)
                .withMaxRetryWait(MAX_RETRY_WAIT)
                .withRetryLimit(AUTH_MAX_RETRY_LIMIT)
                .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                    logger.debug("apikey will be tried to update by retrying");
                    updateApikey(secrets);
                })
                .retryIf((exception) -> isAuthenticationErrorException(exception));
    }

    public <T> T callWithRetry(Callable<T> op)
    {
        try {
            return defaultRetryExecutor.run(() -> {
                try {
                    return authenticatinRetryExecutor().run(() -> op.call());
                } catch (RetryGiveupException ex) {
                    throw ThrowablesUtil.propagate(ex.getCause());
                }
            });
        }
        catch (RetryGiveupException ex) {
            throw ThrowablesUtil.propagate(ex.getCause());
        }
    }

    static boolean isDeterministicClientException(Exception ex)
    {
        if (ex instanceof TDClientHttpException) {
            int statusCode = ((TDClientHttpException) ex).getStatusCode();
            switch (statusCode) {
                case HttpStatus.TOO_MANY_REQUESTS_429:
                case HttpStatus.REQUEST_TIMEOUT_408:
                    return false;
                default:
                    // return true if 4xx
                    return statusCode >= 400 && statusCode < 500;
            }
        }
        return isFailedBeforeSendClientException(ex);
    }

    static boolean isFailedBeforeSendClientException(Exception ex)
    {
        if (ex instanceof TDClientException) {
            // failed before sending HTTP request or receiving HTTP response
            TDClientException.ErrorType errorType = ((TDClientException) ex).getErrorType();
            switch (errorType) {
                case INVALID_CONFIGURATION:  // failed to read td.conf, failed to pares integer in properties set to TDClientBuilder, etc.
                case INVALID_INPUT:          // early table name validation fails, failed to format request body in json, etc.
                    return true;
                default:
                    // other cases such as PROXY_AUTHENTICATION_FAILURE, SSL_ERROR, REQUEST_TIMEOUT, INTERRUPTED, etc.
                    break;  // pass-through
            }
        }
        return false;
    }

    static boolean isAuthenticationErrorException(Exception ex)
    {
        if (ex instanceof TDClientHttpException) {
            int statusCode = ((TDClientHttpException) ex).getStatusCode();
            switch (statusCode) {
                case HttpStatus.UNAUTHORIZED_401:
                    // This is not for authentication basically, but it may be 403 for auth token error. https://tools.ietf.org/html/rfc6750
                case HttpStatus.FORBIDDEN_403:
                    return true;
                default:
                    return false;
            }
        }
        else {
            return false;
        }
    }
}
