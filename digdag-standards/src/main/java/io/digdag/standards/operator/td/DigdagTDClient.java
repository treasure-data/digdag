package io.digdag.standards.operator.td;

import com.treasuredata.client.HttpStatus;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpException;
import io.digdag.spi.SecretProvider;
import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.util.RetryExecutor.retryExecutor;

public class DigdagTDClient
{
    private final TDClient client;
    private final SecretProvider secrets;

    private static final int INITIAL_RETRY_WAIT = 500;
    private static final int MAX_RETRY_WAIT = 2000;
    private static final int MAX_RETRY_LIMIT = 1;

    private static Logger logger = LoggerFactory.getLogger(DigdagTDClient.class);

    DigdagTDClient(TDClient client, SecretProvider secrets)
    {
        this.client = client;
        this.secrets = secrets;
    }

    private RetryExecutor defaultRetryExecutor(){
        return retryExecutor()
            .withInitialRetryWait(INITIAL_RETRY_WAIT)
            .withMaxRetryWait(MAX_RETRY_WAIT)
            .withRetryLimit(MAX_RETRY_LIMIT)
            .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                if (exception instanceof TDClientHttpException) {
                    if (isAuthenticationErrorException(exception)) {
                        logger.warn("apikey will be tried to update by retrying");
                        updateApikey(secrets);
                    }
                }
            })
            .retryIf((exception) -> isAuthenticationErrorException(exception));
    }


    private void createDatabase(String databaseName)
            throws RetryExecutor.RetryGiveupException
    {
        defaultRetryExecutor().run(() -> client.createDatabase(databaseName));
    }

    private boolean isAuthenticationErrorException(Exception ex)
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
        } else {
            return false;
        }
    }

}
