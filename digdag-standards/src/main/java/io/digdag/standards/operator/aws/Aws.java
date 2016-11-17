package io.digdag.standards.operator.aws;

import com.amazonaws.AmazonServiceException;
import org.eclipse.jetty.http.HttpStatus;

class Aws
{
    static boolean isDeterministicException(AmazonServiceException ex)
    {
        int statusCode = ex.getStatusCode();
        switch (statusCode) {
            case HttpStatus.TOO_MANY_REQUESTS_429:
            case HttpStatus.REQUEST_TIMEOUT_408:
                return false;
            default:
                return statusCode >= 400 && statusCode < 500;
        }
    }
}
