package io.digdag.server;

import java.util.Objects;
import javax.ws.rs.container.ContainerRequestContext;

public interface Authenticator
{
    public static class Result
    {
        private final int siteId;
        private final String errorMessage;

        public static Result accept(int siteId)
        {
            return new Result(siteId);
        }

        public static Result reject(String message)
        {
            return new Result(message);
        }

        public Result(int siteId)
        {
            this.siteId = siteId;
            this.errorMessage = null;
        }

        public Result(String errorMessage)
        {
            this.siteId = 0;
            this.errorMessage = Objects.requireNonNull(errorMessage);
        }

        public boolean isAccepted()
        {
            return errorMessage == null;
        }

        public int getSiteId()
        {
            return siteId;
        }

        public String getErrorMessage()
        {
            return errorMessage;
        }
    }

    Result authenticate(ContainerRequestContext requestContext);
}
