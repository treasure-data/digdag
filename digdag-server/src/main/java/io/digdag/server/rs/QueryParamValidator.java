package io.digdag.server.rs;

public class QueryParamValidator
{
    public static int validatePageSize(int pageSize, int maxPageSize)
            throws IllegalArgumentException
    {
        if (pageSize > maxPageSize) {
            String message = "Your specified page_size is " + pageSize + ", " +
                    "but it is larger than MAX_PAGE_SIZE: " + maxPageSize + ". " +
                    "You must specify page_size with a number which is smaller than " + maxPageSize + ".";

            // This error results 400 response
            throw new IllegalArgumentException(message);
        }
        else {
            return pageSize;
        }
    }
}
