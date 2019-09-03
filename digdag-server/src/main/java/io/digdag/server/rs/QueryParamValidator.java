package io.digdag.server.rs;

import com.google.common.base.Optional;

public class QueryParamValidator
{
    public static int validatePageSize(Optional<Integer> pageSize, int maxPageSize, int defaultPageSize)
            throws IllegalArgumentException
    {
        if (!pageSize.isPresent()) { return defaultPageSize; }

        int pageSizeValue = pageSize.get().intValue();
        if (pageSizeValue > maxPageSize) {
            String message = "Your specified page_size is " + pageSize + ", " +
                    "but it is larger than MAX_PAGE_SIZE: " + maxPageSize + ". " +
                    "You must specify page_size with a number which is smaller than " + maxPageSize + ".";

            // This error results 400 response
            throw new IllegalArgumentException(message);
        }
        else {
            return pageSizeValue;
        }
    }

    public static int validatePageNumber(Optional<Integer> pageNumber, int defaultPageNumber)
    {
        return pageNumber.or(defaultPageNumber);
    }
}
