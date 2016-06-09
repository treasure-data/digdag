package utils;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;

public class NopDispatcher
        extends Dispatcher
{
    @Override
    public MockResponse dispatch(RecordedRequest request)
            throws InterruptedException
    {
        return new MockResponse().setResponseCode(200);
    }
}
