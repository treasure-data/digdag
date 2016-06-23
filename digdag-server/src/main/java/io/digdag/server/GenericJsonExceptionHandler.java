package io.digdag.server;

import java.util.HashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.core.MediaType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static io.digdag.client.DigdagClient.objectMapper;

@Provider
public abstract class GenericJsonExceptionHandler<T extends Throwable>
    implements ExceptionMapper<T>
{
    private static final ObjectMapper messageMapper = objectMapper();

    public static Response toResponse(Response.Status status, String message)
    {
        return toResponse(status.getStatusCode(), message);
    }

    public static Response toResponse(int statusCode, String message)
    {
        HashMap<String, Object> map = new HashMap<>();
        map.put("message", message);
        map.put("status", statusCode);

        try {
            return Response.status(statusCode)
                .type("application/json")
                .entity(messageMapper.writeValueAsString(map))
                .build();
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final int statusCode;

    public GenericJsonExceptionHandler(int statusCode)
    {
        this.statusCode = statusCode;
    }

    public GenericJsonExceptionHandler(Response.Status status)
    {
        this(status.getStatusCode());
    }

    @Override
    public Response toResponse(T exception)
    {
        return toResponse(statusCode, exception.getMessage());
    }

    public Response toResponse(String message)
    {
        return toResponse(statusCode, message);
    }
}
