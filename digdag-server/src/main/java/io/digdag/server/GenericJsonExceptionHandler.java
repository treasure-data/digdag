package io.digdag.server;

import java.util.HashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.core.MediaType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Provider
public abstract class GenericJsonExceptionHandler<T extends Throwable>
    implements ExceptionMapper<T>
{
    private final int statusCode;
    private final ObjectMapper messageMapper;

    public GenericJsonExceptionHandler(int statusCode)
    {
        this.statusCode = statusCode;
        this.messageMapper = new ObjectMapper();
    }

    public GenericJsonExceptionHandler(Response.Status status)
    {
        this(status.getStatusCode());
    }

    @Override
    public Response toResponse(T exception)
    {
        return toResponse(exception.getMessage());
    }

    public Response toResponse(String message)
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
}
