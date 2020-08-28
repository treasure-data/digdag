package io.digdag.core.log;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public final class LogMarkers {
    public static final Marker UNEXPECTED_SERVER_ERROR = MarkerFactory.getMarker("UNEXPECTED_SERVER_ERROR");

    private LogMarkers() {}
}
