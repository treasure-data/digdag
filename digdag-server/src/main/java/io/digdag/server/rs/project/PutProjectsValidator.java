package io.digdag.server.rs.project;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

import static java.util.Locale.ENGLISH;

public class PutProjectsValidator {
    public Instant validateAndGetScheduleFrom(String scheduleFromString)
    {
        if (scheduleFromString == null || scheduleFromString.isEmpty()) {
            return Instant.now();
        }
        else {
            try {
                return Instant.parse(scheduleFromString);
            }
            catch (DateTimeParseException ex) {
                throw new IllegalArgumentException("Invalid schedule_from= parameter format. Expected yyyy-MM-dd'T'HH:mm:ss'Z' format", ex);
            }
        }
    }

    public int validateAndGetContentLength(long contentLengthParam, int maxArchiveTotalSize)
    {
        if (contentLengthParam > maxArchiveTotalSize) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                    "Size of the uploaded archive file exceeds limit (%d bytes)", maxArchiveTotalSize));
        }
        return (int) contentLengthParam;
    }

    public void validateTarEntry(TarArchiveEntry entry, int maxArchiveFileSize)
    {
        if (entry.getSize() > maxArchiveFileSize) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                    "Size of a file in the archive exceeds limit (%d > %d bytes): %s",
                    entry.getSize(), maxArchiveFileSize, entry.getName()));
        }
    }

}
