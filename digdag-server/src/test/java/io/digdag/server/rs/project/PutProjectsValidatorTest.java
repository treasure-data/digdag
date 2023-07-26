package io.digdag.server.rs.project;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;
import static io.digdag.commons.AssertUtil.assertException;

@RunWith(MockitoJUnitRunner.class)
public class PutProjectsValidatorTest {
    private PutProjectsValidator validator;

    @Mock TarArchiveEntry archiveEntry;

    @Before
    public void setUp()
    {
        validator = new PutProjectsValidator();
    }

    @Test
    public void testValidateAndGetScheduleFrom()
    {
        Instant now = Instant.now();
        {
            Instant ret = validator.validateAndGetScheduleFrom(null);
            assertThat(ret, notNullValue());
            assertThat(ret.toEpochMilli(), greaterThanOrEqualTo(now.toEpochMilli()));
        }
        {
            Instant ret = validator.validateAndGetScheduleFrom("");
            assertThat(ret, notNullValue());
            assertThat(ret.toEpochMilli(), greaterThanOrEqualTo(now.toEpochMilli()));
        }
        {
            Instant ret = validator.validateAndGetScheduleFrom("2022-04-26T01:23:45Z");
            assertThat(ret, notNullValue());
            assertThat(ret.toEpochMilli(), greaterThanOrEqualTo(Instant.parse("2022-04-26T01:23:45Z").toEpochMilli()));
        }
        {
            assertException(() -> validator.validateAndGetScheduleFrom("2022-04-26"), IllegalArgumentException.class, "Invalid format must fail");
        }
    }

    @Test
    public void testValidateAndGetContentLength()
    {
        final int MAX_CONTENT_LENGTH = 65534;
        {
            assertThat(validator.validateAndGetContentLength(0, MAX_CONTENT_LENGTH), is(0));
        }
        {
            assertThat(validator.validateAndGetContentLength(MAX_CONTENT_LENGTH, MAX_CONTENT_LENGTH), is(MAX_CONTENT_LENGTH));
        }
        {
            assertException(() -> validator.validateAndGetContentLength(MAX_CONTENT_LENGTH + 1, MAX_CONTENT_LENGTH), IllegalArgumentException.class, "Exceeded size must fail");
        }
    }

    @Test
    public void testValidateTarEntry()
    {
        final int MAX_ARCHIVE_FILE_SIZE = 10*1024*1024;
        {
            when(archiveEntry.getSize()).thenReturn(0L);
            validator.validateTarEntry(archiveEntry, MAX_ARCHIVE_FILE_SIZE);
        }
        {
            when(archiveEntry.getSize()).thenReturn(MAX_ARCHIVE_FILE_SIZE + 0L);
            validator.validateTarEntry(archiveEntry, MAX_ARCHIVE_FILE_SIZE);
        }
        {
            when(archiveEntry.getSize()).thenReturn(MAX_ARCHIVE_FILE_SIZE + 1L);
            assertException(() -> validator.validateTarEntry(archiveEntry, MAX_ARCHIVE_FILE_SIZE), IllegalArgumentException.class, "Exceeded size must fail");
        }
    }
}
