package io.digdag.standards.operator.bq;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
interface GcpCredential
{
    GoogleCredential credential();

    Optional<String> projectId();
}
