package com.esp.ingest.api;

import java.util.Map;
import java.util.UUID;

import jakarta.validation.constraints.*;

/**
 * Public wire format. Loose on purpose: producers may not know the Avro
 * schema exists. The service is the boundary that upconverts to Avro.
 */
public record IngestRequest(
        UUID                 eventId,                    // optional — server assigns if missing
        @NotBlank @Size(max = 100) String type,
        @NotBlank @Size(max = 200) String userId,
        Long                 eventTimeEpochMs,           // optional — server stamps now() if missing
        Map<String, String>  payload,                    // free-form, flat
        @Size(max = 32)      String source
) {}
