package com.esp.shared.errors;

import java.time.Instant;
import java.util.List;

public record ApiError(
        int status,
        String code,
        String message,
        List<FieldError> details,
        String traceId,
        Instant timestamp
) {
    public record FieldError(String field, String message) {}
}
