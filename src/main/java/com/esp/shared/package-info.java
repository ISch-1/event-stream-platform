/**
 * Shared kernel: cross-cutting infrastructure (config, error handling, filters).
 * Nothing in {@code shared} may depend on any bounded context.
 */
@org.springframework.modulith.ApplicationModule(displayName = "Shared")
package com.esp.shared;
