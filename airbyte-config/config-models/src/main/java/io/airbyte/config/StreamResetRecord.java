/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import jakarta.annotation.Nullable;
import java.util.UUID;
import lombok.NonNull;

/**
 * A stream reset record is a reference to a stream that has a reset pending or running.
 **/
public record StreamResetRecord(@NonNull UUID connectionId,
                                @NonNull String streamName,
                                @Nullable String streamNamespace) {

}
