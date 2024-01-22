/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

/**
 * Interface to avoid using a generic type for the micronaut injection.
 */
public interface CustomerIoEmailConfigFetcher extends ConfigFetcher<CustomerIoEmailConfig> {}
