/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

/**
 * Interface to avoid using a generic type for the micronaut injection.
 */
interface CustomerIoEmailConfigFetcher : ConfigFetcher<CustomerIoEmailConfig>
