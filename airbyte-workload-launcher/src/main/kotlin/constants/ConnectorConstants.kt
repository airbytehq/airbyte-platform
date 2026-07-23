/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.constants

import java.util.UUID

// Iceberg connectors
val GCS_DATALAKE_DEFINITION_ID: UUID = UUID.fromString("8c8a2d3e-1b4f-4a9c-9e7d-6f5a4b3c2d1e")
const val GCS_DATALAKE_DOCKER_IMAGE: String = "airbyte/destination-gcs-data-lake"
val S3_DATALAKE_DEFINITION_ID: UUID = UUID.fromString("716ca874-520b-4902-9f80-9fad66754b89")
const val S3_DATALAKE_DOCKER_IMAGE: String = "airbyte/destination-s3-data-lake"
