/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.commons.enums.convertTo
import io.airbyte.api.model.generated.JobTypeResourceLimit as InternalJobTypeResourceLimit
import io.airbyte.api.model.generated.ResourceRequirements as InternalResourceRequirements
import io.airbyte.api.model.generated.ScopedResourceRequirements as InternalScopedResourceRequirements
import io.airbyte.publicApi.server.generated.models.JobTypeResourceLimit as PublicJobTypeResourceLimit
import io.airbyte.publicApi.server.generated.models.ResourceRequirements as PublicResourceRequirements
import io.airbyte.publicApi.server.generated.models.ScopedResourceRequirements as PublicScopedResourceRequirements

/**
 * Extension function for converting the non-public [io.airbyte.api.model.generated.ScopedResourceRequirements] type to the public
 * [io.airbyte.publicApi.server.generated.models.ScopedResourceRequirements] type.
 *
 * @receiver [io.airbyte.api.model.generated.ScopedResourceRequirements] the non-public instance
 * @return the public instance [io.airbyte.publicApi.server.generated.models.ScopedResourceRequirements]
 */
internal fun InternalScopedResourceRequirements.toPublic(): PublicScopedResourceRequirements =
  PublicScopedResourceRequirements(
    default = this@toPublic.default.toPublic(),
    jobSpecific = this@toPublic.jobSpecific.map { it.toPublic() },
  )

/**
 * Extension function for converting the public [io.airbyte.publicApi.server.generated.models.ScopedResourceRequirements] type to the non-public
 * [io.airbyte.api.model.generated.ScopedResourceRequirements] type.
 *
 * @receiver [io.airbyte.publicApi.server.generated.models.ScopedResourceRequirements] the non-public instance
 * @return the public instance [io.airbyte.api.model.generated.ScopedResourceRequirements]
 */
internal fun PublicScopedResourceRequirements.toInternal(): InternalScopedResourceRequirements =
  InternalScopedResourceRequirements()
    ._default(this.default?.toInternal())
    .jobSpecific(this.jobSpecific?.map { it.toInternal() })

private fun InternalResourceRequirements.toPublic(): PublicResourceRequirements =
  PublicResourceRequirements(
    cpuRequest = this@toPublic.cpuRequest,
    cpuLimit = this@toPublic.cpuLimit,
    memoryRequest = this@toPublic.memoryRequest,
    memoryLimit = this@toPublic.memoryLimit,
    ephemeralStorageRequest = this@toPublic.ephemeralStorageRequest,
    ephemeralStorageLimit = this@toPublic.ephemeralStorageLimit,
  )

private fun InternalJobTypeResourceLimit.toPublic(): PublicJobTypeResourceLimit =
  PublicJobTypeResourceLimit(
    jobType = this.jobType.convertTo(),
    resourceRequirements = this.resourceRequirements.toPublic(),
  )

private fun PublicResourceRequirements.toInternal(): InternalResourceRequirements =
  InternalResourceRequirements()
    .cpuRequest(this.cpuRequest)
    .cpuLimit(this.cpuLimit)
    .memoryRequest(this.memoryRequest)
    .memoryLimit(this.memoryLimit)
    .ephemeralStorageRequest(this.ephemeralStorageRequest)
    .ephemeralStorageLimit(this.ephemeralStorageLimit)

private fun PublicJobTypeResourceLimit.toInternal(): InternalJobTypeResourceLimit =
  InternalJobTypeResourceLimit()
    .jobType(this.jobType.convertTo())
    .resourceRequirements(this.resourceRequirements.toInternal())
