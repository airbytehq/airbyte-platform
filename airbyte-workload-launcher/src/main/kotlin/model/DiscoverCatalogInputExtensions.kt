/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import io.airbyte.workers.models.DiscoverCatalogInput
import java.util.UUID

fun DiscoverCatalogInput.getJobId(): String = this.jobRunConfig.jobId

fun DiscoverCatalogInput.getAttemptId(): Long = this.jobRunConfig.attemptId

fun DiscoverCatalogInput.usesCustomConnector(): Boolean = this.launcherConfig.isCustomConnector

fun DiscoverCatalogInput.getOrganizationId(): UUID = this.discoverCatalogInput.actorContext.organizationId
