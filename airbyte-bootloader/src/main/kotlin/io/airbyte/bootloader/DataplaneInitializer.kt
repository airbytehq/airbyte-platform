/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.US_DATAPLANE_GROUP
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneClientCredentials
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
class DataplaneInitializer(
  private val service: DataplaneService,
  private val groupService: DataplaneGroupService,
  private val k8sClient: KubernetesClient,
  private val edition: AirbyteEdition,
  @Property(name = "airbyte.auth.kubernetes-secret.name") private val secretName: String,
  @Property(name = "airbyte.auth.dataplane-credentials.client-id-secret-key") private val clientIdSecretKey: String,
  @Property(name = "airbyte.auth.dataplane-credentials.client-secret-secret-key") private val clientSecretSecretKey: String,
  @Property(name = "airbyte.worker.job.kube.namespace") private val jobsNamespace: String,
) {
  /**
   * Creates a dataplane if the following conditions are met
   * - If running on cloud, a dataplane does not yet exist for the "US" dataplane group
   * - If running on OSS, a single dataplane group exists that does not have any existing dataplanes
   */
  fun createDataplaneIfNotExists() {
    val group =
      when (edition) {
        AirbyteEdition.CLOUD ->
          groupService.getDataplaneGroupByOrganizationIdAndName(
            DEFAULT_ORGANIZATION_ID,
            US_DATAPLANE_GROUP,
          )
        else -> {
          val groups = groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false)
          when {
            groups.size > 1 -> {
              log.info { "Skipping dataplane creation because multiple dataplane groups exist." }
              return
            }
            groups.isEmpty() -> throw IllegalStateException("No dataplane groups exist.")
            else -> groups.first()
          }
        }
      }

    val dataplane = createDataplane(group) ?: return
    createK8sSecret(dataplane)

    // Cloud puts dataplane pods in the jobs namespace, so we need to copy the secret containing
    // the dataplane credentials to the jobs namespace.
    if (edition == AirbyteEdition.CLOUD && jobsNamespace.isNotBlank()) {
      log.info { "Copying secret $secretName to jobs namespace" }
      K8sSecretHelper.copySecretToNamespace(
        k8sClient,
        secretName,
        k8sClient.namespace,
        jobsNamespace,
      )
    }
  }

  private fun createDataplane(group: DataplaneGroup): DataplaneWithServiceAccount? {
    val planes = service.listDataplanes(group.id, false)
    if (planes.isNotEmpty()) {
      log.info { "At least one dataplane for the group ${group.name} (${group.id}) already exists." }
      return null
    }

    // If we're here, then we have one dataplane group and no dataplanes associated with it
    val dataplaneAndServiceAccount =
      service.createDataplaneAndServiceAccount(
        dataplane =
          Dataplane().apply {
            id = UUID.randomUUID()
            dataplaneGroupId = group.id
            name = group.name
            enabled = true
          },
        instanceScope = true,
      )
    log.info { "Successfully created dataplane ${dataplaneAndServiceAccount.dataplane.name} (${dataplaneAndServiceAccount.dataplane.id})" }

    return dataplaneAndServiceAccount
  }

  /**
   * Creates or updates a k8s secret [secretName] with the [DataplaneClientCredentials].
   */
  private fun createK8sSecret(dataplaneWithServiceAccount: DataplaneWithServiceAccount) {
    val secretData =
      mapOf(
        clientIdSecretKey to dataplaneWithServiceAccount.serviceAccount.id.toString(),
        clientSecretSecretKey to dataplaneWithServiceAccount.serviceAccount.secret,
      )
    K8sSecretHelper.createOrUpdateSecret(k8sClient, secretName, secretData)
  }
}
