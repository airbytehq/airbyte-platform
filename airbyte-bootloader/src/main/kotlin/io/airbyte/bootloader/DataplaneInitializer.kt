/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneClientCredentials
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneCredentialsService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.util.UUID

private val log = KotlinLogging.logger {}
internal const val SECRET_NAME_CLIENT_ID = "DATAPLANE_CLIENT_ID"
internal const val SECRET_NAME_CLIENT_SECRET = "DATAPLANE_CLIENT_SECRET"

@Singleton
class DataplaneInitializer(
  private val service: DataplaneService,
  private val groupService: DataplaneGroupService,
  private val dataplaneCredentialsService: DataplaneCredentialsService,
  private val k8sClient: KubernetesClient,
  private val edition: AirbyteEdition,
  @Property(name = "airbyte.auth.kubernetes-secret.name") private val secretName: String,
) {
  /**
   * Creates a dataplane if the following conditions are met
   * - not running on CLOUD
   * - a single dataplane group exists
   * - the dataplane group has no existing dataplanes
   */
  fun createDataplaneIfNotExists() {
    if (edition == AirbyteEdition.CLOUD) {
      log.info { "Dataplane registration is not supported for $edition." }
      return
    }

    val group =
      groupService
        .listDataplaneGroups(DEFAULT_ORGANIZATION_ID, false)
        .singleOrNull() ?: run {
        log.info { "Dataplane registration will be skipped due to an incorrect number of default dataplane groups." }
        return
      }

    val dataplane = createDataplane(group) ?: return
    createK8sSecret(dataplaneCredentialsService.createCredentials(dataplaneId = dataplane.id))
  }

  private fun createDataplane(group: DataplaneGroup): Dataplane? {
    val planes = service.listDataplanes(group.id, false)
    if (planes.isNotEmpty()) {
      log.info { "At least one dataplane for the group ${group.name} (${group.id}) already exists." }
      return null
    }

    // if were then we have one dataplane group and no dataplanes associated with it
    val dataplane: Dataplane =
      service.writeDataplane(
        Dataplane().apply {
          id = UUID.randomUUID()
          dataplaneGroupId = group.id
          name = group.name
          enabled = true
        },
      )
    log.info { "Successfully created dataplane ${dataplane.name} (${dataplane.id})" }

    return dataplane
  }

  /**
   * Creates or updates a k8s secret [secretName] with the [DataplaneClientCredentials].
   */
  private fun createK8sSecret(creds: DataplaneClientCredentials) {
    val secretData =
      mapOf(
        SECRET_NAME_CLIENT_ID to creds.clientId,
        SECRET_NAME_CLIENT_SECRET to creds.clientSecret,
      )
    K8sSecretHelper.createOrUpdateSecret(k8sClient, secretName, secretData)
  }
}
