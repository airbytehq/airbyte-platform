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
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
class DataplaneInitializer(
  private val dataplaneService: DataplaneService,
  private val groupService: DataplaneGroupService,
  private val serviceAccountsService: ServiceAccountsService,
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
    // If the secret contains credentials that match an existing dataplane + service account,
    // then do nothing.
    if (isValidServiceAccount()) {
      return
    }

    // We don't have valid crendentials stored in the secret. This could be because:
    // - this is the first install, and the secret and dataplane have never been created.
    // - the secret was deleted.
    // - the dataplane was deleted.
    // - the credentials just don't match.

    // Get the dataplane group.
    // If multiple groups exist, the function returns null, and we skip the rest of this init.
    // If no groups exist, the function throws and we fail.
    val group = getOneGroup() ?: return

    // Create the dataplane and store the secret.
    val dataplane = createDataplane(group)
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

  private fun getOneGroup(): DataplaneGroup? =
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
            null
          }
          groups.isEmpty() -> throw IllegalStateException("No dataplane groups exist.")
          else -> groups.first()
        }
      }
    }

  private fun isValidServiceAccount(): Boolean {
    // Get the secret that stores the credentials.
    val secret = K8sSecretHelper.getAndDecodeSecret(k8sClient, secretName)

    // Check to see whether the secret contains credentials that match an existing service account.
    val clientId = secret?.get(clientIdSecretKey)
    val clientSecret = secret?.get(clientSecretSecretKey)

    if (clientId == null || clientSecret == null) {
      return false
    }

    try {
      serviceAccountsService.getAndVerify(UUID.fromString(clientId), clientSecret)
      return true
    } catch (e: Exception) {
      return false
    }
  }

  private fun createDataplane(group: DataplaneGroup): DataplaneWithServiceAccount {
    val dataplaneAndServiceAccount =
      dataplaneService.createDataplaneAndServiceAccount(
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
