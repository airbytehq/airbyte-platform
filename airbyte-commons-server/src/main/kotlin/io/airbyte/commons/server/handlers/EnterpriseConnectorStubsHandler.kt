/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.model.generated.EnterpriseConnectorStub
import io.airbyte.api.model.generated.EnterpriseConnectorStubsReadList
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.ActorType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.micronaut.runtime.AirbyteConnectorRegistryConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.IOException
import java.time.Duration
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
open class EnterpriseConnectorStubsHandler(
  private val airbyteConnectorRegistryConfig: AirbyteConnectorRegistryConfig,
  private val workspaceHelper: WorkspaceHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
) {
  private val okHttpClient: OkHttpClient =
    OkHttpClient
      .Builder()
      .callTimeout(Duration.ofMillis(airbyteConnectorRegistryConfig.remote.timeoutMs))
      .build()

  @JsonIgnoreProperties(ignoreUnknown = true)
  data class RegistryEnterpriseStub(
    val id: String? = null,
    val name: String? = null,
    val url: String? = null,
    val icon: String? = null,
    val label: String? = null,
    val type: String? = null,
    val definitionId: String? = null,
  )

  private fun getRegistryEnterpriseStubs(): List<RegistryEnterpriseStub> {
    try {
      val request =
        Request
          .Builder()
          .url(airbyteConnectorRegistryConfig.enterprise.enterpriseStubsUrl)
          .build()

      okHttpClient.newCall(request).execute().use { response ->
        if (!response.isSuccessful) throw IOException("Unexpected code ${response.code}")

        val jsonResponse = response.body?.string() ?: throw IOException("Empty response body")

        val objectMapper = jacksonObjectMapper()
        val typeReference = object : TypeReference<List<RegistryEnterpriseStub>>() {}
        return objectMapper.readValue(jsonResponse, typeReference)
      }
    } catch (error: IOException) {
      logger.error(error) {
        "Encountered an HTTP error fetching enterprise connectors. Message: ${error.message}"
      }
      throw IOException("HTTP error fetching enterprise sources", error)
    } catch (error: Exception) {
      logger.error(error) { "Unexpected error fetching enterprise sources" }
      throw IOException("Encountered an unexpected error fetching enterprise sources", error)
    }
  }

  private fun listEnterpriseConnectorStubs(actorType: ActorType): List<EnterpriseConnectorStub> {
    try {
      val registryStubs = getRegistryEnterpriseStubs()
      val expectedType =
        when (actorType) {
          ActorType.SOURCE -> ENTERPRISE_SOURCE_TYPE
          ActorType.DESTINATION -> ENTERPRISE_DESTINATION_TYPE
        }

      return registryStubs
        .filter { it.type == expectedType }
        .map {
          EnterpriseConnectorStub().apply {
            id = it.id
            name = it.name
            url = it.url
            icon = it.icon
            label = it.label
            type = it.type
            definitionId = it.definitionId
          }
        }
    } catch (error: Exception) {
      logger.error(error) { "Unexpected error fetching enterprise ${actorType}s" }
      return listOf()
    }
  }

  private fun listEnterpriseConnectorStubsForWorkspace(
    workspaceId: UUID,
    actorType: ActorType,
  ): EnterpriseConnectorStubsReadList {
    try {
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      val connectorStubs = listEnterpriseConnectorStubs(actorType)
      val entitlement =
        when (actorType) {
          ActorType.SOURCE -> Entitlement.SOURCE_CONNECTOR
          ActorType.DESTINATION -> Entitlement.DESTINATION_CONNECTOR
        }

      // only return stubs for connectors that the org is NOT entitled to
      return EnterpriseConnectorStubsReadList().apply {
        enterpriseConnectorStubs =
          connectorStubs.filter {
            if (it.definitionId == null) {
              return@filter true
            }

            val isEntitled =
              try {
                licenseEntitlementChecker.checkEntitlement(organizationId, entitlement, UUID.fromString(it.definitionId!!))
              } catch (_: ConfigNotFoundException) {
                false
              }

            !isEntitled
          }
      }
    } catch (error: Exception) {
      logger.error(error) { "Unexpected error fetching enterprise ${actorType}s" }
      return EnterpriseConnectorStubsReadList()
    }
  }

  fun listEnterpriseSourceStubs(): EnterpriseConnectorStubsReadList =
    EnterpriseConnectorStubsReadList().enterpriseConnectorStubs(listEnterpriseConnectorStubs(ActorType.SOURCE))

  fun listEnterpriseSourceStubsForWorkspace(workspaceId: UUID): EnterpriseConnectorStubsReadList =
    listEnterpriseConnectorStubsForWorkspace(workspaceId, ActorType.SOURCE)

  fun listEnterpriseDestinationStubsForWorkspace(workspaceId: UUID): EnterpriseConnectorStubsReadList =
    listEnterpriseConnectorStubsForWorkspace(workspaceId, ActorType.DESTINATION)

  companion object {
    private const val ENTERPRISE_SOURCE_TYPE = "enterprise_source"
    private const val ENTERPRISE_DESTINATION_TYPE = "enterprise_destination"
  }
}
