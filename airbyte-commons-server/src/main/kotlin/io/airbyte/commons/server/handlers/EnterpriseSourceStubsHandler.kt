/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.model.generated.EnterpriseSourceStub
import io.airbyte.api.model.generated.EnterpriseSourceStubsReadList
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.persistence.job.WorkspaceHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.IOException
import java.time.Duration
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
open class EnterpriseSourceStubsHandler(
  @Value("\${airbyte.connector-registry.enterprise.enterprise-source-stubs-url}")
  private val enterpriseSourceStubsUrl: String,
  @Value("\${airbyte.connector-registry.remote.timeout-ms}")
  private val remoteTimeoutMs: Long,
  private val workspaceHelper: WorkspaceHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
) {
  private val okHttpClient: OkHttpClient =
    OkHttpClient
      .Builder()
      .callTimeout(Duration.ofMillis(remoteTimeoutMs))
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

  @Throws(IOException::class)
  private fun getRegistryEnterpriseStubs(): List<RegistryEnterpriseStub> {
    try {
      val request =
        Request
          .Builder()
          .url(enterpriseSourceStubsUrl)
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

  fun listEnterpriseSourceStubs(): EnterpriseSourceStubsReadList {
    try {
      val registryStubs = getRegistryEnterpriseStubs()
      return EnterpriseSourceStubsReadList().apply {
        enterpriseSourceStubs =
          registryStubs
            .filter {
              it.type == ENTERPRISE_SOURCE_TYPE
            }.map {
              EnterpriseSourceStub().apply {
                id = it.id
                name = it.name
                url = it.url
                icon = it.icon
                label = it.label
                type = it.type
                definitionId = it.definitionId
              }
            }
      }
    } catch (error: Exception) {
      logger.error("Unexpected error fetching enterprise sources", error)
      return EnterpriseSourceStubsReadList()
    }
  }

  fun listEnterpriseSourceStubsForWorkspace(workspaceId: UUID): EnterpriseSourceStubsReadList {
    try {
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      val sourceStubs = listEnterpriseSourceStubs().enterpriseSourceStubs

      // only return stubs for connectors that the org is NOT entitled to
      return EnterpriseSourceStubsReadList().apply {
        enterpriseSourceStubs =
          sourceStubs.filter {
            if (it.definitionId == null) {
              return@filter true
            }

            val isEntitled =
              try {
                licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.SOURCE_CONNECTOR, UUID.fromString(it.definitionId!!))
              } catch (e: ConfigNotFoundException) {
                false
              }

            !isEntitled
          }
      }
    } catch (error: Exception) {
      logger.error("Unexpected error fetching enterprise sources", error)
      return EnterpriseSourceStubsReadList()
    }
  }

  companion object {
    private const val ENTERPRISE_SOURCE_TYPE = "enterprise_source"
  }
}
