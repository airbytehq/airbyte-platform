/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.problems.throwable.generated.DataplaneNameAlreadyExistsProblem
import io.airbyte.config.Dataplane
import io.airbyte.data.services.DataplaneCredentialsService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.DataplaneTokenService
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import org.jooq.exception.DataAccessException
import java.util.UUID

@Singleton
open class DataplaneService(
  private val dataplaneDataService: DataplaneService,
  private val dataplaneCredentialsService: DataplaneCredentialsService,
  private val dataplaneTokenService: DataplaneTokenService,
) {
  fun createCredentials(dataplaneId: UUID): io.airbyte.config.DataplaneClientCredentials = dataplaneCredentialsService.createCredentials(dataplaneId)

  fun listDataplanes(dataplaneGroupId: UUID): List<Dataplane> = dataplaneDataService.listDataplanes(dataplaneGroupId, false)

  fun updateDataplane(
    dataplaneId: UUID,
    updatedName: String?,
    updatedEnabled: Boolean?,
  ): Dataplane {
    val existingDataplane = dataplaneDataService.getDataplane(dataplaneId)

    val updatedDataplane =
      existingDataplane.apply {
        updatedName?.let { name = it }
        updatedEnabled?.let { enabled = it }
      }

    return writeDataplane(updatedDataplane)
  }

  @Transactional("config")
  open fun deleteDataplane(dataplaneId: UUID): Dataplane {
    val existingDataplane = dataplaneDataService.getDataplane(dataplaneId)
    val tombstonedDataplane =
      existingDataplane.apply {
        tombstone = true
      }

    dataplaneCredentialsService.listCredentialsByDataplaneId(existingDataplane.id).map { dataplaneCredentialsService.deleteCredentials(it.id) }
    return writeDataplane(tombstonedDataplane)
  }

  fun getToken(
    clientId: String,
    clientSecret: String,
  ): String = dataplaneTokenService.getToken(clientId, clientSecret)

  fun getDataplaneFromClientId(clientId: String): Dataplane {
    val dataplaneId = dataplaneCredentialsService.getDataplaneId(clientId)
    return dataplaneDataService.getDataplane(dataplaneId)
  }

  fun writeDataplane(dataplane: Dataplane): Dataplane {
    try {
      return dataplaneDataService.writeDataplane(dataplane)
    } catch (e: DataAccessException) {
      if (e.message?.contains("duplicate key value violates unique constraint") == true &&
        e.message?.contains("dataplane_dataplane_group_id_name_key") == true
      ) {
        throw DataplaneNameAlreadyExistsProblem()
      }
      throw e
    }
  }
}
