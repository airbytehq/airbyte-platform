/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import java.util.UUID

/**
 * Our acceptance tests are large enough that having a configuration object that sets sane defaults
 * ends up reducing lines of code per test. It also reduces mistakes in tests where we run more
 * stuff in a connection than we really need to.
 */
class TestConnectionCreate
/**
   * Constructor intentionally private to force developers to use the builder to enforce sane
   * defaults.
   *
   * @param srcId - source id
   * @param dstId - destination id
   * @param configuredCatalog - configured catalog
   * @param catalogId - discovered catalog id
   * @param scheduleType - schedule type
   * @param scheduleData - schedule data
   * @param operationIds - operation ids (including normalization)
   * @param dataplaneGroupId - dataplaneGroupId
   * @param nameSuffix - optional suffix that will get appended to the connection name.
   */
  private constructor(
    @JvmField val srcId: UUID,
    @JvmField val dstId: UUID,
    @JvmField val configuredCatalog: AirbyteCatalog,
    @JvmField val catalogId: UUID?,
    @JvmField val scheduleType: ConnectionScheduleType?,
    @JvmField val scheduleData: ConnectionScheduleData?,
    @JvmField val operationIds: List<UUID>,
    @JvmField val dataplaneGroupId: UUID?,
    @JvmField val nameSuffix: String?,
  ) {
    /**
     * Builder class for TestConnectionCreate. The EXCLUSIVE way of creating a TestConnectionCreate.
     */
    class Builder(
      private val srcId: UUID,
      private val dstId: UUID,
      private val configuredCatalog: AirbyteCatalog,
      private val catalogId: UUID?,
      private var dataplaneGroupId: UUID?,
    ) {
      private var normalizationOperationId: UUID? = null
      private var additionalOperationIds: List<UUID>
      private var scheduleType: ConnectionScheduleType? = null
      private var scheduleData: ConnectionScheduleData? = null

      private var nameSuffix: String? = null

      /**
       * Builder for TestConnectionCreate. Contains all the required and non-nullable fields needed to
       * create a TestConnectionCreate.
       *
       * @param srcId - source id
       * @param dstId - destination id
       * @param configuredCatalog - configured catalog
       * @param catalogId - discovered catalog id
       */
      init {
        additionalOperationIds = emptyList()
        scheduleType = ConnectionScheduleType.MANUAL
      }

      /**
       * Set the normalization operation id. This is separated out from the additional operation ids
       * setter because we are almost always just setting the one normalization id. If not set, defaults
       * to no normalization.
       *
       * @param normalizationOperationId - normalization operation id
       * @return the builder
       */
      fun setNormalizationOperationId(normalizationOperationId: UUID?): Builder {
        this.normalizationOperationId = normalizationOperationId
        return this
      }

      /**
       * Setting operation ids. This only used when the sync has operations in addition to normalization.
       * Generally used rarely. If not set, defaults to no operations.
       *
       * @param additionalOperationIds - additional operation ids
       * @return the builder
       */
      fun setAdditionalOperationIds(additionalOperationIds: List<UUID>?): Builder {
        this.additionalOperationIds = ArrayList(additionalOperationIds ?: emptyList())
        return this
      }

      /**
       * Set the schedule for a connection. Defaults to MANUAL.
       *
       * @param scheduleType - schedule type
       * @param scheduleData - schedule data
       * @return the builder
       */
      fun setSchedule(
        scheduleType: ConnectionScheduleType?,
        scheduleData: ConnectionScheduleData?,
      ): Builder {
        this.scheduleType = scheduleType
        this.scheduleData = scheduleData
        return this
      }

      /**
       * Set the dataplaneGroupId for a connection.
       *
       * @param dataplaneGroupId - dataplaneGroupId
       * @return the builder
       */
      fun setDataplaneGroupId(dataplaneGroupId: UUID): Builder {
        this.dataplaneGroupId = dataplaneGroupId
        return this
      }

      /**
       * Set the name suffix for a connection. For use if having a human-readable connection name is
       * useful Defaults to no suffix.
       *
       * @param nameSuffix - suffix
       * @return the builder
       */
      fun setNameSuffix(nameSuffix: String?): Builder {
        this.nameSuffix = nameSuffix
        return this
      }

      /**
       * Build the TestConnectionCreate object.
       *
       * @return TestConnectionCreate
       */
      fun build(): TestConnectionCreate {
        val operationIds: MutableList<UUID> = ArrayList()
        if (normalizationOperationId != null) {
          operationIds.add(normalizationOperationId!!)
        }
        operationIds.addAll(additionalOperationIds)

        return TestConnectionCreate(
          srcId,
          dstId,
          configuredCatalog,
          catalogId,
          scheduleType,
          scheduleData,
          operationIds,
          dataplaneGroupId,
          nameSuffix,
        )
      }
    }
  }
