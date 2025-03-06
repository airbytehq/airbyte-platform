/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.Geography;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Our acceptance tests are large enough that having a configuration object that sets sane defaults
 * ends up reducing lines of code per test. It also reduces mistakes in tests where we run more
 * stuff in a connection than we really need to.
 */
public class TestConnectionCreate {

  private final UUID srcId;
  private final UUID dstId;
  private final AirbyteCatalog configuredCatalog;
  private final UUID catalogId;
  private final ConnectionScheduleType scheduleType;
  private final ConnectionScheduleData scheduleData;
  private final List<UUID> operationIds;
  private final Geography geography;
  private final String nameSuffix;

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
   * @param geography - geography
   * @param nameSuffix - optional suffix that will get appended to the connection name.
   */
  private TestConnectionCreate(UUID srcId,
                               UUID dstId,
                               AirbyteCatalog configuredCatalog,
                               UUID catalogId,
                               ConnectionScheduleType scheduleType,
                               ConnectionScheduleData scheduleData,
                               List<UUID> operationIds,
                               Geography geography,
                               String nameSuffix) {
    this.srcId = srcId;
    this.dstId = dstId;
    this.configuredCatalog = configuredCatalog;
    this.catalogId = catalogId;
    this.scheduleType = scheduleType;
    this.scheduleData = scheduleData;
    this.operationIds = operationIds;
    this.geography = geography;
    this.nameSuffix = nameSuffix;
  }

  public UUID getSrcId() {
    return srcId;
  }

  public UUID getDstId() {
    return dstId;
  }

  public AirbyteCatalog getConfiguredCatalog() {
    return configuredCatalog;
  }

  public UUID getCatalogId() {
    return catalogId;
  }

  public ConnectionScheduleType getScheduleType() {
    return scheduleType;
  }

  public ConnectionScheduleData getScheduleData() {
    return scheduleData;
  }

  public List<UUID> getOperationIds() {
    return operationIds;
  }

  public Geography getGeography() {
    return geography;
  }

  public String getNameSuffix() {
    return nameSuffix;
  }

  /**
   * Builder class for TestConnectionCreate. The EXCLUSIVE way of creating a TestConnectionCreate.
   */
  public static class Builder {

    private final UUID srcId;
    private final UUID dstId;

    private final AirbyteCatalog configuredCatalog;
    private final UUID catalogId;

    private UUID normalizationOperationId;
    private List<UUID> additionalOperationIds;
    private ConnectionScheduleType scheduleType;
    private ConnectionScheduleData scheduleData;

    private Geography geography;
    private String nameSuffix;

    /**
     * Builder for TestConnectionCreate. Contains all the required and non-nullable fields needed to
     * create a TestConnectionCreate.
     *
     * @param srcId - source id
     * @param dstId - destination id
     * @param configuredCatalog - configured catalog
     * @param catalogId - discovered catalog id
     */
    public Builder(UUID srcId, UUID dstId, AirbyteCatalog configuredCatalog, UUID catalogId) {
      this.srcId = srcId;
      this.dstId = dstId;
      this.configuredCatalog = configuredCatalog;
      this.catalogId = catalogId;
      additionalOperationIds = Collections.emptyList();
      scheduleType = ConnectionScheduleType.MANUAL;
      geography = Geography.AUTO;
    }

    /**
     * Set the normalization operation id. This is separated out from the additional operation ids
     * setter because we are almost always just setting the one normalization id. If not set, defaults
     * to no normalization.
     *
     * @param normalizationOperationId - normalization operation id
     * @return the builder
     */
    public Builder setNormalizationOperationId(UUID normalizationOperationId) {
      this.normalizationOperationId = normalizationOperationId;
      return this;
    }

    /**
     * Setting operation ids. This only used when the sync has operations in addition to normalization.
     * Generally used rarely. If not set, defaults to no operations.
     *
     * @param additionalOperationIds - additional operation ids
     * @return the builder
     */
    public Builder setAdditionalOperationIds(List<UUID> additionalOperationIds) {
      this.additionalOperationIds = new ArrayList<>(additionalOperationIds);
      return this;
    }

    /**
     * Set the schedule for a connection. Defaults to MANUAL.
     *
     * @param scheduleType - schedule type
     * @param scheduleData - schedule data
     * @return the builder
     */
    public Builder setSchedule(ConnectionScheduleType scheduleType, ConnectionScheduleData scheduleData) {
      this.scheduleType = scheduleType;
      this.scheduleData = scheduleData;
      return this;
    }

    /**
     * Set the geography for a connection. Defaults to AUTO.
     *
     * @param geography - geography
     * @return the builder
     */
    public Builder setGeography(Geography geography) {
      this.geography = geography;
      return this;
    }

    /**
     * Set the name suffix for a connection. For use if having a human-readable connection name is
     * useful Defaults to no suffix.
     *
     * @param nameSuffix - suffix
     * @return the builder
     */
    public Builder setNameSuffix(String nameSuffix) {
      this.nameSuffix = nameSuffix;
      return this;
    }

    /**
     * Build the TestConnectionCreate object.
     *
     * @return TestConnectionCreate
     */
    public TestConnectionCreate build() {
      final List<UUID> operationIds = new ArrayList<>();
      if (normalizationOperationId != null) {
        operationIds.add(normalizationOperationId);
      }
      operationIds.addAll(additionalOperationIds);

      return new TestConnectionCreate(srcId,
          dstId,
          configuredCatalog,
          catalogId,
          scheduleType,
          scheduleData,
          operationIds,
          geography,
          nameSuffix);
    }

  }

}
