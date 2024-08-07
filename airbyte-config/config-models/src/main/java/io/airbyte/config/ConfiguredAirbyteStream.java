/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ConfiguredAirbyteStream.
 * <p>
 *
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "stream",
  "sync_mode",
  "cursor_field",
  "destination_sync_mode",
  "primary_key",
  "generation_id",
  "minimum_generation_id",
  "sync_id"
})
@SuppressWarnings({
  "PMD.AvoidDuplicateLiterals",
  "PMD.AvoidLiteralsInIfCondition",
  "PMD.ImmutableField",
  "PMD.UseEqualsToCompareStrings"
})
public class ConfiguredAirbyteStream implements Serializable {

  @JsonProperty("stream")
  private AirbyteStream stream;
  @JsonProperty("sync_mode")
  private SyncMode syncMode = null;
  /**
   * Path to the field that will be used to determine if a record is new or modified since the last
   * sync. This field is REQUIRED if `sync_mode` is `incremental`. Otherwise it is ignored.
   *
   */
  @JsonProperty("cursor_field")
  @JsonPropertyDescription("""
                           Path to the field that will be used to determine if a record is new or modified since the last sync. This field is REQUIRED
                           if `sync_mode` is `incremental`. Otherwise it is ignored.""")
  private List<String> cursorField = new ArrayList<String>();
  @JsonProperty("destination_sync_mode")
  private DestinationSyncMode destinationSyncMode = null;
  /**
   * Paths to the fields that will be used as primary key. This field is REQUIRED if
   * `destination_sync_mode` is `*_dedup`. Otherwise it is ignored.
   *
   */
  @JsonProperty("primary_key")
  @JsonPropertyDescription("""
                           Paths to the fields that will be used as primary key. This field is REQUIRED if `destination_sync_mode` is `*_dedup`.
                           Otherwise it is ignored.""")
  private List<List<String>> primaryKey = new ArrayList<List<String>>();
  /**
   * Monotically increasing numeric id representing the current generation of a stream. This id can be
   * shared across syncs. If this is null, it means that the platform is not supporting the refresh
   * and it is expected that no extra id will be added to the records and no data from previous
   * generation will be cleanup.
   *
   */
  @JsonProperty("generation_id")
  @JsonPropertyDescription("""
                           Monotically increasing numeric id representing the current generation of a stream. This id can be shared across syncs.
                           If this is null, it means that the platform is not supporting the refresh and it is expected that no extra id will be added
                           to the records and no data from previous generation will be cleanup.\s""")
  private Long generationId;
  /**
   * The minimum generation id which is needed in a stream. If it is present, the destination will try
   * to delete the data that are part of a generation lower than this property. If the minimum
   * generation is equals to 0, no data deletion is expected from the destiantion If this is null, it
   * means that the platform is not supporting the refresh and it is expected that no extra id will be
   * added to the records and no data from previous generation will be cleanup.
   *
   */
  @JsonProperty("minimum_generation_id")
  @JsonPropertyDescription("""
                           The minimum generation id which is needed in a stream. If it is present, the destination will try to delete the data that
                           are part of a generation lower than this property. If the minimum generation is equals to 0, no data deletion is expected
                           from the destiantion. If this is null, it means that the platform is not supporting the refresh and it is expected that no
                           extra id will be added to the records and no data from previous generation will be cleanup.\s""")
  private Long minimumGenerationId;
  /**
   * Monotically increasing numeric id representing the current sync id. This is aimed to be unique
   * per sync. If this is null, it means that the platform is not supporting the refresh and it is
   * expected that no extra id will be added to the records and no data from previous generation will
   * be cleanup.
   *
   */
  @JsonProperty("sync_id")
  @JsonPropertyDescription("""
                           Monotically increasing numeric id representing the current sync id. This is aimed to be unique per sync.
                           If this is null, it means that the platform is not supporting the refresh and it is expected that no extra id will be added
                           to the records and no data from previous generation will be cleanup.\s""")
  private Long syncId;
  @JsonIgnore
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();
  private static final long serialVersionUID = 3961017355969088418L;

  @JsonProperty("stream")
  public AirbyteStream getStream() {
    return stream;
  }

  @JsonProperty("stream")
  public void setStream(AirbyteStream stream) {
    this.stream = stream;
  }

  public ConfiguredAirbyteStream withStream(AirbyteStream stream) {
    this.stream = stream;
    return this;
  }

  @JsonProperty("sync_mode")
  public SyncMode getSyncMode() {
    return syncMode;
  }

  @JsonProperty("sync_mode")
  public void setSyncMode(SyncMode syncMode) {
    this.syncMode = syncMode;
  }

  public ConfiguredAirbyteStream withSyncMode(SyncMode syncMode) {
    this.syncMode = syncMode;
    return this;
  }

  /**
   * Path to the field that will be used to determine if a record is new or modified since the last
   * sync. This field is REQUIRED if `sync_mode` is `incremental`. Otherwise it is ignored.
   *
   */
  @JsonProperty("cursor_field")
  public List<String> getCursorField() {
    return cursorField;
  }

  /**
   * Path to the field that will be used to determine if a record is new or modified since the last
   * sync. This field is REQUIRED if `sync_mode` is `incremental`. Otherwise it is ignored.
   *
   */
  @JsonProperty("cursor_field")
  public void setCursorField(List<String> cursorField) {
    this.cursorField = cursorField;
  }

  public ConfiguredAirbyteStream withCursorField(List<String> cursorField) {
    this.cursorField = cursorField;
    return this;
  }

  @JsonProperty("destination_sync_mode")
  public DestinationSyncMode getDestinationSyncMode() {
    return destinationSyncMode;
  }

  @JsonProperty("destination_sync_mode")
  public void setDestinationSyncMode(DestinationSyncMode destinationSyncMode) {
    this.destinationSyncMode = destinationSyncMode;
  }

  public ConfiguredAirbyteStream withDestinationSyncMode(DestinationSyncMode destinationSyncMode) {
    this.destinationSyncMode = destinationSyncMode;
    return this;
  }

  /**
   * Paths to the fields that will be used as primary key. This field is REQUIRED if
   * `destination_sync_mode` is `*_dedup`. Otherwise it is ignored.
   *
   */
  @JsonProperty("primary_key")
  public List<List<String>> getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Paths to the fields that will be used as primary key. This field is REQUIRED if
   * `destination_sync_mode` is `*_dedup`. Otherwise it is ignored.
   *
   */
  @JsonProperty("primary_key")
  public void setPrimaryKey(List<List<String>> primaryKey) {
    this.primaryKey = primaryKey;
  }

  public ConfiguredAirbyteStream withPrimaryKey(List<List<String>> primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }

  /**
   * Monotically increasing numeric id representing the current generation of a stream. This id can be
   * shared across syncs. If this is null, it means that the platform is not supporting the refresh
   * and it is expected that no extra id will be added to the records and no data from previous
   * generation will be cleanup.
   *
   */
  @JsonProperty("generation_id")
  public Long getGenerationId() {
    return generationId;
  }

  /**
   * Monotically increasing numeric id representing the current generation of a stream. This id can be
   * shared across syncs. If this is null, it means that the platform is not supporting the refresh
   * and it is expected that no extra id will be added to the records and no data from previous
   * generation will be cleanup.
   *
   */
  @JsonProperty("generation_id")
  public void setGenerationId(Long generationId) {
    this.generationId = generationId;
  }

  public ConfiguredAirbyteStream withGenerationId(Long generationId) {
    this.generationId = generationId;
    return this;
  }

  /**
   * The minimum generation id which is needed in a stream. If it is present, the destination will try
   * to delete the data that are part of a generation lower than this property. If the minimum
   * generation is equals to 0, no data deletion is expected from the destiantion If this is null, it
   * means that the platform is not supporting the refresh and it is expected that no extra id will be
   * added to the records and no data from previous generation will be cleanup.
   *
   */
  @JsonProperty("minimum_generation_id")
  public Long getMinimumGenerationId() {
    return minimumGenerationId;
  }

  /**
   * The minimum generation id which is needed in a stream. If it is present, the destination will try
   * to delete the data that are part of a generation lower than this property. If the minimum
   * generation is equals to 0, no data deletion is expected from the destiantion If this is null, it
   * means that the platform is not supporting the refresh and it is expected that no extra id will be
   * added to the records and no data from previous generation will be cleanup.
   *
   */
  @JsonProperty("minimum_generation_id")
  public void setMinimumGenerationId(Long minimumGenerationId) {
    this.minimumGenerationId = minimumGenerationId;
  }

  public ConfiguredAirbyteStream withMinimumGenerationId(Long minimumGenerationId) {
    this.minimumGenerationId = minimumGenerationId;
    return this;
  }

  /**
   * Monotically increasing numeric id representing the current sync id. This is aimed to be unique
   * per sync. If this is null, it means that the platform is not supporting the refresh and it is
   * expected that no extra id will be added to the records and no data from previous generation will
   * be cleanup.
   *
   */
  @JsonProperty("sync_id")
  public Long getSyncId() {
    return syncId;
  }

  /**
   * Monotically increasing numeric id representing the current sync id. This is aimed to be unique
   * per sync. If this is null, it means that the platform is not supporting the refresh and it is
   * expected that no extra id will be added to the records and no data from previous generation will
   * be cleanup.
   *
   */
  @JsonProperty("sync_id")
  public void setSyncId(Long syncId) {
    this.syncId = syncId;
  }

  public ConfiguredAirbyteStream withSyncId(Long syncId) {
    this.syncId = syncId;
    return this;
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
  }

  public ConfiguredAirbyteStream withAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(ConfiguredAirbyteStream.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
    sb.append("stream");
    sb.append('=');
    sb.append(((this.stream == null) ? "<null>" : this.stream));
    sb.append(',');
    sb.append("syncMode");
    sb.append('=');
    sb.append(((this.syncMode == null) ? "<null>" : this.syncMode));
    sb.append(',');
    sb.append("cursorField");
    sb.append('=');
    sb.append(((this.cursorField == null) ? "<null>" : this.cursorField));
    sb.append(',');
    sb.append("destinationSyncMode");
    sb.append('=');
    sb.append(((this.destinationSyncMode == null) ? "<null>" : this.destinationSyncMode));
    sb.append(',');
    sb.append("primaryKey");
    sb.append('=');
    sb.append(((this.primaryKey == null) ? "<null>" : this.primaryKey));
    sb.append(',');
    sb.append("generationId");
    sb.append('=');
    sb.append(((this.generationId == null) ? "<null>" : this.generationId));
    sb.append(',');
    sb.append("minimumGenerationId");
    sb.append('=');
    sb.append(((this.minimumGenerationId == null) ? "<null>" : this.minimumGenerationId));
    sb.append(',');
    sb.append("syncId");
    sb.append('=');
    sb.append(((this.syncId == null) ? "<null>" : this.syncId));
    sb.append(',');
    sb.append("additionalProperties");
    sb.append('=');
    sb.append(((this.additionalProperties == null) ? "<null>" : this.additionalProperties));
    sb.append(',');
    if (sb.charAt((sb.length() - 1)) == ',') {
      sb.setCharAt((sb.length() - 1), ']');
    } else {
      sb.append(']');
    }
    return sb.toString();
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = ((result * 31) + ((this.generationId == null) ? 0 : this.generationId.hashCode()));
    result = ((result * 31) + ((this.stream == null) ? 0 : this.stream.hashCode()));
    result = ((result * 31) + ((this.minimumGenerationId == null) ? 0 : this.minimumGenerationId.hashCode()));
    result = ((result * 31) + ((this.syncMode == null) ? 0 : this.syncMode.hashCode()));
    result = ((result * 31) + ((this.additionalProperties == null) ? 0 : this.additionalProperties.hashCode()));
    result = ((result * 31) + ((this.destinationSyncMode == null) ? 0 : this.destinationSyncMode.hashCode()));
    result = ((result * 31) + ((this.cursorField == null) ? 0 : this.cursorField.hashCode()));
    result = ((result * 31) + ((this.primaryKey == null) ? 0 : this.primaryKey.hashCode()));
    result = ((result * 31) + ((this.syncId == null) ? 0 : this.syncId.hashCode()));
    return result;
  }

  @Override
  @SuppressFBWarnings("RC_REF_COMPARISON")
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof ConfiguredAirbyteStream) == false) {
      return false;
    }
    ConfiguredAirbyteStream rhs = ((ConfiguredAirbyteStream) other);
    return ((((((((((this.generationId == rhs.generationId) || ((this.generationId != null) && this.generationId.equals(rhs.generationId)))
        && ((this.stream == rhs.stream) || ((this.stream != null) && this.stream.equals(rhs.stream))))
        && ((this.minimumGenerationId == rhs.minimumGenerationId)
            || ((this.minimumGenerationId != null) && this.minimumGenerationId.equals(rhs.minimumGenerationId))))
        && ((this.syncMode == rhs.syncMode) || ((this.syncMode != null) && this.syncMode.equals(rhs.syncMode))))
        && ((this.additionalProperties == rhs.additionalProperties)
            || ((this.additionalProperties != null) && this.additionalProperties.equals(rhs.additionalProperties))))
        && ((this.destinationSyncMode == rhs.destinationSyncMode)
            || ((this.destinationSyncMode != null) && this.destinationSyncMode.equals(rhs.destinationSyncMode))))
        && ((this.cursorField == rhs.cursorField) || ((this.cursorField != null) && this.cursorField.equals(rhs.cursorField))))
        && ((this.primaryKey == rhs.primaryKey) || ((this.primaryKey != null) && this.primaryKey.equals(rhs.primaryKey))))
        && ((this.syncId == rhs.syncId) || ((this.syncId != null) && this.syncId.equals(rhs.syncId))));
  }

}
