/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Sets
import io.airbyte.commons.protocol.transformmodels.FieldTransform
import io.airbyte.commons.protocol.transformmodels.FieldTransform.Companion.createAddFieldTransform
import io.airbyte.commons.protocol.transformmodels.FieldTransform.Companion.createRemoveFieldTransform
import io.airbyte.commons.protocol.transformmodels.FieldTransform.Companion.createUpdateFieldTransform
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform.Companion.createUpdatePrimaryKeyTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform.Companion.createAddStreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform.Companion.createRemoveStreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform.Companion.createUpdateStreamTransform
import io.airbyte.commons.protocol.transformmodels.UpdateFieldSchemaTransform
import io.airbyte.commons.protocol.transformmodels.UpdateStreamTransform
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.protocol.models.JsonSchemas
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import java.util.Optional
import java.util.Spliterator
import java.util.Spliterators
import java.util.function.Consumer
import java.util.stream.StreamSupport

/**
 * Helper class to compute catalog and stream diffs.
 */
object CatalogDiffHelpers {
  private const val ITEMS_KEY = "items"

  /**
   * Extracts all field names from a JSONSchema.
   *
   * @param jsonSchema - a JSONSchema node
   * @return a set of all keys for all objects within the node
   */
  @JvmStatic
  @VisibleForTesting
  fun getAllFieldNames(jsonSchema: JsonNode): Set<String?> =
    getFullyQualifiedFieldNamesWithTypes(jsonSchema)
      .asSequence()
      .map { it.first } // only need field name, not fully qualified name
      .mapNotNull { last(it).orElse(null) }
      .toSet()

  /**
   * Get the last element of a list as an optional.
   *
   * @return returns empty optional if the list is empty or if the last element in the list is null.
   */
  private fun last(list: List<String>): Optional<String> {
    if (list.isEmpty()) {
      return Optional.empty()
    }
    return Optional.ofNullable(list[list.size - 1])
  }

  /**
   * Extracts all fields and their schemas from a JSONSchema. This method returns values in
   * depth-first search preorder. It short-circuits at oneOfs--in other words, child fields of a oneOf
   * are not returned.
   *
   * @param jsonSchema - a JSONSchema node
   * @return a list of all keys for all objects within the node. ordered in depth-first search
   * preorder.
   */
  @JvmStatic
  @VisibleForTesting
  fun getFullyQualifiedFieldNamesWithTypes(jsonSchema: JsonNode): List<Pair<List<String>, JsonNode>> {
    // if this were ever a performance issue, it could be replaced with a trie. this seems unlikely,
    // however.
    val fieldNamesThatAreOneOfs: MutableSet<List<String>> = HashSet()

    return JsonSchemas
      .traverseJsonSchemaWithCollector(
        jsonSchema,
      ) { node: JsonNode, basicPath: List<JsonSchemas.FieldNameOrList> ->
        val fieldName =
          basicPath
            .stream()
            .map { fieldOrList: JsonSchemas.FieldNameOrList -> if (fieldOrList.isList) ITEMS_KEY else fieldOrList.fieldName }
            .toList()
        Pair(fieldName, node)
      }.stream() // first node is the original object.
      .skip(1)
      .filter { fieldWithSchema: Pair<List<String>, JsonNode> ->
        filterChildrenOfFoneOneOf(
          fieldWithSchema.first,
          fieldWithSchema.second,
          fieldNamesThatAreOneOfs,
        )
      }.toList()
  }

  /**
   * Predicate that checks if a field is a CHILD of a oneOf field. If child of a oneOf, returns false.
   * Otherwise, true. This method as side effects. It assumes that it will be run in order on field
   * names returned in depth-first search preoorder. As it encounters oneOfs it adds them to a
   * collection. It then checks if subsequent field names are prefix matches to the field that are
   * oneOfs.
   *
   * @param fieldName - field to investigate
   * @param schema - schema of field
   * @param oneOfFieldNameAccumulator - collection of fields that are oneOfs
   * @return If child of a oneOf, returns false. Otherwise, true.
   */
  private fun filterChildrenOfFoneOneOf(
    fieldName: List<String>,
    schema: JsonNode,
    oneOfFieldNameAccumulator: MutableSet<List<String>>,
  ): Boolean {
    if (isOneOfField(schema)) {
      oneOfFieldNameAccumulator.add(fieldName)
      // return early because we know it is a oneOf and therefore cannot be a child of a oneOf.
      return true
    }

    // leverage that nodes are returned in depth-first search preorder. this means the parent field for
    // the oneOf will be present in the list BEFORE any of its children.
    for (oneOfFieldName in oneOfFieldNameAccumulator) {
      val oneOfFieldNameString = java.lang.String.join(".", oneOfFieldName)
      val fieldNameString = java.lang.String.join(".", fieldName)

      if (fieldNameString.startsWith(oneOfFieldNameString)) {
        return false
      }
    }
    return true
  }

  private fun isOneOfField(schema: JsonNode): Boolean =
    StreamSupport
      .stream(Spliterators.spliteratorUnknownSize(schema.fieldNames(), Spliterator.ORDERED), false)
      .noneMatch { name: String -> name.contains("type") }

  private fun streamDescriptorToMap(catalog: AirbyteCatalog): Map<StreamDescriptor, AirbyteStream> =
    catalog.streams.associateBy {
      extractStreamDescriptor(it)
    }

  fun extractStreamDescriptor(airbyteStream: AirbyteStream): StreamDescriptor =
    StreamDescriptor()
      .withName(airbyteStream.name)
      .withNamespace(airbyteStream.namespace)

  /**
   * Returns difference between two provided catalogs.
   *
   * @param oldCatalog - old catalog
   * @param newCatalog - new catalog
   * @return difference between old and new catalogs
   */
  @JvmStatic
  fun getCatalogDiff(
    oldCatalog: AirbyteCatalog,
    newCatalog: AirbyteCatalog,
    configuredCatalog: ConfiguredAirbyteCatalog,
  ): Set<StreamTransform> {
    val streamTransforms: MutableSet<StreamTransform> = HashSet()

    val descriptorToStreamOld =
      streamDescriptorToMap(
        oldCatalog,
      )
    val descriptorToStreamNew =
      streamDescriptorToMap(
        newCatalog,
      )

    Sets
      .difference(descriptorToStreamOld.keys, descriptorToStreamNew.keys)
      .forEach(
        Consumer { descriptor: StreamDescriptor? ->
          streamTransforms.add(
            createRemoveStreamTransform(descriptor!!),
          )
        },
      )
    Sets
      .difference(descriptorToStreamNew.keys, descriptorToStreamOld.keys)
      .forEach(
        Consumer { descriptor: StreamDescriptor? ->
          streamTransforms.add(
            createAddStreamTransform(descriptor!!),
          )
        },
      )
    Sets
      .intersection(descriptorToStreamOld.keys, descriptorToStreamNew.keys)
      .forEach(
        Consumer { descriptor: StreamDescriptor? ->
          val streamOld = descriptorToStreamOld[descriptor]
          val streamNew = descriptorToStreamNew[descriptor]

          val stream =
            configuredCatalog.streams
              .stream()
              .filter { s: ConfiguredAirbyteStream ->
                s.stream.namespace == descriptor!!.namespace &&
                  s.stream.name == descriptor.name
              }.findFirst()
          if (streamOld != streamNew && stream.isPresent) {
            // getStreamDiff only checks for differences in the stream's field name or field type
            // but there are a number of reasons the streams might be different (such as a source-defined
            // primary key or cursor changing). These should not be expressed as "stream updates".
            val streamTransform = getStreamDiff(streamOld!!, streamNew!!, stream)
            if (!streamTransform.fieldTransforms.isEmpty() || !streamTransform.attributeTransforms.isEmpty()) {
              streamTransforms.add(createUpdateStreamTransform(descriptor!!, streamTransform))
            }
          }
        },
      )

    return streamTransforms
  }

  private fun getStreamDiff(
    streamOld: AirbyteStream,
    streamNew: AirbyteStream,
    configuredStream: Optional<ConfiguredAirbyteStream>,
  ): UpdateStreamTransform {
    val attributeTransforms: MutableSet<StreamAttributeTransform> = HashSet()
    if (streamOld.sourceDefinedPrimaryKey != streamNew.sourceDefinedPrimaryKey) {
      attributeTransforms.add(
        createUpdatePrimaryKeyTransform(
          streamOld.sourceDefinedPrimaryKey,
          streamNew.sourceDefinedPrimaryKey,
          primaryKeyTransformBreaksConnection(configuredStream, streamNew.sourceDefinedPrimaryKey),
        ),
      )
    }

    val fieldTransforms: MutableSet<FieldTransform> = HashSet()
    val fieldNameToTypeOld: Map<List<String>, JsonNode> =
      getFullyQualifiedFieldNamesWithTypes(streamOld.jsonSchema)
        .fold(mutableMapOf()) { acc, pair ->
          collectInHashMap(acc, pair)
          acc
        }

    val fieldNameToTypeNew: Map<List<String>, JsonNode> =
      getFullyQualifiedFieldNamesWithTypes(streamNew.jsonSchema)
        .fold(mutableMapOf()) { acc, pair ->
          collectInHashMap(acc, pair)
          acc
        }

    Sets
      .difference(fieldNameToTypeOld.keys, fieldNameToTypeNew.keys)
      .forEach(
        Consumer { fieldName: List<String> ->
          fieldTransforms.add(
            createRemoveFieldTransform(
              fieldName,
              fieldNameToTypeOld[fieldName]!!,
              fieldTransformBreaksConnection(configuredStream, fieldName),
            ),
          )
        },
      )
    Sets
      .difference(fieldNameToTypeNew.keys, fieldNameToTypeOld.keys)
      .forEach(
        Consumer { fieldName: List<String> ->
          fieldTransforms.add(
            createAddFieldTransform(fieldName, fieldNameToTypeNew[fieldName]!!),
          )
        },
      )
    Sets
      .intersection(fieldNameToTypeOld.keys, fieldNameToTypeNew.keys)
      .forEach(
        Consumer { fieldName: List<String>? ->
          val oldType = fieldNameToTypeOld[fieldName]
          val newType = fieldNameToTypeNew[fieldName]
          if (oldType != newType) {
            fieldTransforms.add(
              createUpdateFieldTransform(
                fieldName!!,
                UpdateFieldSchemaTransform(oldType!!, newType!!),
              ),
            )
          }
        },
      )

    return UpdateStreamTransform(fieldTransforms, attributeTransforms)
  }

  @JvmField
  @VisibleForTesting
  val DUPLICATED_SCHEMA: JsonNode = Jsons.jsonNode("Duplicated Schema")

  @VisibleForTesting
  @JvmStatic
  fun collectInHashMap(
    accumulator: MutableMap<List<String>, JsonNode>,
    value: Pair<List<String>, JsonNode>,
  ) {
    if (accumulator.containsKey(value.first)) {
      accumulator[value.first] = DUPLICATED_SCHEMA
    } else {
      accumulator[value.first] = value.second
    }
  }

  @VisibleForTesting
  @JvmStatic
  fun combineAccumulator(
    accumulatorLeft: MutableMap<List<String>, JsonNode>,
    accumulatorRight: Map<List<String>, JsonNode>,
  ) {
    accumulatorRight.forEach { (key: List<String>, value: JsonNode) ->
      if (accumulatorLeft.containsKey(key)) {
        accumulatorLeft[key] = DUPLICATED_SCHEMA
      } else {
        accumulatorLeft[key] = value
      }
    }
  }

  fun primaryKeyTransformBreaksConnection(
    configuredStream: Optional<ConfiguredAirbyteStream>,
    newSourceDefinedPK: List<List<String?>?>,
  ): Boolean {
    if (configuredStream.isEmpty || newSourceDefinedPK.isEmpty()) {
      return false
    }

    val streamConfig = configuredStream.get()
    val destinationSyncMode = streamConfig.destinationSyncMode

    // Change is breaking if deduping and new source-defined PK was not the previously defined PK
    // Sets are used to compare the PKs in a way that ignores order
    val oldPKSet = java.util.Set.copyOf(streamConfig.primaryKey)
    val newPKSet = java.util.Set.copyOf(newSourceDefinedPK)
    return isDedup(destinationSyncMode) && oldPKSet != newPKSet
  }

  fun fieldTransformBreaksConnection(
    configuredStream: Optional<ConfiguredAirbyteStream>,
    fieldName: List<String>?,
  ): Boolean {
    if (configuredStream.isEmpty) {
      return false
    }

    val streamConfig = configuredStream.get()

    val syncMode = streamConfig.syncMode
    if (SyncMode.INCREMENTAL == syncMode && streamConfig.cursorField == fieldName) {
      return true
    }

    val destinationSyncMode = streamConfig.destinationSyncMode
    return isDedup(destinationSyncMode) && streamConfig.primaryKey!!.contains(fieldName!!)
  }

  @JvmStatic
  fun isDedup(syncMode: DestinationSyncMode): Boolean =
    DestinationSyncMode.APPEND_DEDUP == syncMode || DestinationSyncMode.OVERWRITE_DEDUP == syncMode
}
