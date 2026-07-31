/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.commons.json.Jsons
import io.airbyte.workers.helper.CatalogDiffConverter.toApi
import io.airbyte.workers.helper.CatalogDiffConverter.toDomain
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import io.airbyte.api.client.model.generated.CatalogDiff as ApiCatalogDiff
import io.airbyte.api.client.model.generated.FieldAdd as ApiFieldAdd
import io.airbyte.api.client.model.generated.FieldRemove as ApiFieldRemove
import io.airbyte.api.client.model.generated.FieldSchemaUpdate as ApiFieldSchemaUpdate
import io.airbyte.api.client.model.generated.FieldTransform as ApiFieldTransform
import io.airbyte.api.client.model.generated.StreamAttributePrimaryKeyUpdate as ApiStreamAttributePrimaryKeyUpdate
import io.airbyte.api.client.model.generated.StreamAttributeTransform as ApiStreamAttributeTransform
import io.airbyte.api.client.model.generated.StreamDescriptor as ApiStreamDescriptor
import io.airbyte.api.client.model.generated.StreamTransform as ApiStreamTransform
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream as ApiStreamTransformUpdateStream

internal class CatalogDiffConverterTest {
  @Test
  fun `toApi round trips api catalog diff`() {
    val diff =
      ApiCatalogDiff(
        listOf(
          ApiStreamTransform(
            transformType = ApiStreamTransform.TransformType.ADD_STREAM,
            streamDescriptor = ApiStreamDescriptor("added-stream"),
          ),
          ApiStreamTransform(
            transformType = ApiStreamTransform.TransformType.UPDATE_STREAM,
            streamDescriptor = ApiStreamDescriptor("updated-stream", "namespace"),
            updateStream =
              ApiStreamTransformUpdateStream(
                fieldTransforms =
                  listOf(
                    ApiFieldTransform(
                      transformType = ApiFieldTransform.TransformType.ADD_FIELD,
                      fieldName = listOf("added"),
                      breaking = false,
                      addField = ApiFieldAdd(Jsons.jsonNode(mapOf("type" to "string"))),
                    ),
                    ApiFieldTransform(
                      transformType = ApiFieldTransform.TransformType.REMOVE_FIELD,
                      fieldName = listOf("removed"),
                      breaking = true,
                      removeField = ApiFieldRemove(Jsons.jsonNode(mapOf("type" to "integer"))),
                    ),
                    ApiFieldTransform(
                      transformType = ApiFieldTransform.TransformType.UPDATE_FIELD_SCHEMA,
                      fieldName = listOf("updated"),
                      breaking = true,
                      updateFieldSchema =
                        ApiFieldSchemaUpdate(
                          oldSchema = Jsons.jsonNode(mapOf("type" to "integer")),
                          newSchema = Jsons.jsonNode(mapOf("type" to "number")),
                        ),
                    ),
                  ),
                streamAttributeTransforms =
                  listOf(
                    ApiStreamAttributeTransform(
                      transformType = ApiStreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY,
                      breaking = false,
                      updatePrimaryKey =
                        ApiStreamAttributePrimaryKeyUpdate(
                          oldPrimaryKey = listOf(listOf("old_id")),
                          newPrimaryKey = listOf(listOf("new_id")),
                        ),
                    ),
                  ),
              ),
          ),
        ),
      )

    assertEquals(diff, toApi(toDomain(diff)))
  }
}
