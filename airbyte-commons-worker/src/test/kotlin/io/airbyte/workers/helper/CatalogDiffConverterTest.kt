/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.commons.json.Jsons
import io.airbyte.config.CatalogDiff
import io.airbyte.config.FieldSchemaUpdate
import io.airbyte.config.FieldTransform
import io.airbyte.config.StreamAttributePrimaryKeyUpdate
import io.airbyte.config.StreamAttributeTransform
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamTransform
import io.airbyte.config.UpdateStream
import io.airbyte.workers.helper.CatalogDiffConverter.toApi
import io.airbyte.workers.helper.CatalogDiffConverter.toDomain
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class CatalogDiffConverterTest {
  @Test
  fun `toApi round trips field and primary key transforms`() {
    val diff =
      CatalogDiff().withTransforms(
        listOf(
          StreamTransform()
            .withTransformType(StreamTransform.TransformType.UPDATE_STREAM)
            .withStreamDescriptor(StreamDescriptor().withName("stream").withNamespace("namespace"))
            .withUpdateStream(
              UpdateStream()
                .withFieldTransforms(
                  listOf(
                    FieldTransform()
                      .withTransformType(FieldTransform.TransformType.ADD_FIELD)
                      .withFieldName(listOf("added"))
                      .withBreaking(false)
                      .withAddField(Jsons.jsonNode(mapOf("type" to "string"))),
                    FieldTransform()
                      .withTransformType(FieldTransform.TransformType.REMOVE_FIELD)
                      .withFieldName(listOf("removed"))
                      .withBreaking(true)
                      .withRemoveField(Jsons.jsonNode(mapOf("type" to "integer"))),
                    FieldTransform()
                      .withTransformType(FieldTransform.TransformType.UPDATE_FIELD_SCHEMA)
                      .withFieldName(listOf("updated"))
                      .withBreaking(true)
                      .withUpdateFieldSchema(
                        FieldSchemaUpdate()
                          .withOldSchema(Jsons.jsonNode(mapOf("type" to "integer")))
                          .withNewSchema(Jsons.jsonNode(mapOf("type" to "number"))),
                      ),
                  ),
                ).withStreamAttributeTransforms(
                  listOf(
                    StreamAttributeTransform()
                      .withTransformType(StreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY)
                      .withBreaking(false)
                      .withUpdatePrimaryKey(
                        StreamAttributePrimaryKeyUpdate()
                          .withOldPrimaryKey(listOf(listOf("old_id")))
                          .withNewPrimaryKey(listOf(listOf("new_id"))),
                      ),
                  ),
                ),
            ),
        ),
      )

    assertEquals(diff, toDomain(toApi(diff)))
  }
}
