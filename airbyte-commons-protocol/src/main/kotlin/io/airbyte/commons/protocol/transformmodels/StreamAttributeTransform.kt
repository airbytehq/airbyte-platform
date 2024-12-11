package io.airbyte.commons.protocol.transformmodels

/**
 * Represents the diff between two fields.
 */
data class StreamAttributeTransform(
  val transformType: StreamAttributeTransformType?,
  val updatePrimaryKeyTransform: UpdateStreamAttributePrimaryKeyTransform?,
  val breaking: Boolean?,
) {
  companion object {
    @JvmStatic
    fun createUpdatePrimaryKeyTransform(
      oldPrimaryKey: List<List<String>>?,
      newPrimaryKey: List<List<String>>?,
      breaking: Boolean?,
    ): StreamAttributeTransform =
      StreamAttributeTransform(
        StreamAttributeTransformType.UPDATE_PRIMARY_KEY,
        UpdateStreamAttributePrimaryKeyTransform(oldPrimaryKey, newPrimaryKey),
        breaking,
      )
  }
}
