package io.airbyte.commons.protocol.transformmodels

data class UpdateStreamAttributePrimaryKeyTransform(
  val oldPrimaryKey: List<List<String>>?,
  val newPrimaryKey: List<List<String>>?,
)
