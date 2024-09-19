package io.airbyte.mappers.transformations

import io.airbyte.config.Field
import io.airbyte.config.FieldType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SlimStreamTest {
  lateinit var slimStream: SlimStream

  @BeforeEach
  fun setup() {
    slimStream =
      SlimStream(
        fields =
          listOf(
            Field(FIELD1_NAME, FieldType.STRING),
            Field(FIELD2_NAME, FieldType.STRING),
            Field(CURSOR_NAME, FieldType.INTEGER),
            Field(PRIMARY_KEY_NAME, FieldType.STRING),
            Field(PRIMARY_KEY_OTHER_ATTR_NAME, FieldType.STRING),
            Field(SOURCE_CURSOR_NAME, FieldType.STRING),
            Field(SOURCE_PRIMARY_KEY_NAME, FieldType.STRING),
          ),
        cursor = listOf(CURSOR_NAME),
        primaryKey = listOf(listOf(PRIMARY_KEY_NAME), listOf(PRIMARY_KEY_OTHER_ATTR_NAME)),
        sourceDefaultCursor = listOf(SOURCE_CURSOR_NAME),
        sourceDefinedPrimaryKey = listOf(listOf(SOURCE_PRIMARY_KEY_NAME), listOf(PRIMARY_KEY_OTHER_ATTR_NAME)),
      )
  }

  @Test
  fun `renaming a field keeps the type if not provided`() {
    val renamedField = "renamed_field1"
    slimStream.redefineField(FIELD1_NAME, renamedField)

    assertTrue(slimStream.fields.contains(Field(renamedField, FieldType.STRING)))
    assertFalse(slimStream.fields.any { it.name == FIELD1_NAME })
    assertEquals(listOf(CURSOR_NAME), slimStream.cursor)
    assertEquals(listOf(listOf(PRIMARY_KEY_NAME), listOf(PRIMARY_KEY_OTHER_ATTR_NAME)), slimStream.primaryKey)
  }

  @Test
  fun `renaming a field and changing the type`() {
    val renamedField = "renamedFieldWithType"
    slimStream.redefineField(FIELD1_NAME, renamedField, FieldType.NUMBER)

    assertTrue(slimStream.fields.contains(Field(renamedField, FieldType.NUMBER)))
    assertFalse(slimStream.fields.any { it.name == FIELD1_NAME })
  }

  @Test
  fun `renaming a field fails if it leads to a field name collision`() {
    assertThrows<IllegalStateException> {
      slimStream.redefineField(FIELD1_NAME, FIELD2_NAME)
    }
  }

  @Test
  fun `renaming a field fails if the field doesn't exist`() {
    assertThrows<IllegalStateException> {
      slimStream.redefineField("does not exist", "anything")
    }
  }

  @Test
  fun `renaming a field also updates cursor if relevant`() {
    val renamedCursor = "newCursor"
    slimStream.redefineField(CURSOR_NAME, renamedCursor)

    assertTrue(slimStream.fields.contains(Field(renamedCursor, FieldType.INTEGER)))
    assertFalse(slimStream.fields.any { it.name == CURSOR_NAME })
    assertEquals(listOf(renamedCursor), slimStream.cursor)
  }

  @Test
  fun `renaming a field also updates pk if relevant`() {
    val renamedPrimaryKey = "newPk"
    slimStream.redefineField(PRIMARY_KEY_NAME, renamedPrimaryKey)

    assertTrue(slimStream.fields.contains(Field(renamedPrimaryKey, FieldType.STRING)))
    assertFalse(slimStream.fields.any { it.name == PRIMARY_KEY_NAME })
    assertEquals(listOf(listOf(renamedPrimaryKey), listOf(PRIMARY_KEY_OTHER_ATTR_NAME)), slimStream.primaryKey)
  }

  @Test
  fun `renaming a field also updates source default cursor if relevant`() {
    val renamedSourceFieldKey = "newSourceField"
    slimStream.redefineField(SOURCE_CURSOR_NAME, renamedSourceFieldKey)

    assertTrue(slimStream.fields.contains(Field(renamedSourceFieldKey, FieldType.STRING)))
    assertFalse(slimStream.fields.any { it.name == SOURCE_CURSOR_NAME })
    assertEquals(listOf(renamedSourceFieldKey), slimStream.sourceDefaultCursor)
  }

  @Test
  fun `renaming a field also updates source defined PK if relevant`() {
    val renamedSourceFieldKey = "newSourceField"
    slimStream.redefineField(SOURCE_PRIMARY_KEY_NAME, renamedSourceFieldKey)

    assertTrue(slimStream.fields.contains(Field(renamedSourceFieldKey, FieldType.STRING)))
    assertFalse(slimStream.fields.any { it.name == SOURCE_PRIMARY_KEY_NAME })
    assertEquals(listOf(listOf(renamedSourceFieldKey), listOf(PRIMARY_KEY_OTHER_ATTR_NAME)), slimStream.sourceDefinedPrimaryKey)
  }

  companion object {
    const val FIELD1_NAME = "field1"
    const val FIELD2_NAME = "field2"
    const val CURSOR_NAME = "cursor_field"
    const val PRIMARY_KEY_NAME = "primary_key"
    const val PRIMARY_KEY_OTHER_ATTR_NAME = "primary_key_other"
    const val SOURCE_CURSOR_NAME = "source_cursor_field"
    const val SOURCE_PRIMARY_KEY_NAME = "source_primary_key"
  }
}
