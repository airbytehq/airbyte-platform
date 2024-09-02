import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.commons.json.Jsons
import io.airbyte.config.MapperSpecificationField
import io.airbyte.config.MapperSpecificationFieldEnum
import io.airbyte.config.MapperSpecificationFieldInt
import io.airbyte.config.MapperSpecificationFieldString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Test that the serialized specification contains all the needed fields.
 */
class TestMapperSpecifications {
  private val stringSpec =
    MapperSpecificationFieldString(
      title = "title",
      description = "description",
      examples = listOf("example"),
      default = "default",
    )
  private val stringSpecJson = Jsons.jsonNode(stringSpec)
  val stringSpecStr = """
      {
        "title" : "title",
        "description" : "description",
        "examples" : [ "example" ],
        "default" : "default",
        "type" : "string"
      }
    """

  private val intSpec =
    MapperSpecificationFieldInt(
      title = "title",
      description = "description",
      examples = listOf(1),
      default = 2,
      minimum = 0,
      maximum = 10,
    )
  private val intSpecJson = Jsons.jsonNode(intSpec)
  val intSpecStr = """
      {
        "title" : "title",
        "description" : "description",
        "examples" : [ 1 ],
        "default" : 2,
        "minimum" : 0,
        "maximum" : 10,
        "type" : "integer"
      }
    """

  private val enumSpec =
    MapperSpecificationFieldEnum(
      title = "title",
      description = "description",
      enum = listOf("a", "b", "c"),
      default = "a",
      examples = listOf(),
    )
  private val enumSpecJson = Jsons.jsonNode(enumSpec)
  val enumSpecStr = """
      {
        "title" : "title",
        "description" : "description",
        "enum" : [ "a", "b", "c" ],
        "default" : "a",
        "examples" : [ ],
        "type" : "string"
      }
    """

  @Test
  fun testSerializeStringSpec() {
    assertEquals("string", stringSpecJson.get("type").asText())
    assertEquals("title", stringSpecJson.get("title").asText())
    assertEquals("description", stringSpecJson.get("description").asText())
    assertEquals("example", stringSpecJson.get("examples").get(0).asText())
    assertEquals("default", stringSpecJson.get("default").asText())

    println(stringSpecJson.toPrettyString())
  }

  @Test
  fun testSerializeIntSpec() {
    assertEquals("integer", intSpecJson.get("type").asText())
    assertEquals("title", intSpecJson.get("title").asText())
    assertEquals("description", intSpecJson.get("description").asText())
    assertEquals(1, intSpecJson.get("examples").get(0).asInt())
    assertEquals(2, intSpecJson.get("default").asInt())
    assertEquals(0, intSpecJson.get("minimum").asInt())
    assertEquals(10, intSpecJson.get("maximum").asInt())

    println(intSpecJson.toPrettyString())
  }

  @Test
  fun testSerializeEnumSpec() {
    assertEquals("string", enumSpecJson.get("type").asText())
    assertEquals("title", enumSpecJson.get("title").asText())
    assertEquals("description", enumSpecJson.get("description").asText())
    assertEquals("a", enumSpecJson.get("enum").get(0).asText())
    assertEquals("b", enumSpecJson.get("enum").get(1).asText())
    assertEquals("c", enumSpecJson.get("enum").get(2).asText())
    assertEquals("a", enumSpecJson.get("default").asText())

    println(enumSpecJson.toPrettyString())
  }

  @Test
  fun testSerializeInterface() {
    val multipleSpecs: List<MapperSpecificationField<Any>> = listOf(stringSpec, intSpec, enumSpec)

    val multipleSpecsJson = Jsons.jsonNode(multipleSpecs)
    assertEquals(3, multipleSpecsJson.size())
    assertEquals(stringSpecJson, multipleSpecsJson.get(0))
    assertEquals(intSpecJson, multipleSpecsJson.get(1))
    assertEquals(enumSpecJson, multipleSpecsJson.get(2))
  }

  @Test
  fun testDeserializeStringSpec() {
    val deserialized = Jsons.deserialize(stringSpecStr, MapperSpecificationFieldString::class.java)
    assertEquals(stringSpec, deserialized)
  }

  @Test
  fun testDeserializeIntSpec() {
    val deserialized = Jsons.deserialize(intSpecStr, MapperSpecificationFieldInt::class.java)
    assertEquals(intSpec, deserialized)
  }

  @Test
  fun testDeserializeEnumSpec() {
    val deserialized = Jsons.deserialize(enumSpecStr, MapperSpecificationFieldEnum::class.java)
    assertEquals(enumSpec, deserialized)
  }

  @Test
  fun testDeserializeInterface() {
    val deserialized =
      Jsons.deserialize(
        """[$stringSpecStr, $intSpecStr, $enumSpecStr]""",
        object : TypeReference<List<MapperSpecificationField<Any>>>() {},
      )
    assertEquals(listOf(stringSpec, intSpec, enumSpec), deserialized)
  }
}
