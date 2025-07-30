/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.networknt.schema.JsonMetaSchema
import com.networknt.schema.JsonNodePath
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.PathType
import com.networknt.schema.SchemaLocation
import com.networknt.schema.SchemaValidatorsConfig
import com.networknt.schema.SpecVersion
import com.networknt.schema.ValidationContext
import com.networknt.schema.ValidationMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import me.andrz.jackson.JsonContext
import me.andrz.jackson.JsonReferenceException
import me.andrz.jackson.JsonReferenceProcessor
import java.io.File
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Validate a JSON object against a JSONSchema schema.
 */
@Singleton
class JsonSchemaValidator
  @VisibleForTesting
  constructor(
    private val baseUri: URI?,
  ) {
    private val jsonSchemaFactory: JsonSchemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
    private val schemaToValidators: MutableMap<String, JsonSchema> = HashMap()

    constructor() : this(defaultBaseUri)

    /**
     * Create and cache a schema validator for a particular schema. This validator is used when
     * [.testInitializedSchema] and
     * [.validateInitializedSchema] is called.
     */
    fun initializeSchemaValidator(
      schemaName: String,
      schemaJson: JsonNode,
    ) {
      schemaToValidators[schemaName] = getSchemaValidator(schemaJson)
    }

    /**
     * Returns true if the object adheres to the given schema and false otherwise.
     */
    fun testInitializedSchema(
      schemaName: String,
      objectJson: JsonNode?,
    ): Boolean {
      val schema = schemaToValidators[schemaName]
      Preconditions.checkNotNull(
        schema,
        "$schemaName needs to be initialised before calling this method",
      )

      val validate = schema!!.validate(objectJson)
      return validate.isEmpty()
    }

    /**
     * Returns a set of schema validation errors, which is empty if the object adheres to the given
     * schema.
     */
    fun validateInitializedSchema(
      schemaName: String,
      objectNode: JsonNode?,
    ): Set<String> {
      val schema = schemaToValidators[schemaName]
      Preconditions.checkNotNull(
        schema,
        "$schemaName needs to be initialised before calling this method",
      )

      val validationMessages = schema!!.validate(objectNode)
      return validationMessages.stream().map { obj: ValidationMessage -> obj.message }.collect(Collectors.toSet())
    }

    // todo(davin): Rewrite this section to cache schemas.

    /**
     * Test if a JSON object conforms to a given JSONSchema.
     *
     *
     * WARNING
     *
     *
     * The following methods perform JSON validation **by re-creating a validator each time**. This is
     * both CPU and GC expensive, and should be used carefully.
     *
     *
     *
     * @param schemaJson JSONSchema to test against
     * @param objectJson object to test
     * @return true if objectJson conforms to the JSONSchema. Otherwise, false.
     */
    fun test(
      schemaJson: JsonNode,
      objectJson: JsonNode,
    ): Boolean {
      val validationMessages = validateInternal(schemaJson, objectJson)

      if (!validationMessages.isEmpty()) {
        log.info { "JSON schema validation failed. \nerrors: ${validationMessages.joinToString(", ")}" }
      }

      return validationMessages.isEmpty()
    }

    /**
     * Test if a JSON object conforms to a given JSONSchema. Returns the reason for failure if there are
     * any.
     *
     * @param schemaJson JSONSchema to test against
     * @param objectJson object to test
     * @return Set of failure reasons. If empty, then objectJson is valid.
     */
    fun validate(
      schemaJson: JsonNode,
      objectJson: JsonNode,
    ): Set<String> =
      validateInternal(schemaJson, objectJson)
        .stream()
        .map { obj: ValidationMessage? -> obj!!.message }
        .collect(Collectors.toSet())

    /**
     * Test if a JSON object conforms to a given JSONSchema. Throws an exception if the object is not
     * valid.
     *
     * @param schemaJson JSONSchema to test against
     * @param objectJson object to test
     * @throws JsonValidationException thrown is the objectJson is not valid against the schema in
     * schemaJson.
     */
    @Throws(JsonValidationException::class)
    fun ensure(
      schemaJson: JsonNode,
      objectJson: JsonNode,
    ) {
      val validationMessages = validateInternal(schemaJson, objectJson)
      if (validationMessages.isEmpty()) {
        return
      }

      throw JsonValidationException(
        String.format(
          "json schema validation failed when comparing the data to the json schema. \nErrors: %s \nSchema: \n%s",
          validationMessages.joinToString(", "),
          schemaJson.toPrettyString(),
        ),
      )
    }

    /**
     * Test if a JSON object conforms to a given JSONSchema. Throws an exception if the object is not
     * valid.
     *
     * @param schemaJson JSONSchema to test against
     * @param objectJson object to test
     */
    fun ensureAsRuntime(
      schemaJson: JsonNode,
      objectJson: JsonNode,
    ) {
      try {
        ensure(schemaJson, objectJson)
      } catch (e: JsonValidationException) {
        throw RuntimeException(e)
      }
    }

    // keep this internal as it returns a type specific to the wrapped library.
    private fun validateInternal(
      schemaJson: JsonNode,
      objectJson: JsonNode,
    ): Set<ValidationMessage?> {
      Preconditions.checkNotNull(schemaJson)
      Preconditions.checkNotNull(objectJson)

      val schema = getSchemaValidator(schemaJson)
      return schema.validate(objectJson)
    }

    /**
     * Return a schema validator for a json schema, defaulting to the V7 Json schema.
     */
    private fun getSchemaValidator(schemaJson: JsonNode): JsonSchema {
      // Default to draft-07, but have handling for the other metaschemas that networknt supports
      val metaschema: JsonMetaSchema
      val metaschemaNode = schemaJson["\$schema"]
      if (metaschemaNode?.asText() == null || metaschemaNode.asText().isEmpty()) {
        metaschema = JsonMetaSchema.getV7()
      } else {
        val metaschemaString = metaschemaNode.asText()
        // We're not using "http://....".equals(), because we want to avoid weirdness with https, etc.
        metaschema =
          if (metaschemaString.contains("json-schema.org/draft-04")) {
            JsonMetaSchema.getV4()
          } else if (metaschemaString.contains("json-schema.org/draft-06")) {
            JsonMetaSchema.getV6()
          } else if (metaschemaString.contains("json-schema.org/draft/2019-09")) {
            JsonMetaSchema.getV201909()
          } else if (metaschemaString.contains("json-schema.org/draft/2020-12")) {
            JsonMetaSchema.getV202012()
          } else {
            JsonMetaSchema.getV7()
          }
      }

      val context =
        ValidationContext(
          metaschema,
          jsonSchemaFactory,
          SchemaValidatorsConfig(),
        )
      val schema =
        jsonSchemaFactory.create(
          context,
          SchemaLocation.of(baseUri.toString()),
          JsonNodePath(PathType.LEGACY),
          schemaJson,
          null,
        )
      return schema
    }

    companion object {
      // This URI just needs to point at any path in the same directory as /app/WellKnownTypes.json
      // It's required for the JsonSchema#validate method to resolve $ref correctly.
      private var defaultBaseUri: URI? = null

      init {
        try {
          defaultBaseUri = URI("file:///app/nonexistent_file.json")
        } catch (e: URISyntaxException) {
          throw RuntimeException(e)
        }
      }

      /**
       * Get JsonNode for an object defined as the main object in a JsonSchema file. Able to create the
       * JsonNode even if the the JsonSchema refers to objects in other files.
       *
       * @param schemaFile - the schema file
       * @return schema object processed from across all dependency files.
       */
      @JvmStatic
      fun getSchema(schemaFile: File?): JsonNode {
        try {
          return processor.process(schemaFile)
        } catch (e: IOException) {
          throw RuntimeException(e)
        } catch (e: JsonReferenceException) {
          throw RuntimeException(e)
        }
      }

      /**
       * Get JsonNode for an object defined in the "definitions" section of a JsonSchema file. Able to
       * create the JsonNode even if the the JsonSchema refers to objects in other files.
       *
       * @param schemaFile - the schema file
       * @param definitionStructName - get the schema from a struct defined in the "definitions" section
       * of a JsonSchema file (instead of the main object in that file).
       * @return schema object processed from across all dependency files.
       */
      @JvmStatic
      fun getSchema(
        schemaFile: File,
        definitionStructName: String,
      ): JsonNode {
        try {
          val jsonContext = JsonContext(schemaFile)
          val definitionNode: JsonNode? = jsonContext.document["definitions"][definitionStructName]
          if (definitionNode == null) {
            throw IllegalArgumentException("Failed to find definition $definitionStructName in file $schemaFile")
          }
          return processor.process(jsonContext, definitionNode)
        } catch (e: IOException) {
          throw RuntimeException(e)
        } catch (e: JsonReferenceException) {
          throw RuntimeException(e)
        }
      }

      private val processor: JsonReferenceProcessor
        get() {
          // JsonReferenceProcessor follows $ref in json objects. Jackson does not natively support
          // this.
          val jsonReferenceProcessor = JsonReferenceProcessor()
          jsonReferenceProcessor.maxDepth = -1 // no max.

          return jsonReferenceProcessor
        }
    }
  }
