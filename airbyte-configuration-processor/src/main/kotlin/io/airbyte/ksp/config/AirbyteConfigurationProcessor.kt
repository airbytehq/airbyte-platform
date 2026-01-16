/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.ksp.config

import com.google.devtools.ksp.getConstructors
import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFile
import com.google.devtools.ksp.symbol.KSNode
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import java.io.BufferedWriter
import java.io.File
import java.io.OutputStreamWriter
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import kotlin.collections.first
import io.micronaut.context.annotation.ConfigurationProperties as MicronautConfigurationProperties
import io.micronaut.core.annotation.Generated as MicronautGenerated
import jakarta.annotation.Generated as JakartaGenerated
import javax.annotation.processing.Generated as JavaxGenerated

private const val AIRBYTE_CONFIGURATION_PREFIX = "airbyte"
private val DEFAULT_VALUES =
  mapOf(
    String::class.java.simpleName.lowercase() to "",
    Int::class.java.simpleName to 0,
    Integer::class.java.simpleName.lowercase() to 0,
    Long::class.java.simpleName.lowercase() to 0L,
    Float::class.java.simpleName.lowercase() to 0F,
    Double::class.java.simpleName.lowercase() to 0.0,
    Duration::class.java.simpleName.lowercase() to "",
    Boolean::class.java.simpleName.lowercase() to false,
    Path::class.java.simpleName.lowercase() to "",
    UUID::class.java.simpleName.lowercase() to "",
  )
private val FIELD_TOKENS = setOf('_', '.')
private val GENERATED_ANNOTATIONS = setOf(MicronautGenerated::class.java.name, JakartaGenerated::class.java.name, JavaxGenerated::class.java.name)
internal const val YAML_EXTENSION = "yml"
internal const val YAML_PATH = "airbyte-configuration"

fun String.lowerCamelToLowerHyphen(): String =
  this.replace("([A-Z])".toRegex()) { matchResult ->
    "-${matchResult.groupValues[1].lowercase()}"
  }

class AirbyteConfigurationProcessor(
  val codeGenerator: CodeGenerator,
  val logger: KSPLogger,
) : SymbolProcessor {
  internal val configurationProperties = mutableListOf<ConfigurationProperty>()
  internal var yaml: Yaml
  internal lateinit var resolver: Resolver

  init {
    val yamlDumpOptions = DumperOptions()
    yamlDumpOptions.defaultFlowStyle = DumperOptions.FlowStyle.BLOCK
    yamlDumpOptions.isPrettyFlow = true
    yaml = Yaml(yamlDumpOptions)
  }

  override fun finish() {
    val propertyMap: Map<String, Any> = getNestedConfiguration()
    val writer =
      BufferedWriter(
        OutputStreamWriter(
          codeGenerator.createNewFileByPath(
            dependencies = Dependencies(false, *resolver.getAllFiles().toList().toTypedArray()),
            path = YAML_PATH,
            extensionName = YAML_EXTENSION,
          ),
        ),
      )
    writer.use { w ->
      yaml.dump(propertyMap, w)
      logger.info("Generated Airbyte configuration file: $YAML_PATH.$YAML_EXTENSION")
    }
  }

  override fun process(resolver: Resolver): List<KSAnnotated> {
    // Save the resolver for use when creating the output files
    this.resolver = resolver

    // Scan the classes for configuration properties
    configurationProperties.addAll(
      resolver
        .getAllFiles()
        .flatMap { file: KSFile ->
          file.declarations
        }.filterIsInstance<KSClassDeclaration>()
        .filter(this::filterKSClasses)
        .map(this::findConfigurationProperties)
        .filter { it.isNotEmpty() }
        .flatMap { it },
    )
    return emptyList()
  }

  /**
   * Filters the class declaration from the symbols to iterate over based on the following criteria:
   * <ol>
   *   <li>The package starts with <code>io.airbyte</code>
   *   <li>The class is <b>NOT</b> annotated with a [io.micronaut.core.annotation.Generated] or [jakarta.annotation.Generated] annotation</li>
   * </ol>
   * @param classDeclaration a KSP [KSClassDeclaration] to filter.
   * @returns false if the class should be filtered from the symbol processing, true otherwise.
   */
  private fun filterKSClasses(classDeclaration: KSClassDeclaration): Boolean =
    classDeclaration.packageName.asString().matches("io\\.airbyte\\.?.*".toRegex()) &&
      classDeclaration.annotations.none { ksAnnotation ->
        GENERATED_ANNOTATIONS.contains(
          ksAnnotation.annotationType
            .resolve()
            .declaration.qualifiedName
            ?.asString() ?: "",
        )
      }

  private fun findConfigurationProperties(classDeclaration: KSClassDeclaration): List<ConfigurationProperty> =
    generateFromConfigurationProperties(classDeclaration = classDeclaration)

  /**
   * Check if the provided [KSClassDeclaration] is annotated with the [MicronautConfigurationProperties] annotation and if present, extracts
   * the referenced configuration properties prefix and uses the declared properties of the class to build a list of required [ConfigurationProperty]
   * instances.
   * @param classDeclaration A KSP [KSClassDeclaration] to inspect
   * @param prefix The configuration properties prefix
   * @returns A list of required [ConfigurationProperty] objects or an empty list if the annotation is not present.
   */
  private fun generateFromConfigurationProperties(
    classDeclaration: KSClassDeclaration,
    prefix: String? = null,
  ): List<ConfigurationProperty> {
    val annotations =
      classDeclaration.annotations.filter { annotation ->
        hasAnnotation(MicronautConfigurationProperties::class.qualifiedName!!, annotation)
      }
    return annotations
      .map { _ ->
        classDeclaration
          .getDeclaredProperties()
          .filter(KSPropertyDeclaration::hasBackingField)
          .map { property ->
            val defaultValue =
              if (hasDefaultValue(classDeclaration, property)) {
                getDefaultValue(
                  classDeclaration,
                  containingFilePath = classDeclaration.containingFile!!.filePath,
                  node = property,
                )
              } else {
                null
              }
            toConfigurationProperty(
              rootPrefix = prefix ?: extractConfigurationPropertyPrefix(annotations.first()),
              property = property,
              defaultValue = if (defaultValue is String) defaultValue.replace("\"", "") else defaultValue,
            )
          }.flatMap { it }
      }.flatMap { it }
      .toList()
  }

  private fun hasAnnotation(
    expectedQualifiedAnnotationName: String,
    annotation: KSAnnotation,
  ): Boolean =
    expectedQualifiedAnnotationName ==
      annotation.annotationType
        .resolve()
        .declaration.qualifiedName
        ?.asString()

  /**
   * Extracts the configuration property prefix from a [MicronautConfigurationProperties] annotation.
   *
   * @param annotation A [KSAnnotation] representing a [MicronautConfigurationProperties] annotation.
   * @return The configuration prefix extracted from the annotation or null.
   */
  private fun extractConfigurationPropertyPrefix(annotation: KSAnnotation) =
    annotation.arguments
      .find { arg -> arg.name?.asString() == VALUE }
      ?.value
      ?.toString()

  /**
   * Formats a property from a Kotlin class into lower hyphen format to align with Micronaut conventions for configuration properties.
   *
   * @param propertyName The name of a property defined in a Kotlin class.
   * @return The lower-hyphen formatted representation of the property name.
   */
  private fun formatPropertyName(propertyName: String) = propertyName.lowerCamelToLowerHyphen()

  /**
   * Scans the class for any parameters present in a constructor that has a default value for the provided property.  This is necessary
   * because KSP cannot identify default values assigned to these parameters.  This is important for data classes that are mapped to
   * configuration properties and may have default values defined.
   *
   * @param classDeclaration The [KSClassDeclaration] of the class that contains the provided node.
   * @param containingFilePath A path to the source file that contains the node that may have a default value.
   * @param node The [KSNode] that may have a default value associated with it.
   * @return The default value associated with the [KSNode] or null if no default value is found in the source for the property.
   */
  private fun getDefaultValue(
    classDeclaration: KSClassDeclaration,
    containingFilePath: String,
    node: KSNode,
  ): Any? {
    // First, find the class that contains the field.  There may be multiple, nested configuration data classes
    // in the containing file, so it is important to scope the search to that class to avoid running into duplicate
    // field names in other classes in the file.
    // Second, attempt to find the field and extract the assigned default value.  It is possible that a constant
    // is assigned as a default value, so additional processing may be required.
    val classCode = File(containingFilePath).readText(Charsets.UTF_8)
    val className = classDeclaration.simpleName.asString()
    val classText =
      "data\\s+class\\s+$className\\s*\\(([^}]*?)\\)(?!,)\\s*(?:\\{?|$)"
        .toRegex(RegexOption.DOT_MATCHES_ALL)
        .find(classCode)
        ?.groups[1]
        ?.value ?: ""
    val type = getType(node)
    val fieldRegEx = "val\\s+${getName(node)}\\s*:\\s*[^,]+\\s*=\\s*([^,]+)\\s*,?".toRegex()
    val result = fieldRegEx.find(classText)
    val defaultValue = extractDefaultValue(value = result?.groups[1]?.value, classCode = classCode)
    return defaultValue?.let { v -> convertStringToType(type, handleQualifiedValue(v)) }
  }

  private fun extractDefaultValue(
    value: String?,
    classCode: String,
  ) = if (value?.contains(CONSTANT_PREFIX) == true) {
    val constantNameRegex = "($CONSTANT_PREFIX[^)]+)+".toRegex()
    val constantName = constantNameRegex.find(value)?.groups[1]?.value
    constantName?.let {
      val constantRegEx = "\\s*const\\s+val\\s+$constantName\\s*=\\s*(.+),?".toRegex()
      val constantResult = constantRegEx.find(classCode)
      constantResult?.groups[1]?.value
    } ?: value
  } else {
    value
  }

  /**
   * Returns the fallback default value associated with the node's type.  This is to ensure that all entries in the
   * generated configuration file have a default value even if the code does not declare one.  See [DEFAULT_VALUES]
   * for more details.
   *
   * @param node The [KSNode] associated with a configuration property
   * @return The fallback default value based on the node's type.
   */
  private fun getFallbackDefaultValue(node: KSNode) = DEFAULT_VALUES[getType(node).lowercase()]

  /**
   * Returns the simple name associated with the provided [KSNode].
   *
   * @param node A [KSNode] instance representing a symbol.
   * @return The simple name associated with the node or "Unknown" if it cannot be retrieved.
   */
  private fun getName(node: KSNode) =
    when (node) {
      is KSPropertyDeclaration -> node.simpleName.asString()
      is KSValueParameter -> node.name!!.asString()
      else -> "Unknown"
    }

  /**
   * Determines the Kotlin type associated with the provided symbol.  The method currently resolves types for properties and constructor/method
   * value parameters.  Otherwise, the type is assumed to be a string for configuration property purposes.
   *
   * @param node A [KSNode] that represents a typed symbol in a class.
   * @return The Kotlin type's simple name or "String" if it cannot be determined.
   */
  private fun getType(node: KSNode) =
    when (node) {
      is KSPropertyDeclaration -> {
        node.type
          .resolve()
          .declaration.simpleName
          .asString()
      }
      is KSValueParameter -> {
        node.type
          .resolve()
          .declaration.simpleName
          .asString()
      }
      else -> {
        String::class.java.simpleName
      }
    }

  /**
   * Given that the [getDefaultValue] method might find a default value of a fully qualified type (e.g., an enum such as MyEnum.SOME_VALUE),
   * this method ensures that we extract the value/name after any separator (.) for inclusion in the generated configuration file.
   * @param defaultValue A default value that may contain a qualified string value.
   * @return The value after any qualifier or the original value if it does not represent a qualifier.
   */
  private fun handleQualifiedValue(defaultValue: String): String =
    if (defaultValue.all { it.isLetter() || FIELD_TOKENS.contains(it) } && defaultValue.contains(".")) {
      defaultValue.split("\\.".toRegex()).last()
    } else {
      defaultValue
    }

  /**
   * Tests whether the [KSPropertyDeclaration] is associated with a default value assignment in the Kotlin class containing
   * the property.
   *
   * @param classDeclaration The [KSClassDeclaration] of the class that contains the property.
   * @param property A [KSPropertyDeclaration] that represents a class property that may have a default value associated with it.
   * @return true if the property has a default value associated with it, false otherwise.
   */
  private fun hasDefaultValue(
    classDeclaration: KSClassDeclaration,
    property: KSPropertyDeclaration,
  ) = classDeclaration
    .getConstructors()
    .map { constructor ->
      constructor.parameters.filter { p -> p.hasDefault && !p.type.resolve().isMarkedNullable }
    }.flatMap { it }
    .find { it.name?.asString() == property.simpleName.asString() } != null

  /**
   * Converts the provided [KSPropertyDeclaration] to a [ConfigurationProperty].  If the property's type is a class
   * that is also annotated with the [MicronautConfigurationProperties] annotation, it will be scanned for configuration
   * properties to add to the list.
   *
   * @param rootPrefix The root prefix, if any, to be included in the generated configuration property name.
   * @param property The [KSPropertyDeclaration] that presents a class property mapped to a configuration property.
   * @param defaultValue The default value associated with the property.  If null, the default fallback value will
   *  be used.
   * @return A list of [ConfigurationProperty] objects representing the application configuration needed to match
   *  the code.
   */
  private fun toConfigurationProperty(
    rootPrefix: String?,
    property: KSPropertyDeclaration,
    defaultValue: Any?,
  ): List<ConfigurationProperty> {
    val annotations =
      property.type
        .resolve()
        .declaration.annotations
        .filter { annotation ->
          hasAnnotation(MicronautConfigurationProperties::class.qualifiedName!!, annotation)
        }.toList()
    return if (annotations.isEmpty()) {
      listOf(
        ConfigurationProperty(
          propertyKey = formatPropertyName("$rootPrefix.${property.simpleName.asString()}"),
          defaultValue =
            defaultValue ?: getFallbackDefaultValue(property),
        ),
      )
    } else {
      val prefix = (rootPrefix?.let { "$it." } ?: "") + extractConfigurationPropertyPrefix(annotations.first())
      generateFromConfigurationProperties(prefix = prefix, classDeclaration = property.type.resolve().declaration as KSClassDeclaration)
    }
  }

  /**
   * Converts the list of configuration properties with flat keys into a nested set of keys for
   * presentation purposes in the generated YAML file.
   */
  @Suppress("unchecked_cast")
  private fun getNestedConfiguration(): Map<String, Any> {
    val propertyMap: Map<String, Any> =
      configurationProperties
        .associate { it.propertyKey to (it.defaultValue ?: "") }
        .filter { it.key.startsWith(AIRBYTE_CONFIGURATION_PREFIX) }

    val nestedMap = mutableMapOf<String, Any>()
    propertyMap.forEach { (key, value) ->
      val keyParts = key.split(".")
      var currentMap = nestedMap
      keyParts.forEachIndexed { index, keyPart ->
        if (index == keyParts.size - 1) {
          currentMap[keyPart] = value
        } else {
          if (!currentMap.containsKey(keyPart) || currentMap[keyPart] !is Map<*, *>) {
            currentMap[keyPart] = mutableMapOf<String, Any>()
          }
          currentMap = currentMap[keyPart] as MutableMap<String, Any>
        }
      }
    }

    return nestedMap
  }
}

internal data class ConfigurationProperty(
  val propertyKey: String,
  val defaultValue: Any? = null,
)
