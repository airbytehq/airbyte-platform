/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.ksp.config

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSDeclaration
import com.google.devtools.ksp.symbol.KSFile
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSName
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.KSType
import com.google.devtools.ksp.symbol.KSTypeReference
import com.google.devtools.ksp.symbol.KSValueArgument
import com.google.devtools.ksp.symbol.KSValueParameter
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.yaml.snakeyaml.Yaml
import java.io.ByteArrayOutputStream
import io.micronaut.context.annotation.ConfigurationProperties as MicronautConfigurationProperties
import io.micronaut.core.annotation.Generated as MicronautGenerated
import jakarta.annotation.Generated as JakartaGenerated
import javax.annotation.processing.Generated as JavaxGenerated

/**
 * Converts a camel-cased Kotlin data class fields to Micronaut application YAML hyphen-cased properties.
 * @param fieldName The name of the data class field to convert to a property key
 * @returns The property key representation of the field name.
 */
private fun convertFieldNameToPropertyName(fieldName: String) = fieldName.lowerCamelToLowerHyphen()

internal class AirbyteConfigurationProcessorTest {
  @Test
  fun testClassWithNonAirbytePackage() {
    val codeGenerator = mockk<CodeGenerator>()
    val logger =
      mockk<KSPLogger> {
        every { info(any(), any<KSClassDeclaration>()) } returns Unit
      }
    val clazz =
      mockk<KSClassDeclaration> {
        every { annotations } returns emptySequence()
        every { declarations } returns emptySequence()
        every { packageName.asString() } returns "io.other"
        every { qualifiedName!!.asString() } returns "io.other.Test"
      }
    val file =
      mockk<KSFile> {
        every { declarations } returns sequenceOf(clazz)
      }
    val resolver =
      mockk<Resolver> {
        every { getAllFiles() } returns sequenceOf(file)
      }
    val processor = AirbyteConfigurationProcessor(codeGenerator = codeGenerator, logger = logger)

    processor.process(resolver)

    assertTrue(processor.configurationProperties.isEmpty())
  }

  @ParameterizedTest
  @ValueSource(classes = [MicronautGenerated::class, JakartaGenerated::class, JavaxGenerated::class])
  fun testGeneratedClassIgnore(generatedAnnotation: Class<out Annotation>) {
    val codeGenerator = mockk<CodeGenerator>()
    val logger =
      mockk<KSPLogger> {
        every { info(any(), any<KSClassDeclaration>()) } returns Unit
      }
    val generatedAnnotation =
      mockk<KSAnnotation> {
        every { annotationType } returns
          mockk<KSTypeReference> {
            every { resolve() } returns
              mockk<KSType> {
                every { declaration } returns
                  mockk<KSDeclaration> {
                    every { qualifiedName!!.asString() } returns generatedAnnotation::class.qualifiedName!!
                  }
              }
          }
      }
    val clazz =
      mockk<KSClassDeclaration> {
        every { annotations } returns sequenceOf(generatedAnnotation)
        every { declarations } returns emptySequence()
        every { packageName.asString() } returns "io.airbyte"
        every { qualifiedName!!.asString() } returns "io.airbyte.Test"
      }
    val file =
      mockk<KSFile> {
        every { declarations } returns sequenceOf(clazz)
      }
    val resolver =
      mockk<Resolver> {
        every { getAllFiles() } returns sequenceOf(file)
      }
    val processor = AirbyteConfigurationProcessor(codeGenerator = codeGenerator, logger = logger)

    processor.process(resolver)

    assertTrue(processor.configurationProperties.isEmpty())
  }

  @ParameterizedTest
  @CsvSource("String, ''", "Boolean,false", "Integer,0", "Int,0", "Long,0", "Double,0.0", "Float,0.0f", "Duration,''")
  fun testClassWithConfigurationPropertiesAnnotation(
    propertyType: String,
    defaultValue: String,
  ) {
    val codeGenerator = mockk<CodeGenerator>()
    val logger =
      mockk<KSPLogger> {
        every { info(any(), any<KSClassDeclaration>()) } returns Unit
      }
    val configurationPropertyPrefix = "airbyte.test"
    val configurationPropertiesAnnotation =
      mockk<KSAnnotation> {
        every { annotationType } returns
          mockk<KSTypeReference> {
            every { resolve() } returns
              mockk<KSType> {
                every { declaration } returns
                  mockk<KSDeclaration> {
                    every { qualifiedName!!.asString() } returns MicronautConfigurationProperties::class.qualifiedName!!
                  }
              }
          }
        every { arguments } returns
          listOf(
            mockk<KSValueArgument> {
              every { name!!.asString() } returns VALUE
              every { value } returns configurationPropertyPrefix
            },
          )
      }
    val typeDeclaration =
      mockk<KSDeclaration> {
        every { annotations } returns emptySequence()
        every { simpleName.asString() } returns propertyType
      }
    val actualType =
      mockk<KSType> {
        every { annotations } returns emptySequence()
        every { declaration } returns typeDeclaration
      }
    val typeReference =
      mockk<KSTypeReference> {
        every { resolve() } returns actualType
      }
    val propertyName = "stringValue"
    val property =
      mockk<KSPropertyDeclaration> {
        every { annotations } returns emptySequence()
        every { hasBackingField } returns true
        every { simpleName.asString() } returns propertyName
        every { type } returns typeReference
      }
    val classPackageName = "io.airbyte"
    val className = "Test"
    val clazz =
      mockk<KSClassDeclaration> {
        every { annotations } returns sequenceOf(configurationPropertiesAnnotation)
        every { declarations } returns sequenceOf(property)
        every { packageName.asString() } returns classPackageName
        every { qualifiedName!!.asString() } returns "$classPackageName.$className"
      }
    val file =
      mockk<KSFile> {
        every { declarations } returns sequenceOf(clazz)
      }
    val resolver =
      mockk<Resolver> {
        every { getAllFiles() } returns sequenceOf(file)
      }
    val processor = AirbyteConfigurationProcessor(codeGenerator = codeGenerator, logger = logger)

    processor.process(resolver)

    assertTrue(processor.configurationProperties.isNotEmpty())
    assertEquals(
      convertFieldNameToPropertyName("$configurationPropertyPrefix.$propertyName"),
      processor.configurationProperties.first().propertyKey,
    )
    assertEquals(convertStringToType(propertyType, defaultValue), processor.configurationProperties.first().defaultValue)
  }

  @Test
  fun testClassWithConfigurationPropertiesAnnotationAndDefaultValue() {
    val filePath =
      this::class.java.classLoader
        .getResource("TestClass.kt")
        ?.path!!
    println(filePath)
    val codeGenerator = mockk<CodeGenerator>()
    val logger =
      mockk<KSPLogger> {
        every { info(any(), any<KSClassDeclaration>()) } returns Unit
      }
    val configurationPropertyPrefix = "airbyte.test"
    val configurationPropertiesAnnotation =
      mockk<KSAnnotation> {
        every { annotationType } returns
          mockk<KSTypeReference> {
            every { resolve() } returns
              mockk<KSType> {
                every { declaration } returns
                  mockk<KSDeclaration> {
                    every { qualifiedName!!.asString() } returns MicronautConfigurationProperties::class.qualifiedName!!
                  }
              }
          }
        every { arguments } returns
          listOf(
            mockk<KSValueArgument> {
              every { name!!.asString() } returns VALUE
              every { value } returns configurationPropertyPrefix
            },
          )
      }
    val typeDeclaration =
      mockk<KSDeclaration> {
        every { annotations } returns emptySequence()
        every { simpleName.asString() } returns String::class.qualifiedName!!
      }
    val actualType =
      mockk<KSType> {
        every { annotations } returns emptySequence()
        every { declaration } returns typeDeclaration
      }
    val typeReference =
      mockk<KSTypeReference> {
        every { resolve() } returns actualType
      }
    val propertyName = "stringValue"
    val property =
      mockk<KSPropertyDeclaration> {
        every { annotations } returns emptySequence()
        every { hasBackingField } returns true
        every { simpleName.asString() } returns propertyName
        every { type } returns typeReference
      }
    val declaredParameter =
      mockk<KSValueParameter> {
        every { annotations } returns emptySequence()
        every { name } returns
          mockk<KSName> {
            every { asString() } returns propertyName
          }
        every { type } returns
          mockk<KSTypeReference> {
            every { resolve() } returns
              mockk<KSType> {
                every { hasDefault } returns true
                every { isMarkedNullable } returns false
              }
          }
      }
    val declaredConstructor =
      mockk<KSFunctionDeclaration> {
        every { annotations } returns emptySequence()
        every { parameters } returns listOf(declaredParameter)
        every { simpleName.asString() } returns "<init>"
      }
    val classPackageName = "io.airbyte.test"
    val className = "TestClass"
    val clazz =
      mockk<KSClassDeclaration> {
        every { annotations } returns sequenceOf(configurationPropertiesAnnotation)
        every { containingFile!!.filePath } returns filePath
        every { declarations } returns sequenceOf(declaredConstructor, property)
        every { packageName.asString() } returns classPackageName
        every { qualifiedName!!.asString() } returns "$classPackageName.$className"
        every { simpleName.asString() } returns className
      }
    val file =
      mockk<KSFile> {
        every { declarations } returns sequenceOf(clazz)
      }
    val resolver =
      mockk<Resolver> {
        every { getAllFiles() } returns sequenceOf(file)
      }
    val processor = AirbyteConfigurationProcessor(codeGenerator = codeGenerator, logger = logger)

    processor.process(resolver)

    assertTrue(processor.configurationProperties.isNotEmpty())
    assertEquals(
      convertFieldNameToPropertyName("$configurationPropertyPrefix.$propertyName"),
      processor.configurationProperties.first().propertyKey,
    )
    assertEquals("Hello World!", processor.configurationProperties.first().defaultValue)
  }

  @Test
  @Suppress("unchecked_cast")
  fun testFinishWritesConfigurationToOutput() {
    val configurationProperty = "airbyte.test"
    val defaultValue = "\"\""
    val configuration = ConfigurationProperty(configurationProperty, defaultValue)
    val outputStream = ByteArrayOutputStream()
    val codeGenerator =
      mockk<CodeGenerator> {
        every { createNewFileByPath(dependencies = any(), path = YAML_PATH, extensionName = YAML_EXTENSION) } returns outputStream
      }
    val logger =
      mockk<KSPLogger> {
        every { info(any(), any<KSClassDeclaration>()) } returns Unit
      }
    val yaml = Yaml()
    val processor = AirbyteConfigurationProcessor(codeGenerator = codeGenerator, logger = logger)
    processor.resolver =
      mockk {
        every { getAllFiles() } returns sequenceOf(mockk<KSFile>(relaxed = true))
      }
    processor.configurationProperties.add(configuration)
    processor.finish()

    val configurations: Map<String, Any> = yaml.load(outputStream.toString())
    assertEquals(configuration.propertyKey.split(".").first(), configurations.keys.first())
    assertEquals(configuration.defaultValue, (configurations[configuration.propertyKey.split(".").first()] as Map<String, Any>).values.first())
  }
}
