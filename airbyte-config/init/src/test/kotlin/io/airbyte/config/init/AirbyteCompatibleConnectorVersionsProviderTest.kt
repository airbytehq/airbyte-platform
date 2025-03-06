/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.ResponseBody
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.io.IOException

internal class AirbyteCompatibleConnectorVersionsProviderTest {
  @Test
  internal fun testSuccessfulRetrievalOfRemoteFile() {
    val okHttpClient: OkHttpClient = mockk()
    val call: Call = mockk()
    val response: Response = mockk()
    val responseBody: ResponseBody = mockk()
    val airbyteCompatibleConnectorVersionsProvider =
      AirbyteCompatibleConnectorVersionsProvider(
        okHttpClient = okHttpClient,
      )

    every { responseBody.string() } returns getMockJsonFileOutput()
    every { response.isSuccessful } returns true
    every { response.body } returns responseBody
    every { response.close() } returns Unit
    every { call.execute() } returns response
    every { okHttpClient.newCall(any()) } returns call

    val matrix = airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix()

    assertEquals(3, matrix.size)

    val mysql = matrix["4e3b6a13-18db-4a77-b9c8-56b4f10d0b91"]!!
    assertTrue(matrix.containsKey("4e3b6a13-18db-4a77-b9c8-56b4f10d0b91"))
    assertEquals("source-mysql", mysql.connectorName)
    assertEquals("source", mysql.connectorType)
    assertEquals(3, mysql.compatibilityMatrix.size)
    assertEquals("1.0.0", mysql.compatibilityMatrix[0].connectorVersion)
    assertEquals("0.1.0", mysql.compatibilityMatrix[0].airbyteVersion)
    assertEquals("1.1.0", mysql.compatibilityMatrix[1].connectorVersion)
    assertEquals("0.1.0", mysql.compatibilityMatrix[1].airbyteVersion)
    assertEquals("1.2.0", mysql.compatibilityMatrix[2].connectorVersion)
    assertEquals("0.2.0", mysql.compatibilityMatrix[2].airbyteVersion)

    val redshift = matrix["7e88a0d7-dc48-4d7e-b824-f6e1de317b76"]!!
    assertTrue(matrix.containsKey("7e88a0d7-dc48-4d7e-b824-f6e1de317b76"))
    assertEquals("destination-redshift", redshift.connectorName)
    assertEquals("destination", redshift.connectorType)
    assertEquals(2, redshift.compatibilityMatrix.size)
    assertEquals("2.0.0", redshift.compatibilityMatrix[0].connectorVersion)
    assertEquals("0.3.0", redshift.compatibilityMatrix[0].airbyteVersion)
    assertEquals("2.1.0", redshift.compatibilityMatrix[1].connectorVersion)
    assertEquals("0.3.0", redshift.compatibilityMatrix[1].airbyteVersion)

    val s3 = matrix["d6964f6e-abe7-4a26-b7eb-f4a74e1c2b75"]!!
    assertTrue(matrix.containsKey("d6964f6e-abe7-4a26-b7eb-f4a74e1c2b75"))
    assertEquals("source-s3", s3.connectorName)
    assertEquals("source", s3.connectorType)
    assertEquals(1, s3.compatibilityMatrix.size)
    assertEquals("3.0.0", s3.compatibilityMatrix[0].connectorVersion)
    assertEquals("0.5.0", s3.compatibilityMatrix[0].airbyteVersion)
  }

  @Test
  internal fun testUnsuccessfulResponseFromRemoteFile() {
    val okHttpClient: OkHttpClient = mockk()
    val call: Call = mockk()
    val response: Response = mockk()
    val responseBody: ResponseBody = mockk()
    val airbyteCompatibleConnectorVersionsProvider =
      AirbyteCompatibleConnectorVersionsProvider(
        okHttpClient = okHttpClient,
      )

    every { responseBody.string() } returns ""
    every { response.isSuccessful } returns false
    every { response.body } returns responseBody
    every { response.code } returns HttpStatus.NOT_FOUND.code
    every { response.message } returns "not found"
    every { response.close() } returns Unit
    every { call.execute() } returns response
    every { okHttpClient.newCall(any()) } returns call

    val matrix = airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix()
    assertEquals(0, matrix.size)
  }

  @Test
  internal fun testNullBodyResponseFromRemoteFile() {
    val okHttpClient: OkHttpClient = mockk()
    val call: Call = mockk()
    val response: Response = mockk()
    val airbyteCompatibleConnectorVersionsProvider =
      AirbyteCompatibleConnectorVersionsProvider(
        okHttpClient = okHttpClient,
      )

    every { response.isSuccessful } returns true
    every { response.body } returns null
    every { response.code } returns HttpStatus.OK.code
    every { response.message } returns ""
    every { response.close() } returns Unit
    every { call.execute() } returns response
    every { okHttpClient.newCall(any()) } returns call

    val matrix = airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix()
    assertEquals(0, matrix.size)
  }

  @Test
  internal fun testExceptionDuringRetrievalOfRemoteFile() {
    val okHttpClient: OkHttpClient = mockk()
    val call: Call = mockk()
    val airbyteCompatibleConnectorVersionsProvider =
      AirbyteCompatibleConnectorVersionsProvider(
        okHttpClient = okHttpClient,
      )

    every { call.execute() } throws IOException("test")
    every { okHttpClient.newCall(any()) } returns call

    assertDoesNotThrow {
      val matrix = airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix()
      assertEquals(0, matrix.size)
    }
  }

  private fun getMockJsonFileOutput(): String =
    "{\n" +
      "  \"compatibleConnectors\": [\n" +
      "    {\n" +
      "      \"connectorName\": \"source-mysql\",\n" +
      "      \"connectorType\": \"source\",\n" +
      "      \"connectorDefinitionId\": \"4e3b6a13-18db-4a77-b9c8-56b4f10d0b91\",\n" +
      "      \"compatibilityMatrix\": [\n" +
      "        {\n" +
      "          \"connectorVersion\": \"1.0.0\",\n" +
      "          \"airbyteVersion\": \"0.1.0\"\n" +
      "        },\n" +
      "        {\n" +
      "          \"connectorVersion\": \"1.1.0\",\n" +
      "          \"airbyteVersion\": \"0.1.0\"\n" +
      "        },\n" +
      "        {\n" +
      "          \"connectorVersion\": \"1.2.0\",\n" +
      "          \"airbyteVersion\": \"0.2.0\"\n" +
      "        }\n" +
      "      ]\n" +
      "    },\n" +
      "    {\n" +
      "      \"connectorName\": \"destination-redshift\",\n" +
      "      \"connectorType\": \"destination\",\n" +
      "      \"connectorDefinitionId\": \"7e88a0d7-dc48-4d7e-b824-f6e1de317b76\",\n" +
      "      \"compatibilityMatrix\": [\n" +
      "        {\n" +
      "          \"connectorVersion\": \"2.0.0\",\n" +
      "          \"airbyteVersion\": \"0.3.0\"\n" +
      "        },\n" +
      "        {\n" +
      "          \"connectorVersion\": \"2.1.0\",\n" +
      "          \"airbyteVersion\": \"0.3.0\"\n" +
      "        }\n" +
      "      ]\n" +
      "    },\n" +
      "    {\n" +
      "      \"connectorName\": \"source-s3\",\n" +
      "      \"connectorType\": \"source\",\n" +
      "      \"connectorDefinitionId\": \"d6964f6e-abe7-4a26-b7eb-f4a74e1c2b75\",\n" +
      "      \"compatibilityMatrix\": [\n" +
      "        {\n" +
      "          \"connectorVersion\": \"3.0.0\",\n" +
      "          \"airbyteVersion\": \"0.5.0\"\n" +
      "        }\n" +
      "      ]\n" +
      "    }\n" +
      "  ]\n" +
      "}\n"
}
