/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteNotificationConfig
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody
import okio.Buffer
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException
import java.math.BigDecimal
import java.util.UUID

internal class CustomerIoNotificationSenderTest {
  private val okHttpClient: OkHttpClient = mockk()
  private val apiToken = "apitoken"
  private val metricClient: MetricClient = mockk(relaxed = true)
  private val airbyteNotificationConfig =
    AirbyteNotificationConfig(
      customerIo =
        AirbyteNotificationConfig.AirbyteNotificationCustomerIoConfig(apiKey = apiToken),
    )
  private val customerIoEmailNotificationSender = CustomerIoEmailNotificationSender(okHttpClient, airbyteNotificationConfig, metricClient)

  @Test
  fun testSuccessfulSend() {
    val call = mockk<Call>()
    val response = mockk<Response>(relaxed = true)
    val responseBody = mockk<ResponseBody>()
    every { responseBody.string() } returns ""
    every { response.body } returns responseBody
    every { response.code } returns 200
    every { response.isSuccessful } returns true
    every { call.execute() } returns response

    every { okHttpClient.newCall(any()) } returns call
    customerIoEmailNotificationSender.sendNotification(CustomerIoEmailConfig("to"), "subject", "message", UUID.randomUUID())

    verify { okHttpClient.newCall(any()) }
  }

  @Test
  fun testUnsuccessfulSend() {
    val call = mockk<Call>()
    val response = mockk<Response>(relaxed = true)
    val responseBody = mockk<ResponseBody>()
    every { responseBody.string() } returns ""
    every { response.body } returns responseBody
    every { response.code } returns 500
    every { response.isSuccessful } returns false
    every { call.execute() } returns response

    every { okHttpClient.newCall(any()) } returns call

    Assertions
      .assertThatThrownBy {
        customerIoEmailNotificationSender.sendNotification(
          CustomerIoEmailConfig("to"),
          "subject",
          "message",
          UUID.randomUUID(),
        )
      }.isInstanceOf(RuntimeException::class.java)
      .hasCauseInstanceOf(IOException::class.java)
  }

  @Test
  fun `billing settings confirmation sends message 40 with numeric thresholds`() {
    val request = slot<Request>()
    stubSuccessfulCall(request)

    customerIoEmailNotificationSender.sendBillingNotificationSettingsConfirmation(
      CustomerIoEmailConfig("alerts@example.com"),
      "Example \"Quoted\" & Partners",
      BigDecimal("100.00"),
      BigDecimal("500"),
    )

    val payload = ObjectMapper().readTree(request.captured.bodyAsString())
    Assertions.assertThat(payload.path("transactional_message_id").asText()).isEqualTo("40")
    Assertions.assertThat(payload.path("to").asText()).isEqualTo("alerts@example.com")
    Assertions.assertThat(payload.path("identifiers").path("email").asText()).isEqualTo("alerts@example.com")
    val messageData = payload.path("message_data")
    Assertions.assertThat(messageData.path("organization_name").asText()).isEqualTo("Example \"Quoted\" & Partners")
    Assertions.assertThat(messageData.path("sync_credit_threshold").isNumber).isTrue()
    Assertions.assertThat(messageData.path("sync_credit_threshold").toString()).isEqualTo("100")
    Assertions.assertThat(messageData.path("invoice_amount_threshold").toString()).isEqualTo("500")
    Assertions.assertThat(payload.path("queue_draft").asBoolean()).isFalse()
    Assertions.assertThat(payload.path("send_to_unsubscribed").asBoolean()).isFalse()
    Assertions.assertThat(payload.path("tracked").asBoolean()).isFalse()
  }

  @Test
  fun `billing settings confirmation keeps zero thresholds as the number zero`() {
    val request = slot<Request>()
    stubSuccessfulCall(request)

    customerIoEmailNotificationSender.sendBillingNotificationSettingsConfirmation(
      CustomerIoEmailConfig("alerts@example.com"),
      "Example",
      BigDecimal("0.00"),
      BigDecimal.ZERO,
    )

    val messageData = ObjectMapper().readTree(request.captured.bodyAsString()).path("message_data")
    Assertions.assertThat(messageData.path("sync_credit_threshold").toString()).isEqualTo("0")
    Assertions.assertThat(messageData.path("invoice_amount_threshold").toString()).isEqualTo("0")
  }

  @Test
  fun `billing settings confirmation keeps fractional thresholds`() {
    val request = slot<Request>()
    stubSuccessfulCall(request)

    customerIoEmailNotificationSender.sendBillingNotificationSettingsConfirmation(
      CustomerIoEmailConfig("alerts@example.com"),
      "Example",
      BigDecimal("12.50"),
      BigDecimal("0.5"),
    )

    val messageData = ObjectMapper().readTree(request.captured.bodyAsString()).path("message_data")
    Assertions.assertThat(messageData.path("sync_credit_threshold").toString()).isEqualTo("12.5")
    Assertions.assertThat(messageData.path("invoice_amount_threshold").toString()).isEqualTo("0.5")
  }

  @Test
  fun `billing settings confirmation surfaces a rejected send`() {
    val call = mockk<Call>()
    val response = mockk<Response>(relaxed = true)
    val responseBody = mockk<ResponseBody>()
    every { responseBody.string() } returns "message not active"
    every { response.body } returns responseBody
    every { response.code } returns 400
    every { response.isSuccessful } returns false
    every { call.execute() } returns response
    every { okHttpClient.newCall(any()) } returns call

    Assertions
      .assertThatThrownBy {
        customerIoEmailNotificationSender.sendBillingNotificationSettingsConfirmation(
          CustomerIoEmailConfig("alerts@example.com"),
          "Example",
          BigDecimal.ONE,
          BigDecimal.ONE,
        )
      }.isInstanceOf(RuntimeException::class.java)
      .hasCauseInstanceOf(IOException::class.java)
  }

  private fun stubSuccessfulCall(request: CapturingSlot<Request>) {
    val call = mockk<Call>()
    val response = mockk<Response>(relaxed = true)
    val responseBody = mockk<ResponseBody>()
    every { responseBody.string() } returns ""
    every { response.body } returns responseBody
    every { response.code } returns 200
    every { response.isSuccessful } returns true
    every { call.execute() } returns response
    every { okHttpClient.newCall(capture(request)) } returns call
  }

  private fun Request.bodyAsString(): String = Buffer().also { body!!.writeTo(it) }.readUtf8()
}
