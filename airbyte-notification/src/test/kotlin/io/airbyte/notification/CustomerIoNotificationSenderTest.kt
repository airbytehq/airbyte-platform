/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteNotificationConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.ResponseBody
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException
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
}
