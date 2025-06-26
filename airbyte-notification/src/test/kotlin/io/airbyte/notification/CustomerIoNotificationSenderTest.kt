/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.ResponseBody
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException

internal class CustomerIoNotificationSenderTest {
  private val okHttpClient: OkHttpClient = Mockito.mock(OkHttpClient::class.java)
  private val apiToken = "apitoken"
  private val customerIoEmailNotificationSender = CustomerIoEmailNotificationSender(okHttpClient, apiToken)

  @Test
  @Throws(IOException::class)
  fun testSuccessfulSend() {
    val call = Mockito.mock(Call::class.java)
    val response = Mockito.mock(Response::class.java)
    val responseBody = Mockito.mock(ResponseBody::class.java)
    Mockito.`when`(responseBody.string()).thenReturn("")
    Mockito.`when`(response.body).thenReturn(responseBody)
    Mockito.`when`(response.code).thenReturn(200)
    Mockito.`when`(response.isSuccessful).thenReturn(true)
    Mockito.`when`(call.execute()).thenReturn(response)

    Mockito
      .`when`(okHttpClient.newCall(org.mockito.kotlin.anyOrNull()))
      .thenReturn(call)
    customerIoEmailNotificationSender.sendNotification(CustomerIoEmailConfig("to"), "subject", "message")

    Mockito.verify(okHttpClient).newCall(org.mockito.kotlin.anyOrNull())
  }

  @Test
  @Throws(IOException::class)
  fun testUnsuccessfulSend() {
    val call = Mockito.mock(Call::class.java)
    val response = Mockito.mock(Response::class.java)
    val responseBody = Mockito.mock(ResponseBody::class.java)
    Mockito.`when`(responseBody.string()).thenReturn("")
    Mockito.`when`(response.body).thenReturn(responseBody)
    Mockito.`when`(response.code).thenReturn(500)
    Mockito.`when`(response.isSuccessful).thenReturn(false)
    Mockito.`when`(call.execute()).thenReturn(response)

    Mockito
      .`when`(okHttpClient.newCall(org.mockito.kotlin.anyOrNull()))
      .thenReturn(call)

    Assertions
      .assertThatThrownBy {
        customerIoEmailNotificationSender.sendNotification(
          CustomerIoEmailConfig("to"),
          "subject",
          "message",
        )
      }.isInstanceOf(RuntimeException::class.java)
      .hasCauseInstanceOf(IOException::class.java)
  }
}
