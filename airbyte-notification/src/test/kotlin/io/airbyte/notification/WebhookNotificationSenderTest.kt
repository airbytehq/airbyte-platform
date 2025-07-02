/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.mockk.clearMocks
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class WebhookNotificationSenderTest {
  private val httpClient: OkHttpClient = mockk()
  private val webhookNotificationSender = WebhookNotificationSender(httpClient)

  private val subject = "subject"
  private val message = "message"
  private val webhook = WebhookConfig("http://webhook")

  @BeforeEach
  fun init() {
    clearMocks(httpClient)
  }

  @Test
  fun testSendNotificationSuccessful() {
    val successfulCall: Call = mockk()
    val response: Response = mockk(relaxed = true)

    every {
      response.isSuccessful
    } returns true

    justRun {
      response.close()
    }

    every {
      successfulCall.execute()
    } returns response

    every {
      httpClient.newCall(any())
    } returns successfulCall

    webhookNotificationSender.sendNotification(webhook, subject, message, UUID.randomUUID())

    verify {
      httpClient.newCall(any())
      successfulCall.execute()
    }
  }

  @Test
  fun testFailedNotification() {
    val unSuccessfulCall: Call = mockk()
    val response: Response = mockk(relaxed = true)

    every {
      response.isSuccessful
    } returns false

    justRun {
      response.close()
    }

    every {
      unSuccessfulCall.execute()
    } returns response

    every {
      httpClient.newCall(any())
    } returns unSuccessfulCall

    Assertions.assertThrows(
      IOException::class.java,
    ) {
      webhookNotificationSender.sendNotification(webhook, subject, message, UUID.randomUUID())
    }
  }
}
