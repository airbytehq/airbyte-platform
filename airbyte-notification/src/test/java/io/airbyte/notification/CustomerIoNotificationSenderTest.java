/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class CustomerIoNotificationSenderTest {

  private final OkHttpClient okHttpClient = mock(OkHttpClient.class);
  private final String apiToken = "apitoken";
  private final CustomerIoEmailNotificationSender customerIoEmailNotificationSender = new CustomerIoEmailNotificationSender(okHttpClient, apiToken);

  @Test
  void testSuccessfulSend() throws IOException {
    final Call call = mock(Call.class);
    final Response response = mock(Response.class);
    final ResponseBody responseBody = mock(ResponseBody.class);
    when(responseBody.string()).thenReturn("");
    when(response.body()).thenReturn(responseBody);
    when(response.code()).thenReturn(200);
    when(response.isSuccessful()).thenReturn(true);
    when(call.execute()).thenReturn(response);

    when(okHttpClient.newCall(any()))
        .thenReturn(call);
    customerIoEmailNotificationSender.sendNotification(new CustomerIoEmailConfig("to"), "subject", "message");

    verify(okHttpClient).newCall(any());
  }

  @Test
  void testUnsuccessfulSend() throws IOException {
    final Call call = mock(Call.class);
    final Response response = mock(Response.class);
    final ResponseBody responseBody = mock(ResponseBody.class);
    when(responseBody.string()).thenReturn("");
    when(response.body()).thenReturn(responseBody);
    when(response.code()).thenReturn(500);
    when(response.isSuccessful()).thenReturn(false);
    when(call.execute()).thenReturn(response);

    when(okHttpClient.newCall(any()))
        .thenReturn(call);

    Assertions.assertThatThrownBy(
        () -> customerIoEmailNotificationSender.sendNotification(
            new CustomerIoEmailConfig("to"), "subject", "message"))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(IOException.class);

  }

}
