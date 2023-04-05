package io.airbyte.notification

import com.sendgrid.Request
import com.sendgrid.Response
import io.micronaut.email.EmailException
import io.micronaut.email.EmailSender
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException

class SendGridEmailNotificationSenderTest {
    private val emailSender: EmailSender<Request, Response> = mockk()
    private val sendGridEmailNotificationSender: SendGridEmailNotificationSender = SendGridEmailNotificationSender(emailSender)

    private val subject = "subject"
    private val message = "message"
    private val from = "from"
    private val to = "to"
    private val sendGridEmailConfig = SendGridEmailConfig(from, to)

    @Test
    fun testSendNotificationSuccessful() {
        every {
            emailSender.send(any())
        } returns mockk()

        sendGridEmailNotificationSender.sendNotification(sendGridEmailConfig, subject, message)

        verify {
            emailSender.send(any())
        }
    }

    @Test
    fun testFailedNotification() {
        every {
            emailSender.send(any())
        } throws EmailException("")

        Assertions.assertThrows(IOException::class.java
        ) {
            sendGridEmailNotificationSender.sendNotification(sendGridEmailConfig, subject, message)
        }
    }
}