/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class NotificationTest {

  @Test
  public void testTextSerialization() throws JsonProcessingException {
    Notification notification = new Notification();
    notification.setText("test content");

    ObjectMapper mapper = new ObjectMapper();

    JsonNode expected = mapper.readTree("""
                                        { "text": "test content" }""");
    assertEquals(expected, notification.toJsonNode());
  }

  @Test
  public void testBlockSerialization() throws JsonProcessingException {
    Notification notification = new Notification();
    notification.setText("A text node");

    Section section = notification.addSection();
    section.setText("Section text");
    notification.addDivider();
    Section anotherSection = notification.addSection();
    Field field1 = anotherSection.addField();
    field1.setText("field1");
    field1.setType("text");
    Field field2 = anotherSection.addField();
    field2.setText("another field");
    field2.setType("mkdwn");

    ObjectMapper mapper = new ObjectMapper();

    JsonNode expected = mapper.readTree("""
                                        {
                                          "text": "A text node",
                                          "blocks": [
                                            {
                                              "type": "section",
                                              "text": {
                                                "type": "mrkdwn",
                                                "text": "Section text"
                                              }
                                            },
                                            {
                                              "type": "divider"
                                            },
                                            {
                                              "type": "section",
                                              "fields": [
                                                {
                                                  "type": "text",
                                                  "text": "field1"
                                                },
                                                {
                                                  "type": "mkdwn",
                                                  "text": "another field"
                                                }
                                              ]
                                            }
                                          ]
                                        }""");
    assertEquals(expected, notification.toJsonNode());
  }

}
