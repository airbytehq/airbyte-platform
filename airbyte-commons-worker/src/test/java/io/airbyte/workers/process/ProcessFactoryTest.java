/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static io.airbyte.workers.process.Metadata.SYNC_JOB;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ProcessFactoryTest {

  @Test
  void getPodNameNormal() {
    final var name = ProcessFactory.createProcessName("airbyte/tester:1", SYNC_JOB, "1", 10,
        KubeProcessFactory.KUBE_NAME_LEN_LIMIT);
    final var withoutRandSuffix = name.substring(0, name.length() - 5);
    Assertions.assertEquals("tester-sync-1-10-", withoutRandSuffix);
  }

  @Test
  void getPodNameTruncated() {
    final var name =
        ProcessFactory.createProcessName("airbyte/very-very-very-long-name-longer-than-63-chars:2",
            SYNC_JOB, "1", 10, KubeProcessFactory.KUBE_NAME_LEN_LIMIT);
    final var withoutRandSuffix = name.substring(0, name.length() - 5);
    Assertions.assertEquals("very-very-very-long-name-longer-than-63-chars-sync-1-10-", withoutRandSuffix);
  }

  @Test
  void testHandlingDashAsFirstCharacter() {
    final var uuid = "7339ba3b-cb53-4210-9591-c70d4a372330";
    final var name = ProcessFactory.createProcessName("airbyte/source-google-adwordsv2:latest", SYNC_JOB,
        uuid, 10, KubeProcessFactory.KUBE_NAME_LEN_LIMIT);

    final var withoutRandSuffix = name.substring(0, name.length() - 5);
    Assertions.assertEquals("le-adwordsv2-sync-7339ba3b-cb53-4210-9591-c70d4a372330-10-", withoutRandSuffix);
  }

  @Test
  void testOnlyDashes() {
    final var uuid = "7339ba3b-cb53-4210-9591-c70d4a372330";
    final var name = ProcessFactory.createProcessName("--------", SYNC_JOB, uuid,
        10, KubeProcessFactory.KUBE_NAME_LEN_LIMIT);

    final var withoutRandSuffix = name.substring(0, name.length() - 5);
    Assertions.assertEquals("sync-7339ba3b-cb53-4210-9591-c70d4a372330-10-", withoutRandSuffix);
  }

  @Test
  void testOnlyNumeric() {
    final var uuid = "7339ba3b-cb53-4210-9591-c70d4a372330";
    final var name = ProcessFactory.createProcessName("0000000000", SYNC_JOB, uuid,
        10, KubeProcessFactory.KUBE_NAME_LEN_LIMIT);

    final var withoutRandSuffix = name.substring(0, name.length() - 5);
    Assertions.assertEquals("sync-7339ba3b-cb53-4210-9591-c70d4a372330-10-", withoutRandSuffix);
  }

  @Test
  void testGetShortImageName() {
    final String imagePath = "airbyte/scheduler:v1";
    Assertions.assertEquals(ProcessFactory.getShortImageName(imagePath), "scheduler");
  }

  @Test
  void testGetShortImageNameEmpty() {
    final String imagePath = "";
    Assertions.assertEquals(ProcessFactory.getShortImageName(imagePath), null);
  }

  @Test
  void testGetShortImageNameNull() {
    final String imagePath = null;
    Assertions.assertEquals(ProcessFactory.getShortImageName(imagePath), null);
  }

  @Test
  void testGetImageVersion() {
    String imagePath = "airbyte/scheduler:dev";
    Assertions.assertEquals(ProcessFactory.getImageVersion(imagePath), "dev");
    imagePath = "airbyte/source-pokeapi:0.1.5";
    Assertions.assertEquals(ProcessFactory.getImageVersion(imagePath), "0.1.5");
  }

  @Test
  void testGetImageVersionEmpty() {
    final String imagePath = "";
    Assertions.assertEquals(ProcessFactory.getImageVersion(imagePath), null);
  }

  @Test
  void testGetImageVersionNull() {
    final String imagePath = null;
    Assertions.assertEquals(ProcessFactory.getImageVersion(imagePath), null);
  }

  @Test
  void testGetImageVersionNoVersion() {
    final String imagePath = "airbyte/scheduler:";
    Assertions.assertEquals(ProcessFactory.getImageVersion(imagePath), null);
  }

}
