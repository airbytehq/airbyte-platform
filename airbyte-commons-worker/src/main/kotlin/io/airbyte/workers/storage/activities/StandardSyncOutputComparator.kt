/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage.activities

import io.airbyte.config.StandardSyncOutput

class StandardSyncOutputComparator : Comparator<StandardSyncOutput> {
  override fun compare(
    o1: StandardSyncOutput?,
    o2: StandardSyncOutput?,
  ): Int {
    return if (o1 == o2) {
      return 0
    } else {
      val result =
        o1?.outputCatalog == o2?.outputCatalog &&
          o1?.failures == o2?.failures &&
          o1?.normalizationSummary == o2?.normalizationSummary &&
          o1?.state == o2?.state &&
          o1?.additionalProperties == o2?.additionalProperties &&
          o1?.standardSyncSummary == o2?.standardSyncSummary &&
          o1?.uri == o2?.uri
      // Temporarily ignore the webhook operation summary until the flow is corrected to include this when persisting the output
//          && o1?.webhookOperationSummary == o2?.webhookOperationSummary
      return if (result) {
        return 0
      } else {
        return 1
      }
    }
  }
}
