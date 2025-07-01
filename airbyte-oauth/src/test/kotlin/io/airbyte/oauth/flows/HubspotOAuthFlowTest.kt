/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.oauth.BaseOAuthFlow

class HubspotOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = HubspotOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://app.hubspot.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&state=state&scopes=crm.schemas.contacts.read+crm.objects.contacts.read&optional_scopes=content+automation+e-commerce+files+files.ui_hidden.read+forms+forms-uploaded-files+sales-email-read+tickets+crm.lists.read+crm.objects.companies.read+crm.objects.custom.read+crm.objects.deals.read+crm.objects.feedback_submissions.read+crm.objects.goals.read+crm.objects.owners.read+crm.schemas.companies.read+crm.schemas.custom.read+crm.schemas.deals.read+crm.objects.leads.read"

  override fun testGetSourceConsentUrlEmptyOAuthSpec() {}

  override fun testGetSourceConsentUrl() {}

  override fun testGetDestinationConsentUrl() {}

  override fun testGetDestinationConsentUrlEmptyOAuthSpec() {}
}
