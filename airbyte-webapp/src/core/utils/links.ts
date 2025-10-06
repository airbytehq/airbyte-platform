// This file should contain all hard-coded outbound links we use in the UI.
// Everything that is exported via `links` here will be validated in the CI for it's
// existence as well as periodically checked that they are still reachable.

const BASE_DOCS_LINK = "https://docs.airbyte.com";

export const links = {
  connectorSupportLevels: `${BASE_DOCS_LINK}/project-overview/product-support-levels/`,
  dbtCloud: "https://cloud.getdbt.com/",
  dbtCloudIntegrationDocs: `${BASE_DOCS_LINK}/cloud/dbt-cloud-integration`,
  technicalSupport: `${BASE_DOCS_LINK}/troubleshooting`,
  termsLink: "https://airbyte.com/terms",
  privacyLink: "https://airbyte.com/privacy-policy",
  updateLink: `${BASE_DOCS_LINK}/operator-guides/upgrading-airbyte`,
  slackLink: "https://slack.airbyte.com",
  supportPortal: "https://support.airbyte.com",
  docsLink: BASE_DOCS_LINK,
  normalizationLink: `${BASE_DOCS_LINK}/understanding-airbyte/connections#airbyte-basic-normalization`,
  namespaceLink: `${BASE_DOCS_LINK}/understanding-airbyte/namespaces`,
  statusLink: "https://status.airbyte.io/",
  syncModeLink: `${BASE_DOCS_LINK}/understanding-airbyte/connections`,
  sourceDefinedCursorLink: `${BASE_DOCS_LINK}/understanding-airbyte/connections/incremental-append-deduped/#source-defined-cursor`,
  sourceDefinedPKLink: `${BASE_DOCS_LINK}/understanding-airbyte/connections/incremental-append-deduped/#source-defined-primary-key`,
  contactSales: "https://airbyte.com/talk-to-sales",
  cronReferenceLink: "http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html",
  cloudAllowlistIPsLink: `${BASE_DOCS_LINK}/platform/operating-airbyte/ip-allowlist`,
  lowCodeYamlDescription: `${BASE_DOCS_LINK}/connector-development/config-based/understanding-the-yaml-file/yaml-overview`,
  ossSecurityDocs: `${BASE_DOCS_LINK}/operator-guides/security/#securing-airbyte-open-source`,
  connectorBuilderAuthentication: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/authentication`,
  connectorBuilderRecordSelector: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/record-processing#record-selection`,
  connectorBuilderPagination: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/pagination`,
  connectorBuilderIncrementalSync: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/incremental-sync`,
  connectorBuilderErrorHandler: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/error-handling`,
  connectorBuilderPartitioning: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/partitioning`,
  connectorBuilderTransformations: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/record-processing#transformations`,
  connectorBuilderTutorial: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/tutorial`,
  connectorBuilderAssist: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/ai-assist`,
  connectorBuilderCustomComponents: `${BASE_DOCS_LINK}/connector-development/connector-builder-ui/custom-components`,
  connectorBuilderStreamTemplates: `${BASE_DOCS_LINK}/platform/next/connector-development/connector-builder-ui/stream-templates`,
  contributeNewConnectorGitHubToken: `${BASE_DOCS_LINK}/contributing-to-airbyte/submit-new-connector#obtaining-your-github-access-token`,
  creditDescription: `${BASE_DOCS_LINK}/cloud/managing-airbyte-cloud/manage-credits#what-are-credits`,
  pricingPage: "https://airbyte.com/pricing",
  usingCustomConnectors: `${BASE_DOCS_LINK}/operator-guides/using-custom-connectors/`,
  gettingSupport: `${BASE_DOCS_LINK}/community/getting-support`,
  connectorSpecificationReference: `${BASE_DOCS_LINK}/connector-development/connector-specification-reference`,
  connectorSpecificationDocs: `${BASE_DOCS_LINK}/connector-development/connector-specification-reference/#airbyte-modifications-to-jsonschema`,
  schemaChangeManagement: `${BASE_DOCS_LINK}/using-airbyte/schema-change-management`,
  apiAccess: `${BASE_DOCS_LINK}/using-airbyte/configuring-api-access`,
  deployingViaHttp: `${BASE_DOCS_LINK}/using-airbyte/getting-started/oss-quickstart#running-over-http`,
  ossAuthentication: `${BASE_DOCS_LINK}/deploying-airbyte/integrations/authentication`,
  featureTalkToSales:
    "https://airbyte.com/company/talk-to-sales?utm_source=airbyte&utm_medium=product&utm_content=feature-{feature}",
  sonarTalktoSales: "https://calendly.com/teo-airbyte/15min",
  billingNotificationsForm:
    "https://airbyte.retool.com/form/f06009f2-aad6-4df4-bb54-41f3b17d50d2?orgId={organizationId}&email={email}",
  connectionMappings: `${BASE_DOCS_LINK}/using-airbyte/mappings`,
  dataActivationDocs: `${BASE_DOCS_LINK}/platform/next/move-data/elt-data-activation`,
  ssoDocs: `${BASE_DOCS_LINK}/platform/access-management/sso`,
  embeddedOnboardingDocs: `${BASE_DOCS_LINK}/ai-agents/embedded/widget/develop-your-app`,
  fixIngress1_7: `${BASE_DOCS_LINK}/platform/next/deploying-airbyte/integrations/ingress-1-7`,
} as const;

export type OutboundLinks = typeof links;
