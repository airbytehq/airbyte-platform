// Airbyte Plans (includes trials)
export const AIRBYTE_PLAN_IDS = {
  CORE: "plan-airbyte-core",
  STANDARD: "plan-airbyte-standard",
  SME: "plan-airbyte-sme",
  FLEX: "plan-airbyte-flex",
  PRO: "plan-airbyte-pro",
  STANDARD_TRIAL: "plan-airbyte-standard-trial",
  UNIFIED_TRIAL: "plan-airbyte-unified-trial",
} as const;

// ADP Plans (Agent Data Platform / Sonar / Agent Engine)
// Organizations with these plans should be managed via app.airbyte.ai
export const ADP_PLAN_IDS = {
  EMBEDDED_PAYG: "plan-airbyte-embedded-payg",
  EMBEDDED_ANNUAL_COMMITMENT: "plan-airbyte-embedded-annual-commitment",
  AGENT_ENGINE_PAYG: "plan-airbyte-agent-engine-payg",
} as const;

// Combined for backward compatibility with existing code
export const ORG_PLAN_IDS = {
  ...AIRBYTE_PLAN_IDS,
  ...ADP_PLAN_IDS,
} as const;

export type PlanId = (typeof ORG_PLAN_IDS)[keyof typeof ORG_PLAN_IDS];
