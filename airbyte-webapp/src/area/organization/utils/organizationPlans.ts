// Airbyte Plans (includes trials)
export const AIRBYTE_PLAN_IDS = {
  CORE: "plan-airbyte-core",
  STANDARD: "plan-airbyte-standard",
  PLUS: "plan-airbyte-plus",
  SME: "plan-airbyte-sme",
  FLEX: "plan-airbyte-flex",
  PRO: "plan-airbyte-pro",
  STANDARD_TRIAL: "plan-airbyte-standard-trial",
  UNIFIED_TRIAL: "plan-airbyte-unified-trial",
} as const;

export const ORG_PLAN_IDS = AIRBYTE_PLAN_IDS;

export type PlanId = (typeof ORG_PLAN_IDS)[keyof typeof ORG_PLAN_IDS];
