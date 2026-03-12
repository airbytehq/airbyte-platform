import { AIRBYTE_PLAN_IDS, ADP_PLAN_IDS, ORG_PLAN_IDS } from "./organizationPlans";

describe("organizationPlans", () => {
  describe("AIRBYTE_PLAN_IDS", () => {
    it("should contain 7 Airbyte plan IDs", () => {
      expect(Object.keys(AIRBYTE_PLAN_IDS)).toHaveLength(7);
    });

    it("should have correct plan ID values", () => {
      expect(AIRBYTE_PLAN_IDS.CORE).toBe("plan-airbyte-core");
      expect(AIRBYTE_PLAN_IDS.STANDARD).toBe("plan-airbyte-standard");
      expect(AIRBYTE_PLAN_IDS.SME).toBe("plan-airbyte-sme");
      expect(AIRBYTE_PLAN_IDS.FLEX).toBe("plan-airbyte-flex");
      expect(AIRBYTE_PLAN_IDS.PRO).toBe("plan-airbyte-pro");
      expect(AIRBYTE_PLAN_IDS.STANDARD_TRIAL).toBe("plan-airbyte-standard-trial");
      expect(AIRBYTE_PLAN_IDS.UNIFIED_TRIAL).toBe("plan-airbyte-unified-trial");
    });
  });

  describe("ADP_PLAN_IDS", () => {
    it("should contain 3 ADP plan IDs", () => {
      expect(Object.keys(ADP_PLAN_IDS)).toHaveLength(3);
    });

    it("should have correct ADP plan ID values", () => {
      expect(ADP_PLAN_IDS.EMBEDDED_PAYG).toBe("plan-airbyte-embedded-payg");
      expect(ADP_PLAN_IDS.EMBEDDED_ANNUAL_COMMITMENT).toBe("plan-airbyte-embedded-annual-commitment");
      expect(ADP_PLAN_IDS.AGENT_ENGINE_PAYG).toBe("plan-airbyte-agent-engine-payg");
    });
  });

  describe("ORG_PLAN_IDS", () => {
    it("should contain all plans from AIRBYTE_PLAN_IDS and ADP_PLAN_IDS", () => {
      const airbytePlanCount = Object.keys(AIRBYTE_PLAN_IDS).length;
      const adpPlanCount = Object.keys(ADP_PLAN_IDS).length;
      expect(Object.keys(ORG_PLAN_IDS)).toHaveLength(airbytePlanCount + adpPlanCount);
    });

    it("should include all Airbyte plans", () => {
      expect(ORG_PLAN_IDS.CORE).toBe(AIRBYTE_PLAN_IDS.CORE);
      expect(ORG_PLAN_IDS.STANDARD).toBe(AIRBYTE_PLAN_IDS.STANDARD);
      expect(ORG_PLAN_IDS.SME).toBe(AIRBYTE_PLAN_IDS.SME);
      expect(ORG_PLAN_IDS.FLEX).toBe(AIRBYTE_PLAN_IDS.FLEX);
      expect(ORG_PLAN_IDS.PRO).toBe(AIRBYTE_PLAN_IDS.PRO);
      expect(ORG_PLAN_IDS.STANDARD_TRIAL).toBe(AIRBYTE_PLAN_IDS.STANDARD_TRIAL);
      expect(ORG_PLAN_IDS.UNIFIED_TRIAL).toBe(AIRBYTE_PLAN_IDS.UNIFIED_TRIAL);
    });

    it("should include all ADP plans", () => {
      expect(ORG_PLAN_IDS.EMBEDDED_PAYG).toBe(ADP_PLAN_IDS.EMBEDDED_PAYG);
      expect(ORG_PLAN_IDS.EMBEDDED_ANNUAL_COMMITMENT).toBe(ADP_PLAN_IDS.EMBEDDED_ANNUAL_COMMITMENT);
      expect(ORG_PLAN_IDS.AGENT_ENGINE_PAYG).toBe(ADP_PLAN_IDS.AGENT_ENGINE_PAYG);
    });
  });
});
