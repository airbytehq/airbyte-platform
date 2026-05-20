import { AIRBYTE_PLAN_IDS, ORG_PLAN_IDS } from "./organizationPlans";

describe("organizationPlans", () => {
  describe("AIRBYTE_PLAN_IDS", () => {
    it("should contain 8 Airbyte plan IDs", () => {
      expect(Object.keys(AIRBYTE_PLAN_IDS)).toHaveLength(8);
    });

    it("should have correct plan ID values", () => {
      expect(AIRBYTE_PLAN_IDS.CORE).toBe("plan-airbyte-core");
      expect(AIRBYTE_PLAN_IDS.STANDARD).toBe("plan-airbyte-standard");
      expect(AIRBYTE_PLAN_IDS.PLUS).toBe("plan-airbyte-plus");
      expect(AIRBYTE_PLAN_IDS.SME).toBe("plan-airbyte-sme");
      expect(AIRBYTE_PLAN_IDS.FLEX).toBe("plan-airbyte-flex");
      expect(AIRBYTE_PLAN_IDS.PRO).toBe("plan-airbyte-pro");
      expect(AIRBYTE_PLAN_IDS.STANDARD_TRIAL).toBe("plan-airbyte-standard-trial");
      expect(AIRBYTE_PLAN_IDS.UNIFIED_TRIAL).toBe("plan-airbyte-unified-trial");
    });
  });

  describe("ORG_PLAN_IDS", () => {
    it("should mirror AIRBYTE_PLAN_IDS", () => {
      expect(Object.keys(ORG_PLAN_IDS)).toHaveLength(Object.keys(AIRBYTE_PLAN_IDS).length);
    });

    it("should include all Airbyte plans", () => {
      expect(ORG_PLAN_IDS.CORE).toBe(AIRBYTE_PLAN_IDS.CORE);
      expect(ORG_PLAN_IDS.STANDARD).toBe(AIRBYTE_PLAN_IDS.STANDARD);
      expect(ORG_PLAN_IDS.PLUS).toBe(AIRBYTE_PLAN_IDS.PLUS);
      expect(ORG_PLAN_IDS.SME).toBe(AIRBYTE_PLAN_IDS.SME);
      expect(ORG_PLAN_IDS.FLEX).toBe(AIRBYTE_PLAN_IDS.FLEX);
      expect(ORG_PLAN_IDS.PRO).toBe(AIRBYTE_PLAN_IDS.PRO);
      expect(ORG_PLAN_IDS.STANDARD_TRIAL).toBe(AIRBYTE_PLAN_IDS.STANDARD_TRIAL);
      expect(ORG_PLAN_IDS.UNIFIED_TRIAL).toBe(AIRBYTE_PLAN_IDS.UNIFIED_TRIAL);
    });
  });
});
