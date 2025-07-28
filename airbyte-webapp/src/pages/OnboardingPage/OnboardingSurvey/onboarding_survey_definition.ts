export interface SurveyOption {
  value: string;
  label: string;
  showInput?: boolean;
}

export interface SurveyField {
  id: string;
  type: "dropdown" | "text" | "radio" | "checkbox" | "multiselect";
  step: number;
  question: string;
  options?: SurveyOption[];
  required?: boolean;
  placeholder?: string;
}

export interface Survey {
  version: string;
  titleKey: string;
  descriptionKey: string;
  fields: SurveyField[];
}

// Define field IDs as constants for type safety
export const SURVEY_FIELD_IDS = {
  OCCUPATION: "occupation",
  ROLE_IN_DECISIONS: "role_in_decisions",
  COMPANY_SIZE: "company_size",
  MOTIVATION: "motivation",
  PRIMARY_GOAL: "primary_goal",
} as const;

export const onboarding_survey: Survey = {
  version: "002",
  titleKey: "onboarding.title",
  descriptionKey: "onboarding.description",
  fields: [
    // Step 1: User Profile and Company Context
    {
      id: SURVEY_FIELD_IDS.OCCUPATION,
      type: "dropdown",
      step: 1,
      question: "onboarding.survey.occupation.question",
      required: true,
      options: [
        { value: "product_manager", label: "onboarding.survey.occupation.productManager" },
        { value: "data_analyst", label: "onboarding.survey.occupation.dataAnalyst" },
        { value: "data_engineer", label: "onboarding.survey.occupation.dataEngineer" },
        { value: "software_engineer", label: "onboarding.survey.occupation.softwareEngineer" },
        { value: "business_analyst", label: "onboarding.survey.occupation.businessAnalyst" },
        { value: "executive", label: "onboarding.survey.occupation.executive" },
        { value: "other", label: "onboarding.survey.occupation.other" },
      ],
    },
    {
      id: SURVEY_FIELD_IDS.ROLE_IN_DECISIONS,
      type: "multiselect",
      step: 1,
      question: "onboarding.survey.roleInDecisions.question",
      required: true,
      options: [
        { value: "evaluate_purchase", label: "onboarding.survey.roleInDecisions.evaluatePurchase" },
        { value: "operate_for_clients", label: "onboarding.survey.roleInDecisions.operateForClients" },
        { value: "implement_configure", label: "onboarding.survey.roleInDecisions.implementConfigure" },
        {
          value: "both_evaluation_implementation",
          label: "onboarding.survey.roleInDecisions.bothEvaluationImplementation",
        },
        { value: "other", label: "onboarding.survey.roleInDecisions.other" },
      ],
    },
    {
      id: SURVEY_FIELD_IDS.COMPANY_SIZE,
      type: "dropdown",
      step: 1,
      question: "onboarding.survey.companySize.question",
      required: true,
      options: [
        { value: "1_10", label: "onboarding.survey.companySize.1to10" },
        { value: "11_50", label: "onboarding.survey.companySize.11to50" },
        { value: "51_200", label: "onboarding.survey.companySize.51to200" },
        { value: "201_500", label: "onboarding.survey.companySize.201to500" },
        { value: "501_2000", label: "onboarding.survey.companySize.501to2000" },
        { value: "2000_plus", label: "onboarding.survey.companySize.2000plus" },
      ],
    },
    // Step 2: Intent and Motivation Understanding
    {
      id: SURVEY_FIELD_IDS.MOTIVATION,
      type: "dropdown",
      step: 2,
      question: "onboarding.survey.motivation.question",
      required: true,
      options: [
        { value: "actively_solving", label: "onboarding.survey.motivation.activelySolving" },
        { value: "researching_future", label: "onboarding.survey.motivation.researchingFuture" },
        { value: "just_exploring", label: "onboarding.survey.motivation.justExploring" },
        { value: "other", label: "onboarding.survey.motivation.other", showInput: true },
      ],
    },
    {
      id: SURVEY_FIELD_IDS.PRIMARY_GOAL,
      type: "multiselect",
      step: 2,
      question: "onboarding.survey.primaryGoal.question",
      required: true,
      options: [
        { value: "migrate_databases_cloud", label: "onboarding.survey.primaryGoal.migrateDatabasesCloud" },
        { value: "unify_advertising_marketing", label: "onboarding.survey.primaryGoal.unifyAdvertisingMarketing" },
        { value: "unify_crm_support_product", label: "onboarding.survey.primaryGoal.unifyCRMSupportProduct" },
        { value: "reduce_spend_data_tools", label: "onboarding.survey.primaryGoal.reduceSpendDataTools" },
        { value: "unify_sales_payment", label: "onboarding.survey.primaryGoal.unifySalesPayment" },
        { value: "build_custom_connectors", label: "onboarding.survey.primaryGoal.buildCustomConnectors" },
        { value: "other", label: "onboarding.survey.primaryGoal.other", showInput: true },
      ],
    },
  ],
};

// Helper functions
export const getSurveyFieldsByStep = (step: number): SurveyField[] => {
  return onboarding_survey.fields.filter((field) => field.step === step);
};

export const getTotalSteps = (): number => {
  return Math.max(...onboarding_survey.fields.map((f) => f.step));
};

export const getFieldById = (id: string): SurveyField | undefined => {
  return onboarding_survey.fields.find((field) => field.id === id);
};
