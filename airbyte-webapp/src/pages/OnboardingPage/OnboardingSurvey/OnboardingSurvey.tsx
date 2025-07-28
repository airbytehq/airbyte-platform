import { zodResolver } from "@hookform/resolvers/zod";
import React, { useEffect, useState } from "react";
import { Controller, FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useUpdateUser } from "core/api";
import { useAnalyticsService } from "core/services/analytics";
import { Action, Namespace } from "core/services/analytics/types";
import { useAuthService, useCurrentUser } from "core/services/auth";
import { FormModeProvider } from "core/services/ui/FormModeContext";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { RoutePaths } from "pages/routePaths";

import { getSurveyFieldsByStep, getTotalSteps, SURVEY_FIELD_IDS } from "./onboarding_survey_definition";
import { OnboardingDropdown } from "./OnboardingDropdown";
import { OnboardingMultiselect } from "./OnboardingMultiselect";
import styles from "./OnboardingSurvey.module.scss";

interface OnboardingSurveyFormValues {
  [SURVEY_FIELD_IDS.OCCUPATION]: string;
  [SURVEY_FIELD_IDS.ROLE_IN_DECISIONS]: string[];
  [SURVEY_FIELD_IDS.COMPANY_SIZE]: string;
  [SURVEY_FIELD_IDS.MOTIVATION]: string;
  [SURVEY_FIELD_IDS.PRIMARY_GOAL]: string[];
}

const createValidationSchema = (
  currentStep: number,
  formatMessage: (descriptor: { id: string; values?: Record<string, string | string[]> }) => string
) => {
  const currentFields = getSurveyFieldsByStep(currentStep);
  const schemaFields: Record<string, z.ZodTypeAny> = {};

  currentFields.forEach((field) => {
    if (field.type === "multiselect") {
      if (field.required) {
        schemaFields[field.id] = z
          .array(z.string())
          .min(1, formatMessage({ id: "onboarding.survey.fieldRequired", values: { question: field.question } }));
      } else {
        schemaFields[field.id] = z.array(z.string()).optional();
      }
    } else if (field.required) {
      schemaFields[field.id] = z
        .string()
        .min(1, formatMessage({ id: "onboarding.survey.fieldRequired", values: { question: field.question } }));
    } else {
      schemaFields[field.id] = z.string().optional();
    }
  });

  return z.object(schemaFields);
};

export const OnboardingSurvey: React.FC = () => {
  const navigate = useNavigate();
  const analyticsService = useAnalyticsService();
  const user = useCurrentUser();
  const { getAccessToken } = useAuthService();
  const { mutateAsync: updateUser } = useUpdateUser();
  const { formatMessage } = useIntl();

  const [, setIsNewSignup] = useLocalStorage("airbyte_new-signup", false);

  const [currentStep, setCurrentStep] = useState(1);
  const totalSteps = getTotalSteps();
  const currentFields = getSurveyFieldsByStep(currentStep);

  const form = useForm<OnboardingSurveyFormValues>({
    resolver: zodResolver(createValidationSchema(currentStep, formatMessage)),
    mode: "onChange",
    defaultValues: {
      [SURVEY_FIELD_IDS.OCCUPATION]: "",
      [SURVEY_FIELD_IDS.ROLE_IN_DECISIONS]: [],
      [SURVEY_FIELD_IDS.COMPANY_SIZE]: "",
      [SURVEY_FIELD_IDS.MOTIVATION]: "",
      [SURVEY_FIELD_IDS.PRIMARY_GOAL]: [],
    },
  });

  const { watch, trigger } = form;
  const formValues = watch();

  // Update validation schema when step changes
  useEffect(() => {
    form.clearErrors();
  }, [currentStep, form]);

  const handleNext = async () => {
    const isValid = await trigger(currentFields.map((f) => f.id) as Array<keyof OnboardingSurveyFormValues>);

    if (isValid) {
      if (currentStep < totalSteps) {
        analyticsService.track(Namespace.ONBOARDING, Action.NEXT, {
          step: currentStep,
          responses: formValues,
        });
        setCurrentStep(currentStep + 1);
      } else {
        analyticsService.track(Namespace.ONBOARDING, Action.COMPLETED, {
          step: currentStep,
          responses: formValues,
        });

        analyticsService.identify(user.userId, {
          ...formValues,
          onboardingStatus: "completed",
        });

        if (getAccessToken) {
          await updateUser({
            userUpdate: {
              userId: user.userId,
              metadata: {
                ...user.metadata,
                onboarding: "completed",
                ...formValues,
              },
            },
            getAccessToken,
          });
        }
        // Clear the new signup flag since onboarding is complete
        setIsNewSignup(false);
        navigate(RoutePaths.Connections);
      }
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleSkip = async () => {
    analyticsService.track(Namespace.ONBOARDING, Action.SKIP, {
      action: "skip",
      step: currentStep,
      responses: formValues,
    });

    analyticsService.identify(user.userId, {
      ...formValues,
      onboardingStatus: "skipped",
      onboardingSkippedAtStep: currentStep,
    });

    if (getAccessToken) {
      await updateUser({
        userUpdate: {
          userId: user.userId,
          metadata: {
            ...user.metadata,
            onboarding: "skipped",
          },
        },
        getAccessToken,
      });
    }
    // Clear the new signup flag since onboarding is skipped
    setIsNewSignup(false);
    navigate(RoutePaths.Connections);
  };

  const isStepComplete = currentFields.every((field) => {
    if (!field.required) {
      return true;
    }
    const value = formValues[field.id as keyof OnboardingSurveyFormValues];
    if (field.type === "multiselect") {
      return Array.isArray(value) && value.length > 0;
    }
    return !!value && value !== "";
  });

  const progressPercentage = (currentStep / totalSteps) * 100;

  return (
    <Box className={styles.surveyContainer}>
      <FormModeProvider mode="create">
        <FormProvider {...form}>
          <form onSubmit={form.handleSubmit(handleNext)} className={styles.form}>
            {/* Progress bar */}
            <Box className={styles.progressContainer}>
              <FlexContainer justifyContent="space-between" alignItems="center">
                <Text size="sm" color="grey">
                  <FormattedMessage id="onboarding.survey.stepProgress" values={{ currentStep, totalSteps }} />
                </Text>
                <Box className={styles.progressBarWrapper}>
                  <div className={styles.progressBar} style={{ width: `${progressPercentage}%` }} />
                </Box>
              </FlexContainer>
            </Box>

            <FlexContainer direction="column" gap="lg">
              {currentFields.map((formField) => (
                <Controller
                  key={formField.id}
                  name={formField.id as keyof OnboardingSurveyFormValues}
                  control={form.control}
                  render={({ field }) => {
                    return (
                      <Box className={styles.fieldContainer}>
                        <Text size="md" className={styles.question}>
                          <FormattedMessage id={formField.question} />
                          {formField.required && <span className={styles.required}>*</span>}
                        </Text>
                        {formField.type === "multiselect" && formField.options && (
                          <OnboardingMultiselect
                            selectedValues={(field.value as string[]) || []}
                            onSelectValues={(values) => field.onChange(values)}
                            options={formField.options.map((option) => ({
                              ...option,
                              label: formatMessage({ id: option.label }),
                            }))}
                            showSelected
                            label={formatMessage({ id: "onboarding.survey.selectAtLeastOne" })}
                          />
                        )}
                        {formField.type === "dropdown" && formField.options && (
                          <OnboardingDropdown
                            selectedValue={field.value as string}
                            onSelect={(value) => field.onChange(value)}
                            options={formField.options.map((option) => ({
                              ...option,
                              label: formatMessage({ id: option.label }),
                            }))}
                          />
                        )}
                      </Box>
                    );
                  }}
                />
              ))}
            </FlexContainer>

            <FlexContainer justifyContent="space-between" className={styles.navigationButtons}>
              <FlexContainer gap="md">
                <Button type="button" variant="secondary" onClick={handleBack} disabled={currentStep === 1}>
                  <FormattedMessage id="onboarding.survey.back" />
                </Button>
                <Button type="button" variant="clear" onClick={handleSkip}>
                  <FormattedMessage id="onboarding.survey.skip" />
                </Button>
              </FlexContainer>
              <Button type="button" onClick={handleNext} disabled={!isStepComplete}>
                {currentStep === totalSteps ? (
                  <FormattedMessage id="onboarding.survey.complete" />
                ) : (
                  <FormattedMessage id="onboarding.survey.next" />
                )}
              </Button>
            </FlexContainer>
          </form>
        </FormProvider>
      </FormModeProvider>
    </Box>
  );
};
