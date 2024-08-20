import React from "react";
import { useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { FormConnectionFormValues } from "./formConfig";

/**
 * react-hook-form create connection and error form controls component
 * since we need to access the form context, these controls combined into separated component
 */
export const CreateConnectionFormControls: React.FC = () => {
  const { isSubmitting, isValid, errors } = useFormState<FormConnectionFormValues>();
  const { trigger } = useFormContext<FormConnectionFormValues>();
  const { getErrorMessage } = useConnectionFormService();
  const errorMessage = getErrorMessage(isValid, errors);

  const watchedScheduleType = useWatch<FormConnectionFormValues>({ name: "scheduleType" });
  const willSyncAfterCreation = watchedScheduleType === ConnectionScheduleType.basic;

  // If the source doesn't select any streams by default, the initial untouched state
  // won't validate that at least one is selected. In this case, a user could submit the form
  // without selecting any streams, which would trigger an error and cause a lousy UX.
  useEffectOnce(() => {
    trigger("syncCatalog.streams");
  });

  return (
    <Box>
      <FlexContainer justifyContent="space-between" alignItems="center" gap="xl">
        <Text color="red" size="lg">
          {errorMessage}
        </Text>
        <Button type="submit" isLoading={isSubmitting} disabled={isSubmitting || !isValid}>
          <FormattedMessage
            id={willSyncAfterCreation ? "onboarding.setUpConnectionNext" : "onboarding.setUpConnection"}
          />
        </Button>
      </FlexContainer>
    </Box>
  );
};
