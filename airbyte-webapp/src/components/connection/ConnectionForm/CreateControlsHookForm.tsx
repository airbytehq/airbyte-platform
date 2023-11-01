import React from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConnectionHookFormService } from "hooks/services/ConnectionForm/ConnectionHookFormService";

import { HookFormConnectionFormValues } from "./hookFormConfig";

/**
 * react-hook-form create connection and error form controls component
 * will replace CreateControls.tsx in future
 * @see CreateControls
 */
export const CreateControlsHookForm: React.FC = () => {
  const { isSubmitting, isValid, errors } = useFormState<HookFormConnectionFormValues>();
  const { trigger } = useFormContext<HookFormConnectionFormValues>();
  const { getErrorMessage } = useConnectionHookFormService();
  const errorMessage = getErrorMessage(isValid, errors);

  // If the source doesn't select any streams by default, the initial untouched state
  // won't validate that at least one is selected. In this case, a user could submit the form
  // without selecting any streams, which would trigger an error and cause a lousy UX.
  useEffectOnce(() => {
    trigger("syncCatalog.streams");
  });

  return (
    <Box mt="md">
      <FlexContainer justifyContent="space-between" gap="xl">
        <Text color="red" size="lg">
          {errorMessage}
        </Text>
        <Button type="submit" isLoading={isSubmitting} disabled={isSubmitting || !isValid}>
          <FormattedMessage id="onboarding.setUpConnection" />
        </Button>
      </FlexContainer>
    </Box>
  );
};
