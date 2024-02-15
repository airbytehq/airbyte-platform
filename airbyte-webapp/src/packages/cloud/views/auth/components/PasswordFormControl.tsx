import React from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormLabel } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

interface PasswordFormControlProps {
  label: string;
}

export const PasswordFormControl: React.FC<PasswordFormControlProps> = ({ label }) => {
  const { formatMessage } = useIntl();
  const {
    errors: { password: error },
    dirtyFields: { password: isDirty },
  } = useFormState({ name: "password" });
  const { register } = useFormContext();

  return (
    <>
      <Box mb="lg">
        <FormLabel label={formatMessage({ id: label })} htmlFor="password" />
        <Input
          {...register("password")}
          type="password"
          placeholder={formatMessage({ id: "login.password.placeholder" })}
          error={Boolean(error)}
          autoComplete="new-password"
        />
      </Box>
      <FlexContainer gap="sm" alignItems="center">
        <Icon
          type={error ? "cross" : "check"}
          color={error ? "error" : !isDirty ? "action" : "success"}
          withBackground
          size="sm"
        />
        <Text size="sm" color={error ? "red" : "darkBlue"}>
          <FormattedMessage id="signup.password.minLength" />
        </Text>
      </FlexContainer>
    </>
  );
};
