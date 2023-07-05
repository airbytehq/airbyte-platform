import React from "react";
import { useFieldArray, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormControl } from "components/forms";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { InviteUsersFormValues } from "./InviteUsersModal";

export const EmailFormControlList: React.FC = () => {
  const { isValid, isDirty } = useFormState<InviteUsersFormValues>();
  const { fields, remove, append } = useFieldArray<InviteUsersFormValues>({
    name: "users",
  });

  const appendNewRow = () =>
    append({
      email: "",
      role: "admin", // the only role we currently have
    });

  return (
    <>
      {fields.map((field, index) => (
        <FlexContainer key={field.id}>
          <FlexItem grow>
            <FormControl name={`users.${index}.email`} type="text" fieldType="input" placeholder="email@company.com" />
          </FlexItem>
          <Button
            type="button"
            size="sm"
            disabled={fields.length < 2}
            onClick={() => remove(index)}
            variant="secondary"
            icon={<Icon type="cross" />}
          />
        </FlexContainer>
      ))}
      <FlexItem alignSelf="flex-start">
        <Button type="button" disabled={!isValid || !isDirty} onClick={appendNewRow} variant="secondary">
          <FormattedMessage id="modals.addUser.button.addUser" />
        </Button>
      </FlexItem>
    </>
  );
};
