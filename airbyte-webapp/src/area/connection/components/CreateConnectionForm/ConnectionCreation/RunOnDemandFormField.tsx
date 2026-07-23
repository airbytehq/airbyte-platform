import uniqueId from "lodash/uniqueId";
import { useCallback, useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ControlLabels } from "components/ui/LabeledControl";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";
import { useModalService } from "core/services/Modal";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

export const RunOnDemandFormField: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const canManageOnDemand = useGeneratedIntent(Intent.ManageConnectionOnDemandCapacity);
  const { openModal } = useModalService();

  const handleToggle = useCallback(
    async (checked: boolean, fieldOnChange: (value: boolean) => void) => {
      if (!checked) {
        fieldOnChange(false);
        return;
      }

      const result = await openModal<void>({
        size: "md",
        title: <FormattedMessage id="connectionForm.runOnDemand.modal.title" />,
        content: ({ onComplete, onCancel }) => (
          <>
            <ModalBody>
              <Text>
                <FormattedMessage id="connectionForm.runOnDemand.modal.body" />
              </Text>
            </ModalBody>
            <ModalFooter>
              <FlexContainer justifyContent="flex-end" gap="md">
                <Button variant="secondary" onClick={onCancel}>
                  <FormattedMessage id="form.cancel" />
                </Button>
                <Button onClick={() => onComplete(undefined)}>
                  <FormattedMessage id="connectionForm.runOnDemand.modal.submit" />
                </Button>
              </FlexContainer>
            </ModalFooter>
          </>
        ),
      });

      if (result.type === "completed") {
        fieldOnChange(true);
      }
    },
    [openModal]
  );

  return (
    <Controller
      name="onDemandEnabled"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="connectionForm.runOnDemand.title" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="connectionForm.runOnDemand.description" />
                </Text>
              </FlexContainer>
            }
          />
          {!canManageOnDemand ? (
            <Tooltip control={<Switch id={controlId} checked={field.value ?? false} size="lg" disabled />}>
              <FormattedMessage id="connectionForm.runOnDemand.adminOnly.tooltip" />
            </Tooltip>
          ) : (
            <Switch
              id={controlId}
              checked={field.value ?? false}
              onChange={(e) => handleToggle(e.target.checked, field.onChange)}
              size="lg"
              disabled={disabled}
            />
          )}
        </FormFieldLayout>
      )}
    />
  );
};
