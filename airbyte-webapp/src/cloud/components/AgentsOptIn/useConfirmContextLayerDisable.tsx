import React, { useCallback, useId, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useLocalStorage } from "core/utils/useLocalStorage";

const DontAskAgainCheckbox: React.FC<{ onChange: (checked: boolean) => void }> = ({ onChange }) => {
  const [checked, setChecked] = useState(false);
  const checkboxId = useId();

  return (
    <Box pt="xl">
      <FlexContainer alignItems="center" gap="sm">
        <CheckBox
          id={checkboxId}
          checkboxSize="sm"
          checked={checked}
          onChange={(event) => {
            setChecked(event.target.checked);
            onChange(event.target.checked);
          }}
        />
        {/* eslint-disable-next-line jsx-a11y/label-has-associated-control -- eslint loses the input even though it has an "htmlFor" */}
        <label htmlFor={checkboxId}>
          <Text as="span" size="sm">
            <FormattedMessage id="cloud.contextLayer.disableConfirm.dontAskAgain" />
          </Text>
        </label>
      </FlexContainer>
    </Box>
  );
};

// The confirmation modal is a shared singleton, so a second confirmation replaces the first;
// resolving the earlier promise with `false` keeps the first caller's await from hanging.
let pendingResolve: ((proceed: boolean) => void) | null = null;
// useLocalStorage doesn't sync across hook instances, so the "don't ask again" choice made in
// one toggle is mirrored here for sibling instances, which would otherwise still open the dialog.
let skipConfirmationSet = false;
// Key for the checkbox inside the shared modal: incrementing it remounts the checkbox so a
// replaced dialog starts unchecked instead of inheriting the previous dialog's checked state.
let requestId = 0;

export const resetContextLayerDisableConfirmationState = () => {
  pendingResolve = null;
  skipConfirmationSet = false;
  requestId = 0;
};

export const useConfirmContextLayerDisable = () => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const [skipConfirmation, setSkipConfirmation] = useLocalStorage(
    "airbyte_context-layer-skip-disable-confirmation",
    false
  );
  const dontAskAgainRef = useRef(false);

  return useCallback(
    (connectorName: string): Promise<boolean> => {
      if (skipConfirmation || skipConfirmationSet) {
        return Promise.resolve(true);
      }
      pendingResolve?.(false);
      dontAskAgainRef.current = false;
      requestId += 1;
      return new Promise<boolean>((resolve) => {
        pendingResolve = resolve;
        openConfirmationModal({
          title: "cloud.contextLayer.disableConfirm.title",
          text: "cloud.contextLayer.disableConfirm.text",
          textValues: { name: connectorName },
          submitButtonText: "cloud.contextLayer.disableConfirm.submit",
          submitButtonVariant: "danger",
          submitButtonDataId: "context-layer-disable-confirm",
          additionalContent: (
            <DontAskAgainCheckbox key={requestId} onChange={(checked) => (dontAskAgainRef.current = checked)} />
          ),
          onSubmit: () => {
            pendingResolve = null;
            if (dontAskAgainRef.current) {
              setSkipConfirmation(true);
              skipConfirmationSet = true;
            }
            closeConfirmationModal();
            resolve(true);
          },
          onCancel: () => {
            pendingResolve = null;
            resolve(false);
          },
        });
      });
    },
    [skipConfirmation, setSkipConfirmation, openConfirmationModal, closeConfirmationModal]
  );
};
