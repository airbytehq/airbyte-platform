import classNames from "classnames";
import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import React, { useCallback, useMemo, useState } from "react";
import { useWatch } from "react-hook-form";
import { useIntl } from "react-intl";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal } from "components/ui/Modal";
import { Pre } from "components/ui/Pre";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./LinkComponentsToggle.module.scss";
import { useRefsHandler } from "./RefsHandler";
import { AirbyteJsonSchema, displayName } from "./utils";

interface LinkComponentsToggleProps {
  path: string;
  fieldSchema: AirbyteJsonSchema;
}

export const LinkComponentsToggle: React.FC<LinkComponentsToggleProps> = ({ path, fieldSchema }) => {
  const { formatMessage } = useIntl();
  const { getReferenceInfo, handleLinkAction, handleUnlinkAction, getRefTargetPathForField } = useRefsHandler();
  const currentValue = useWatch({ name: path });
  const refTargetPath = useMemo(() => getRefTargetPathForField(path), [getRefTargetPathForField, path]);
  const refTargetValue = useWatch({ name: refTargetPath ?? "" });
  const [isModalOpen, setIsModalOpen] = useState(false);

  const fieldDisplayName = useMemo(() => {
    return displayName(path, fieldSchema?.title);
  }, [path, fieldSchema?.title]);

  const currentValueExists = useMemo(() => currentValue !== undefined && !isEmpty(currentValue), [currentValue]);

  // Get reference information
  const refInfo = getReferenceInfo(path);

  // Check if this field is linkable
  const isLinkable = useMemo(
    () => fieldSchema.linkable === true && (refInfo.type === "source" || refInfo.type === "none") && !!refTargetPath,
    [fieldSchema.linkable, refInfo.type, refTargetPath]
  );

  // Determine if the toggle should be on (field is a source or target)
  const isToggleOn = refInfo.type === "source";

  const closeModal = useCallback(() => {
    setIsModalOpen(false);
  }, []);

  const selectTargetValue = useCallback(() => {
    handleLinkAction(path, false);
    closeModal();
  }, [closeModal, handleLinkAction, path]);

  const selectFieldValue = useCallback(() => {
    handleLinkAction(path, true);
    closeModal();
  }, [closeModal, handleLinkAction, path]);

  // Handle toggle change
  const handleToggleChange = useCallback(
    (toggleState: boolean) => {
      if (!toggleState) {
        handleUnlinkAction(path);
        return;
      }

      // If target already has a value, show modal
      if (currentValueExists && refTargetValue && !isEqual(refTargetValue, currentValue)) {
        setIsModalOpen(true);
      } else {
        // No existing value at target, just create the reference
        handleLinkAction(path);
      }
    },
    [currentValueExists, refTargetValue, currentValue, handleUnlinkAction, path, handleLinkAction]
  );

  // If not linkable, don't render anything
  if (!isLinkable) {
    return null;
  }

  const isDisabled = !currentValueExists && !refTargetValue;

  return (
    <>
      <div className={styles.container}>
        <Tooltip
          placement="top"
          control={
            <FlexContainer
              className={classNames(styles.toggleContainer, {
                [styles.disabled]: isDisabled,
              })}
              alignItems="center"
              gap="sm"
              onClick={() => !isDisabled && handleToggleChange(!isToggleOn)}
            >
              <Icon type="link" size="sm" color={isToggleOn ? "primary" : "disabled"} />
              <FlexContainer className={styles.switchWrapper} alignItems="center">
                <Switch checked={isToggleOn} onChange={(e) => handleToggleChange(e.target.checked)} size="sm" />
              </FlexContainer>
            </FlexContainer>
          }
        >
          {isDisabled
            ? formatMessage({ id: "form.linkComponentsToggle.disabled" }, { fieldName: fieldDisplayName })
            : isToggleOn
            ? formatMessage({ id: "form.linkComponentsToggle.toggled.on" }, { fieldName: fieldDisplayName })
            : formatMessage({ id: "form.linkComponentsToggle.toggled.off" }, { fieldName: fieldDisplayName })}
        </Tooltip>
      </div>

      {isModalOpen && (
        <SharedValueModal
          fieldDisplayName={fieldDisplayName ?? path.split(".").at(-1) ?? "value"}
          onCancel={closeModal}
          fieldValue={currentValue}
          targetValue={refTargetValue}
          selectFieldValue={selectFieldValue}
          selectTargetValue={selectTargetValue}
        />
      )}
    </>
  );
};

interface SharedValueModalProps {
  fieldDisplayName: string;
  onCancel: () => void;
  fieldValue: unknown;
  targetValue: unknown;
  selectFieldValue: () => void;
  selectTargetValue: () => void;
}
type SharedValueOption = "useFieldValue" | "useTargetValue";

const SharedValueModal = ({
  fieldDisplayName,
  onCancel,
  fieldValue,
  targetValue,
  selectFieldValue,
  selectTargetValue,
}: SharedValueModalProps) => {
  const { formatMessage } = useIntl();
  const [selectedOption, setSelectedOption] = useState<"useFieldValue" | "useTargetValue">("useTargetValue");

  return (
    <Modal
      title={formatMessage({ id: "form.linkComponentsToggle.confirmationTitle" }, { fieldName: fieldDisplayName })}
      onCancel={onCancel}
      size="sm"
    >
      <FlexContainer className={styles.modalContent} direction="column" gap="xl">
        <Text>
          {formatMessage({ id: "form.linkComponentsToggle.confirmationText" }, { fieldName: fieldDisplayName })}
        </Text>
        <RadioButtonTiles<SharedValueOption>
          name="sharedValueOption"
          options={[
            {
              value: "useTargetValue",
              label: formatMessage({ id: "form.linkComponentsToggle.useTargetValue" }, { fieldName: fieldDisplayName }),
              description: <Pre>{JSON.stringify(targetValue, null, 2)}</Pre>,
            },
            {
              value: "useFieldValue",
              label: formatMessage({ id: "form.linkComponentsToggle.useFieldValue" }, { fieldName: fieldDisplayName }),
              description: <Pre>{JSON.stringify(fieldValue, null, 2)}</Pre>,
            },
          ]}
          selectedValue={selectedOption}
          onSelectRadioButton={setSelectedOption}
          direction="column"
        />
        <FlexContainer justifyContent="flex-end">
          <Button type="button" variant="secondary" onClick={onCancel}>
            {formatMessage({ id: "form.cancel" })}
          </Button>
          <Button
            type="button"
            onClick={() => {
              if (selectedOption === "useFieldValue") {
                selectFieldValue();
              } else {
                selectTargetValue();
              }
            }}
          >
            {formatMessage({ id: "form.confirm" })}
          </Button>
        </FlexContainer>
      </FlexContainer>
    </Modal>
  );
};
