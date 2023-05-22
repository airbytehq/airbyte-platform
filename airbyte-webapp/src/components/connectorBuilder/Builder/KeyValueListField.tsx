import React, { ReactNode, useCallback } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { getLabelAndTooltip } from "./manifestHelpers";
import { RemoveButton } from "./RemoveButton";

interface KeyValueInputProps {
  path: string;
  onRemove: () => void;
}

const KeyValueInput: React.FC<KeyValueInputProps> = ({ onRemove, path }) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer gap="xl" alignItems="flex-start">
      <FlexItem grow>
        <BuilderFieldWithInputs
          label={formatMessage({ id: "connectorBuilder.key" })}
          type="string"
          path={`${path}.0`}
        />
      </FlexItem>
      <FlexItem grow>
        <BuilderFieldWithInputs
          label={formatMessage({ id: "connectorBuilder.value" })}
          type="string"
          path={`${path}.1`}
        />
      </FlexItem>
      <RemoveButton onClick={onRemove} />
    </FlexContainer>
  );
};

interface KeyValueListFieldProps {
  path: string;
  label?: string;
  tooltip?: ReactNode;
  manifestPath?: string;
}

export const KeyValueListField: React.FC<KeyValueListFieldProps> = ({ path, label, tooltip, manifestPath }) => {
  const keyValueList = useWatch({ name: path });
  const { setValue } = useFormContext();
  const setKeyValueList = useCallback(
    (newValue: Array<[string, string]>) => {
      setValue(path, newValue);
    },
    [path, setValue]
  );
  const { label: finalLabel, tooltip: finalTooltip } = getLabelAndTooltip(label, tooltip, manifestPath, path);

  return (
    <KeyValueList
      label={finalLabel}
      tooltip={finalTooltip}
      keyValueList={keyValueList}
      setKeyValueList={setKeyValueList}
      path={path}
    />
  );
};

const KeyValueList = React.memo(
  ({
    keyValueList,
    setKeyValueList,
    label,
    tooltip,
    path,
  }: KeyValueListFieldProps & {
    keyValueList: Array<[string, string]>;
    setKeyValueList: (val: Array<[string, string]>) => void;
  }) => {
    return (
      <GroupControls
        label={<ControlLabels label={label} infoTooltipContent={tooltip} />}
        control={
          <Button type="button" variant="secondary" onClick={() => setKeyValueList([...keyValueList, ["", ""]])}>
            <FormattedMessage id="connectorBuilder.addKeyValue" />
          </Button>
        }
      >
        {keyValueList.map((_keyValue, keyValueIndex) => (
          <KeyValueInput
            key={keyValueIndex}
            path={`${path}.${keyValueIndex}`}
            onRemove={() => {
              const updatedList = keyValueList.filter((_, index) => index !== keyValueIndex);
              setKeyValueList(updatedList);
            }}
          />
        ))}
      </GroupControls>
    );
  }
);
KeyValueList.displayName = "KeyValueList";
