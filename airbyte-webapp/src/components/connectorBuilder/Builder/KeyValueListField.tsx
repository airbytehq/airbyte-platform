import { useField } from "formik";
import React, { useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
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
  label: string;
  tooltip: string;
}

export const KeyValueListField: React.FC<KeyValueListFieldProps> = ({ path, label, tooltip }) => {
  const [{ value: keyValueList }, , { setValue: setKeyValueList }] = useField<Array<[string, string]>>(path);

  // need to wrap the setter into a ref because it will be a new function on every formik state update
  const setKeyValueListRef = useRef(setKeyValueList);
  setKeyValueListRef.current = setKeyValueList;

  return (
    <KeyValueList
      label={label}
      tooltip={tooltip}
      keyValueList={keyValueList}
      setKeyValueList={setKeyValueListRef}
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
    setKeyValueList: React.MutableRefObject<(val: Array<[string, string]>) => void>;
  }) => {
    return (
      <GroupControls
        label={<ControlLabels label={label} infoTooltipContent={tooltip} />}
        control={
          <Button
            type="button"
            variant="secondary"
            onClick={() => setKeyValueList.current([...keyValueList, ["", ""]])}
          >
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
              setKeyValueList.current(updatedList);
            }}
          />
        ))}
      </GroupControls>
    );
  }
);
