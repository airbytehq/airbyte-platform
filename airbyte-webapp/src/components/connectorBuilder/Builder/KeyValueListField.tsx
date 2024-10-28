import React, { ReactNode } from "react";
import { FieldPath } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";

import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { getLabelAndTooltip } from "./manifestHelpers";
import { useBuilderWatchArrayWithPreview } from "../preview";
import { BuilderState, concatPath } from "../types";

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
          path={concatPath(path, "0")}
        />
      </FlexItem>
      <FlexItem grow>
        <BuilderFieldWithInputs
          label={formatMessage({ id: "connectorBuilder.value" })}
          type="string"
          path={concatPath(path, "1")}
        />
      </FlexItem>
      <RemoveButton onClick={onRemove} />
    </FlexContainer>
  );
};

interface KeyValueListFieldProps {
  path: FieldPath<BuilderState>;
  label?: string;
  tooltip?: ReactNode;
  manifestPath?: string;
  optional?: boolean;
  omitInterpolationContext?: boolean;
}

export const KeyValueListField: React.FC<KeyValueListFieldProps> = ({
  path,
  label,
  tooltip,
  manifestPath,
  optional,
  omitInterpolationContext = false,
}) => {
  const { label: finalLabel, tooltip: finalTooltip } = getLabelAndTooltip(
    label,
    tooltip,
    manifestPath,
    path,
    false,
    omitInterpolationContext
  );
  const { fieldValue: fields, append, remove } = useBuilderWatchArrayWithPreview(path);

  return (
    <GroupControls
      label={<ControlLabels label={finalLabel} infoTooltipContent={finalTooltip} optional={optional} />}
      control={
        <Button type="button" variant="secondary" onClick={() => append([["", ""]])}>
          <FormattedMessage id="connectorBuilder.addKeyValue" />
        </Button>
      }
    >
      {fields.map((keyValue: { id: React.Key | null | undefined; 0: string; 1: string }, keyValueIndex: number) => (
        <KeyValueInput
          key={keyValue.id}
          path={`${path}.${keyValueIndex}`}
          onRemove={() => {
            remove(keyValueIndex);
          }}
        />
      ))}
    </GroupControls>
  );
};
