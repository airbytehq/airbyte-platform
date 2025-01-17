import uniq from "lodash/uniq";
import uniqueId from "lodash/uniqueId";
import { useCallback, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { LabelInfo } from "components";
import { ControlLabels } from "components/LabeledControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";
import { PropertyError } from "views/Connector/ConnectorForm/components/Property/PropertyError";
import { useConnectorForm } from "views/Connector/ConnectorForm/connectorFormContext";

export const API_RESOURCES = {
  default: {
    memory: 2,
    cpu: 1,
  },
  large: {
    memory: 3,
    cpu: 2,
  },
  memoryIntensive: {
    memory: 6,
    cpu: 2,
  },
  maximum: {
    memory: 8,
    cpu: 4,
  },
};

export const DB_RESOURCES = {
  default: {
    memory: 2,
    cpu: 2,
  },
  large: {
    memory: 3,
    cpu: 3,
  },
  memoryIntensive: {
    memory: 6,
    cpu: 3,
  },
  maximum: {
    memory: 8,
    cpu: 4,
  },
};

const getOptions = (connectorType: "api" | "database") => {
  const values = connectorType === "api" ? API_RESOURCES : DB_RESOURCES;

  return Object.entries(values).map(([size, value]) => {
    return {
      value,
      label: (
        <Text>
          <FormattedMessage
            id="form.resourceAllocation.label"
            values={{
              size,
              cpu: value.cpu,
              memory: value.memory,
              details: (...details: React.ReactNode[]) => (
                <Text color="grey" as="span">
                  {details}
                </Text>
              ),
            }}
          />
        </Text>
      ),
    };
  });
};

export const getConnectorType = (selectedConnectorDefinition?: ConnectorDefinition) => {
  if (!selectedConnectorDefinition) {
    return undefined;
  }

  return isSourceDefinition(selectedConnectorDefinition)
    ? selectedConnectorDefinition.sourceType === "file"
      ? "database"
      : selectedConnectorDefinition.sourceType === "custom"
      ? "api"
      : selectedConnectorDefinition.sourceType
    : "database";
};

export const useConnectorResourceAllocation = () => {
  const supportsResourceAllocation = useFeature(FeatureItem.ConnectorResourceAllocation);

  const isHiddenResourceAllocationField = useCallback(
    (fieldPath: string) => {
      // we want to hide the resourceAllocation sub-fields
      return supportsResourceAllocation && fieldPath.startsWith("resourceAllocation.");
    },
    [supportsResourceAllocation]
  );
  return { isHiddenResourceAllocationField };
};

/**
 * TODO: when we add support for reading the current value from the SourceRead/DestinationRead
 * we will want to (a) map from the current value to one of the selectable options AND ALSO
 * (b) support values that do not fit the preconfigured options (and just show them in the UI).
 * We do not need to support a custom input yet, though.
 */
export const ResourceAllocationMenu: React.FC = () => {
  const { selectedConnectorDefinition } = useConnectorForm();
  const [controlId] = useState(`resource-listbox-control-${uniqueId()}`);

  const { getValues, setValue, formState, getFieldState } = useFormContext<ConnectorFormValues>();
  const meta = getFieldState("resourceAllocation", formState);

  const connectorType = getConnectorType(selectedConnectorDefinition);
  if (!connectorType) {
    return null;
  }

  const options = getOptions(connectorType);

  const errorMessage = Array.isArray(meta.error) ? (
    <FlexContainer direction="column" gap="none">
      {uniq(meta.error.map((error) => error?.message).filter(Boolean)).map((errorMessage, index) => {
        return <PropertyError key={index}>{errorMessage}</PropertyError>;
      })}
    </FlexContainer>
  ) : !!meta.error?.message ? (
    <PropertyError>{meta.error.message}</PropertyError>
  ) : null;

  return (
    <Box pb="xl" data-testid="resourceAllocationMenu">
      <ControlLabels
        htmlFor={controlId}
        label={<FormattedMessage id="form.resourceAllocation" />}
        optional
        infoTooltipContent={
          <LabelInfo
            label={<FormattedMessage id="form.resourceAllocation" />}
            description={
              <FlexContainer direction="column" gap="sm">
                <Text inverseColor>
                  <FormattedMessage id="form.resourceAllocation.tooltip" />
                </Text>
                <Text inverseColor>
                  <FormattedMessage id="form.resourceAllocation.tooltip.note" />
                </Text>
              </FlexContainer>
            }
          />
        }
      />
      <ListBox
        id={controlId}
        options={options}
        selectedValue={getValues("resourceAllocation")}
        onSelect={(selectedValue) => setValue("resourceAllocation", selectedValue, { shouldDirty: true })}
      />
      {errorMessage}
    </Box>
  );
};
