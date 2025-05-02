import uniq from "lodash/uniq";
import uniqueId from "lodash/uniqueId";
import { useCallback, useState } from "react";
import { useFormContext, Controller } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { LabelInfo } from "components";
import { ControlLabels } from "components/LabeledControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { JobType, ScopedResourceRequirements } from "core/api/types/AirbyteClient";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useIsAirbyteEmbeddedContext } from "core/services/embedded";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";
import { PropertyError } from "views/Connector/ConnectorForm/components/Property/PropertyError";
import { useConnectorForm } from "views/Connector/ConnectorForm/connectorFormContext";

export const API_RESOURCE_DEFAULTS: Record<string, SimpleResourceRequirement> = {
  default: {
    memory: "2Gi",
    cpu: "1",
  },
  large: {
    memory: "3Gi",
    cpu: "2",
  },
  memoryIntensive: {
    memory: "6Gi",
    cpu: "2",
  },
  maximum: {
    memory: "8Gi",
    cpu: "4",
  },
};

export const DB_RESOURCE_DEFAULTS = {
  default: {
    memory: "2Gi",
    cpu: "2",
  },
  large: {
    memory: "3Gi",
    cpu: "3 ",
  },
  memoryIntensive: {
    memory: "6Gi",
    cpu: "3",
  },
  maximum: {
    memory: "8Gi",
    cpu: "4",
  },
};

export const getResourceOptions = (selectedConnectorDefinition: ConnectorDefinition) => {
  const connectorType = getConnectorType(selectedConnectorDefinition);
  const hardcodedValues = connectorType === "api" ? API_RESOURCE_DEFAULTS : DB_RESOURCE_DEFAULTS;

  const definitionResources = selectedConnectorDefinition.resourceRequirements;
  const valuesToUse = {
    ...hardcodedValues,
    default: {
      memory:
        definitionResources?.jobSpecific?.[0]?.resourceRequirements?.memory_request ?? hardcodedValues.default.memory,
      cpu: definitionResources?.jobSpecific?.[0]?.resourceRequirements?.cpu_request ?? hardcodedValues.default.cpu,
    },
  };

  return Object.entries(valuesToUse).map(([size, value]) => {
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

export const getConnectorType = (selectedConnectorDefinition: ConnectorDefinition) =>
  isSourceDefinition(selectedConnectorDefinition)
    ? selectedConnectorDefinition.sourceType === "file"
      ? "database"
      : selectedConnectorDefinition.sourceType === "custom"
      ? "api"
      : selectedConnectorDefinition.sourceType
    : "database";

export const useConnectorResourceAllocation = () => {
  const supportsResourceAllocation = useFeature(FeatureItem.ConnectorResourceAllocation);
  const isEmbedded = useIsAirbyteEmbeddedContext();

  const isHiddenResourceAllocationField = useCallback(
    (fieldPath: string) => {
      // we want to hide the resourceAllocation sub-fields
      return supportsResourceAllocation && !isEmbedded && fieldPath.startsWith("resourceAllocation.");
    },
    [supportsResourceAllocation, isEmbedded]
  );
  return { isHiddenResourceAllocationField };
};

interface SimpleResourceRequirement {
  memory: string;
  cpu: string;
}

const toScopedRequirements = (simple?: SimpleResourceRequirement): ScopedResourceRequirements | undefined => {
  if (!simple) {
    return undefined;
  }

  return {
    jobSpecific: [
      {
        jobType: JobType.sync,
        resourceRequirements: {
          memory_request: simple.memory,
          cpu_request: simple.cpu,
        },
      },
    ],
  };
};

const fromScopedRequirements = (scoped?: ScopedResourceRequirements): SimpleResourceRequirement | undefined => {
  if (!scoped?.jobSpecific?.[0]?.resourceRequirements) {
    return undefined;
  }
  const syncJob = scoped.jobSpecific.find((job) => job.jobType === JobType.sync);
  if (!syncJob) {
    return undefined;
  }
  return {
    memory: syncJob.resourceRequirements.memory_request ?? "",
    cpu: syncJob.resourceRequirements.cpu_request ?? "",
  };
};

export const ResourceAllocationMenu: React.FC = () => {
  const { selectedConnectorDefinition } = useConnectorForm();
  const [controlId] = useState(`resource-listbox-control-${uniqueId()}`);

  const { control, formState, getFieldState } = useFormContext<ConnectorFormValues>();
  const meta = getFieldState("resourceAllocation", formState);

  const options = getResourceOptions(selectedConnectorDefinition);

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
      <Controller
        name="resourceAllocation"
        control={control}
        render={({ field: { onChange, value } }) => (
          <ListBox
            id={controlId}
            options={options}
            selectedValue={fromScopedRequirements(value)}
            onSelect={(selectedValue: SimpleResourceRequirement) => {
              onChange(toScopedRequirements(selectedValue));
            }}
          />
        )}
      />
      {errorMessage}
    </Box>
  );
};
