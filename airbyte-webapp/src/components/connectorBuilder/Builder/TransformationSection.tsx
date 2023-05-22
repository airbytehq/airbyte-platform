import React from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";

import { links } from "utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import { useBuilderWatch } from "../types";

interface PartitionSectionProps {
  streamFieldPath: <T extends string>(fieldPath: T) => `streams.${number}.${T}`;
  currentStreamIndex: number;
}

export const TransformationSection: React.FC<PartitionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const path = streamFieldPath("transformations");
  const value = useBuilderWatch(path, { exact: true });

  const handleToggle = (newToggleValue: boolean) => {
    if (newToggleValue) {
      setValue(path, [
        {
          type: "remove",
          path: [],
        },
      ]);
    } else {
      setValue(path, undefined, { shouldValidate: true });
    }
  };

  const getTransformationOptions = (buildPath: (path: string) => string): OneOfOption[] => [
    {
      label: "Remove field",
      typeValue: "remove",
      default: {
        path: [],
      },
      children: (
        <BuilderField type="array" path={buildPath("path")} label="Path" tooltip="Path to the field to remove" />
      ),
    },
    {
      label: "Add field",
      typeValue: "add",
      default: {
        value: "",
        path: [],
      },
      children: (
        <>
          <BuilderField type="array" path={buildPath("path")} manifestPath="AddedFieldDefinition.properties.path" />
          <BuilderFieldWithInputs
            type="string"
            path={buildPath("value")}
            manifestPath="AddedFieldDefinition.properties.value"
          />
        </>
      ),
    },
  ];
  const toggledOn = value !== undefined;

  return (
    <BuilderCard
      docLink={links.connectorBuilderTransformations}
      label={
        <ControlLabels
          label={getLabelByManifest("DeclarativeStream.properties.transformations")}
          infoTooltipContent={getDescriptionByManifest("DeclarativeStream.properties.transformations")}
        />
      }
      toggleConfig={{
        toggledOn,
        onToggle: handleToggle,
      }}
      copyConfig={{
        path: "transformations",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromTransformationTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToTransformationTitle" }),
      }}
    >
      <BuilderList
        addButtonLabel={formatMessage({ id: "connectorBuilder.addNewTransformation" })}
        basePath={streamFieldPath("transformations")}
        emptyItem={{
          type: "remove",
          path: [],
        }}
      >
        {({ buildPath }) => (
          <BuilderOneOf
            path={buildPath("")}
            label="Transformation"
            tooltip="Add or remove a field"
            options={getTransformationOptions(buildPath)}
          />
        )}
      </BuilderList>
    </BuilderCard>
  );
};

TransformationSection.displayName = "TransformationSection";
