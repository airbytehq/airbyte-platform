import React from "react";
import { useIntl } from "react-intl";

import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import { BuilderTransformation } from "../types";

interface TransformationSectionProps {
  streamFieldPath: <T extends string>(fieldPath: T) => `streams.${number}.${T}`;
  currentStreamIndex: number;
}

export const TransformationSection: React.FC<TransformationSectionProps> = ({
  streamFieldPath,
  currentStreamIndex,
}) => {
  const { formatMessage } = useIntl();

  const getTransformationOptions = (buildPath: (path: string) => string): Array<OneOfOption<BuilderTransformation>> => [
    {
      label: "Remove field",
      default: {
        type: "remove",
        path: [],
      },
      children: (
        <BuilderField type="array" path={buildPath("path")} label="Path" tooltip="Path to the field to remove" />
      ),
    },
    {
      label: "Add field",
      default: {
        type: "add",
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

  return (
    <BuilderCard
      docLink={links.connectorBuilderTransformations}
      label={getLabelByManifest("DeclarativeStream.properties.transformations")}
      tooltip={getDescriptionByManifest("DeclarativeStream.properties.transformations")}
      toggleConfig={{
        path: streamFieldPath("transformations"),
        defaultValue: [
          {
            type: "remove",
            path: [],
          },
        ],
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
          <BuilderOneOf<BuilderTransformation>
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
