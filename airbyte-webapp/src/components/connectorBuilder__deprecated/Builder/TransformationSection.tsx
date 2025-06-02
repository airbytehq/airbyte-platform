import React from "react";
import { useIntl } from "react-intl";

import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import { manifestTransformationsToBuilder } from "../convertManifestToBuilderForm";
import {
  BuilderTransformation,
  DownloadRequesterPathFn,
  AnyDeclarativeStreamPathFn,
  StreamId,
  builderTransformationsToManifest,
} from "../types";
import { StreamFieldPath } from "../utils";

interface TransformationSectionProps {
  streamFieldPath: AnyDeclarativeStreamPathFn | DownloadRequesterPathFn;
  streamId: StreamId;
}

export const TransformationSection: React.FC<TransformationSectionProps> = ({ streamFieldPath, streamId }) => {
  const { formatMessage } = useIntl();

  const getTransformationOptions = (buildPath: (path: string) => string): Array<OneOfOption<BuilderTransformation>> => [
    {
      label: formatMessage({ id: "connectorBuilder.transformation.remove" }),
      default: {
        type: "remove",
        path: [],
      },
      children: (
        <BuilderField
          type="array"
          path={buildPath("path")}
          label={formatMessage({ id: "connectorBuilder.transformation.remove.path.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.transformation.remove.path.tooltip" })}
        />
      ),
    },
    {
      label: formatMessage({ id: "connectorBuilder.transformation.add" }),
      default: {
        type: "add",
        value: "",
        path: [],
      },
      children: (
        <>
          <BuilderField type="array" path={buildPath("path")} manifestPath="AddedFieldDefinition.properties.path" />
          <BuilderField type="jinja" path={buildPath("value")} manifestPath="AddedFieldDefinition.properties.value" />
        </>
      ),
    },
  ];
  const label = getLabelByManifest("DeclarativeStream.properties.transformations");

  return (
    <BuilderCard
      docLink={links.connectorBuilderTransformations}
      label={label}
      tooltip={getDescriptionByManifest("DeclarativeStream.properties.transformations")}
      inputsConfig={{
        toggleable: true,
        path: streamFieldPath("transformations"),
        defaultValue: [
          {
            type: "remove",
            path: [],
          },
        ],
        yamlConfig: {
          builderToManifest: builderTransformationsToManifest,
          manifestToBuilder: manifestTransformationsToBuilder,
        },
      }}
      copyConfig={
        streamId.type === "stream"
          ? {
              path: streamFieldPath("transformations") as StreamFieldPath,
              currentStreamIndex: streamId.index,
              componentName: label,
            }
          : undefined
      }
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
            label={formatMessage({ id: "connectorBuilder.transformation.label" })}
            tooltip={formatMessage({ id: "connectorBuilder.transformation.tooltip" })}
            options={getTransformationOptions(buildPath)}
          />
        )}
      </BuilderList>
    </BuilderCard>
  );
};

TransformationSection.displayName = "TransformationSection";
