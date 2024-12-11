import { useCallback, useMemo } from "react";
import { useIntl } from "react-intl";
import { ReactMarkdown } from "react-markdown/lib/react-markdown";

import GroupControls from "components/GroupControls";

import { RequestOption, SimpleRetrieverPartitionRouter } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { StreamReferenceField } from "./StreamReferenceField";
import { ToggleGroupField } from "./ToggleGroupField";
import { manifestSubstreamPartitionRouterToBuilder } from "../convertManifestToBuilderForm";
import { StreamPathFn, BuilderParentStream, builderParentStreamsToManifest, useBuilderWatch } from "../types";

interface ParentStreamsSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

const EMPTY_PARENT_STREAM: BuilderParentStream = {
  parent_key: "",
  partition_field: "",
  parentStreamReference: "",
};

export const ParentStreamsSection: React.FC<ParentStreamsSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const label = formatMessage({ id: "connectorBuilder.parentStreams.label" });
  const builderStreams = useBuilderWatch("formValues.streams");

  const parentStreamsToManifest = useCallback(
    (parentStreams: BuilderParentStream[] | undefined) => builderParentStreamsToManifest(parentStreams, builderStreams),
    [builderStreams]
  );

  const streamNameToId: Record<string, string> = useMemo(
    () => builderStreams.reduce((acc, stream) => ({ ...acc, [stream.name]: stream.id }), {}),
    [builderStreams]
  );
  const substreamPartitionRouterToBuilder = useCallback(
    (partitionRouter: SimpleRetrieverPartitionRouter | undefined) =>
      manifestSubstreamPartitionRouterToBuilder(partitionRouter, streamNameToId),
    [streamNameToId]
  );

  return (
    <BuilderCard
      docLink={links.connectorBuilderParentStream}
      label={label}
      tooltip={formatMessage({ id: "connectorBuilder.parentStreams.tooltip" })}
      inputsConfig={{
        toggleable: true,
        path: streamFieldPath("parentStreams"),
        defaultValue: [EMPTY_PARENT_STREAM],
        yamlConfig: {
          builderToManifest: parentStreamsToManifest,
          manifestToBuilder: substreamPartitionRouterToBuilder,
        },
      }}
      copyConfig={{
        path: "parentStreams",
        currentStreamIndex,
        componentName: label,
      }}
    >
      <BuilderList
        addButtonLabel={formatMessage({ id: "connectorBuilder.addNewParentStream" })}
        basePath={streamFieldPath("parentStreams")}
        emptyItem={EMPTY_PARENT_STREAM}
      >
        {({ buildPath }) => (
          <GroupControls>
            <StreamReferenceField
              currentStreamIndex={currentStreamIndex}
              path={buildPath("parentStreamReference")}
              label={formatMessage({ id: "connectorBuilder.parentStreams.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.parentStreams.parentStream.tooltip" })}
            />
            <BuilderField
              type="jinja"
              path={buildPath("parent_key")}
              manifestPath="ParentStreamConfig.properties.parent_key"
            />
            <BuilderField
              type="jinja"
              path={buildPath("partition_field")}
              manifestPath="ParentStreamConfig.properties.partition_field"
              tooltip={
                <ReactMarkdown>
                  {formatMessage({ id: "connectorBuilder.parentStreams.parentStream.partitionField.tooltip" })}
                </ReactMarkdown>
              }
            />
            <BuilderField
              type="boolean"
              path={buildPath("incremental_dependency")}
              label={formatMessage({ id: "connectorBuilder.parentStreams.parentStream.incrementalParent.label" })}
              tooltip={
                <ReactMarkdown>
                  {formatMessage({ id: "connectorBuilder.parentStreams.parentStream.incrementalParent.tooltip" })}
                </ReactMarkdown>
              }
            />
            <ToggleGroupField<RequestOption>
              label={formatMessage({ id: "connectorBuilder.parentStreams.parentStream.requestOption.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.parentStreams.parentStream.requestOption.tooltip" })}
              fieldPath={buildPath("request_option")}
              initialValues={{
                inject_into: "request_parameter",
                type: "RequestOption",
                field_name: "",
              }}
            >
              <BuilderRequestInjection
                path={buildPath("request_option")}
                descriptor={formatMessage({
                  id: "connectorBuilder.parentStreams.parentStream.requestOption.descriptor",
                })}
                excludeValues={["path"]}
              />
            </ToggleGroupField>
          </GroupControls>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
