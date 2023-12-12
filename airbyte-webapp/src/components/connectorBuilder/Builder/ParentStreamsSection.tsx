import { useIntl } from "react-intl";
import ReactMarkdown from "react-markdown";

import GroupControls from "components/GroupControls";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { StreamReferenceField } from "./StreamReferenceField";
import { ToggleGroupField } from "./ToggleGroupField";
import { StreamPathFn, BuilderParentStream } from "../types";

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

  return (
    <BuilderCard
      docLink={links.connectorBuilderParentStream}
      label={formatMessage({ id: "connectorBuilder.parentStreams.label" })}
      tooltip={formatMessage({ id: "connectorBuilder.parentStreams.tooltip" })}
      toggleConfig={{
        path: streamFieldPath("parentStreams"),
        defaultValue: [EMPTY_PARENT_STREAM],
      }}
      copyConfig={{
        path: "parentStreams",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromParentStreamsTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToParentStreamsTitle" }),
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
            <BuilderFieldWithInputs
              type="string"
              path={buildPath("parent_key")}
              manifestPath="ParentStreamConfig.properties.parent_key"
            />
            <BuilderFieldWithInputs
              type="string"
              path={buildPath("partition_field")}
              manifestPath="ParentStreamConfig.properties.partition_field"
              tooltip={
                <ReactMarkdown>
                  {formatMessage({ id: "connectorBuilder.parentStreams.parentStream.partitionField.tooltip" })}
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
