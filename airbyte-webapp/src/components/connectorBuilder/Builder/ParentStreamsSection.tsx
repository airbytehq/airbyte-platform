import { useIntl } from "react-intl";
import ReactMarkdown from "react-markdown";

import GroupControls from "components/GroupControls";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { InjectIntoFields } from "./InjectIntoFields";
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
      label="Parent Stream"
      tooltip="Configure another stream to be the parent of this stream. This means that for each record in the parent stream, a separate request will be made for the current stream with the parent record's data accessible for injection or interpolation."
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
              label="Parent Stream"
              tooltip="The stream to read records from. Make sure there are no cyclic dependencies between streams"
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
                  {
                    "The identifier that should be used for referencing the parent key value in interpolation. For example, if this field is set to `parent_id`, then the parent key value can be referenced in interpolation as `{{ stream_partition.parent_id }}`"
                  }
                </ReactMarkdown>
              }
            />
            <ToggleGroupField<RequestOption>
              label="Inject Parent Key into outgoing HTTP Request"
              tooltip="Optionally configures how the parent key will be sent in requests to the source API"
              fieldPath={buildPath("request_option")}
              initialValues={{
                inject_into: "request_parameter",
                type: "RequestOption",
                field_name: "",
              }}
            >
              <InjectIntoFields path={buildPath("request_option")} descriptor="parent key" excludeValues={["path"]} />
            </ToggleGroupField>
          </GroupControls>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
