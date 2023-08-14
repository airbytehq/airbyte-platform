import { useIntl } from "react-intl";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { InjectIntoFields } from "./InjectIntoFields";
import { StreamReferenceField } from "./StreamReferenceField";
import { ToggleGroupField } from "./ToggleGroupField";
import {
  LIST_PARTITION_ROUTER,
  SUBSTREAM_PARTITION_ROUTER,
  StreamPathFn,
  BuilderListPartitionRouter,
  BuilderSubstreamPartitionRouter,
} from "../types";

interface PartitionSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

const EMPTY_LIST_PARTITION_ROUTER: BuilderListPartitionRouter = {
  type: LIST_PARTITION_ROUTER,
  values: { type: "list", value: [] },
  cursor_field: "",
};

export const PartitionSection: React.FC<PartitionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();

  const getSlicingOptions = (
    buildPath: (path: string) => string
  ): Array<OneOfOption<BuilderListPartitionRouter | BuilderSubstreamPartitionRouter>> => [
    {
      label: "List",
      default: {
        type: LIST_PARTITION_ROUTER,
        values: { type: "list", value: [] },
        cursor_field: "",
      },
      children: (
        <>
          <BuilderOneOf<BuilderListPartitionRouter["values"]>
            path={buildPath("values")}
            manifestPath="ListPartitionRouter.properties.values"
            options={[
              {
                label: "Value List",
                default: { type: "list", value: [] },
                children: <BuilderField type="array" path={buildPath("values.value")} label="Value List" />,
              },
              {
                label: "User Input",
                default: { type: "variable", value: "" },
                children: (
                  <BuilderFieldWithInputs
                    type="string"
                    path={buildPath("values.value")}
                    label="Value"
                    tooltip="Reference an array user input here to allow the user to specify the values to iterate over: {{ config['user_input_name'] }}"
                    pattern={"{{ config['user_input_name'] }}"}
                  />
                ),
              },
            ]}
          />
          <BuilderFieldWithInputs
            type="string"
            path={buildPath("cursor_field")}
            manifestPath="ListPartitionRouter.properties.cursor_field"
          />
          <ToggleGroupField<RequestOption>
            label="Inject Partition Value into outgoing HTTP Request"
            tooltip="Optionally configures how the partition value will be sent in requests to the source API"
            fieldPath={buildPath("request_option")}
            initialValues={{
              inject_into: "request_parameter",
              type: "RequestOption",
              field_name: "",
            }}
          >
            <InjectIntoFields path={buildPath("request_option")} descriptor="slice value" excludeValues={["path"]} />
          </ToggleGroupField>
        </>
      ),
    },
    {
      label: "Substream",
      default: {
        type: SUBSTREAM_PARTITION_ROUTER,
        parent_key: "",
        partition_field: "",
        parentStreamReference: "",
      },
      children: (
        <>
          <StreamReferenceField
            currentStreamIndex={currentStreamIndex}
            path={buildPath("parentStreamReference")}
            label="Parent stream"
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
          />
        </>
      ),
    },
  ];

  return (
    <BuilderCard
      docLink={links.connectorBuilderPartitioning}
      label="Partitioning"
      tooltip="Configure how to partition a stream into subsets of records and iterate over the data. If multiple partition routers are defined, the cartesian product of the slices from all routers is formed."
      toggleConfig={{
        path: streamFieldPath("partitionRouter"),
        defaultValue: [EMPTY_LIST_PARTITION_ROUTER],
      }}
      copyConfig={{
        path: "partitionRouter",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromPartitionRouterTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToPartitionRouterTitle" }),
      }}
    >
      <BuilderList
        addButtonLabel={formatMessage({ id: "connectorBuilder.addNewPartitionRouter" })}
        basePath={streamFieldPath("partitionRouter")}
        emptyItem={EMPTY_LIST_PARTITION_ROUTER}
      >
        {({ buildPath }) => (
          <BuilderOneOf<BuilderListPartitionRouter | BuilderSubstreamPartitionRouter>
            path={buildPath("")}
            label="Partition Router"
            manifestOptionPaths={["ListPartitionRouter", "ParentStreamConfig"]}
            tooltip="Method to use on this router"
            options={getSlicingOptions(buildPath)}
          />
        )}
      </BuilderList>
    </BuilderCard>
  );
};
