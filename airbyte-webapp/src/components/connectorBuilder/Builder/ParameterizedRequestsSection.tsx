import { useIntl } from "react-intl";
import ReactMarkdown from "react-markdown";

import GroupControls from "components/GroupControls";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf } from "./BuilderOneOf";
import { InjectIntoFields } from "./InjectIntoFields";
import { ToggleGroupField } from "./ToggleGroupField";
import { LIST_PARTITION_ROUTER, StreamPathFn, BuilderParameterizedRequests } from "../types";

interface ParameterizedRequestsSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

const EMPTY_PARAMETERIZED_REQUEST: BuilderParameterizedRequests = {
  type: LIST_PARTITION_ROUTER,
  values: { type: "list", value: [] },
  cursor_field: "",
};

export const ParameterizedRequestsSection: React.FC<ParameterizedRequestsSectionProps> = ({
  streamFieldPath,
  currentStreamIndex,
}) => {
  const { formatMessage } = useIntl();

  return (
    <BuilderCard
      docLink={links.connectorBuilderParameterizedRequests}
      label="Parameterized Requests"
      tooltip="Configure how to parameterize requests with a list of values. This means that for each value in the list, a separate request will be made for the current stream with the list value injected into the request."
      toggleConfig={{
        path: streamFieldPath("parameterizedRequests"),
        defaultValue: [EMPTY_PARAMETERIZED_REQUEST],
      }}
      copyConfig={{
        path: "parameterizedRequests",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromParameterizedRequestsTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToParameterizedRequestsTitle" }),
      }}
    >
      <BuilderList
        addButtonLabel={formatMessage({ id: "connectorBuilder.addNewParameterizedRequest" })}
        basePath={streamFieldPath("parameterizedRequests")}
        emptyItem={EMPTY_PARAMETERIZED_REQUEST}
      >
        {({ buildPath }) => (
          <GroupControls>
            <BuilderOneOf<BuilderParameterizedRequests["values"]>
              path={buildPath("values")}
              manifestPath="ListPartitionRouter.properties.values"
              label="Parameter Values"
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
                      tooltip={
                        <ReactMarkdown>
                          {
                            "Reference an array user input here to allow the user to specify the values to iterate over, e.g. `{{ config['user_input_name'] }}`"
                          }
                        </ReactMarkdown>
                      }
                      pattern={"{{ config['user_input_name'] }}"}
                    />
                  ),
                },
              ]}
            />
            <BuilderFieldWithInputs
              type="string"
              path={buildPath("cursor_field")}
              label="Current Parameter Value Identifier"
              tooltip={
                <ReactMarkdown>
                  {
                    "The name of field used to reference a parameter value. The parameter value can be accessed with string interpolation, e.g. `{{ stream_partition['my_key'] }}` where `my_key` is the Current Parameter Value Identifier."
                  }
                </ReactMarkdown>
              }
            />
            <ToggleGroupField<RequestOption>
              label="Inject Parameter Value into outgoing HTTP Request"
              tooltip="Optionally configures how the parameter value will be sent in requests to the source API"
              fieldPath={buildPath("request_option")}
              initialValues={{
                inject_into: "request_parameter",
                type: "RequestOption",
                field_name: "",
              }}
            >
              <InjectIntoFields
                path={buildPath("request_option")}
                descriptor="parameter value"
                excludeValues={["path"]}
              />
            </ToggleGroupField>
          </GroupControls>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
