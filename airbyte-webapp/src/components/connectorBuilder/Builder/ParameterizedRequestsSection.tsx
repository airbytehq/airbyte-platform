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
import { BuilderRequestInjection } from "./BuilderRequestInjection";
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
      label={formatMessage({ id: "connectorBuilder.parameterizedRequests.label" })}
      tooltip={formatMessage({ id: "connectorBuilder.parameterizedRequests.tooltip" })}
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
        addButtonLabel={formatMessage({ id: "connectorBuilder.parameterizedRequest.addButton" })}
        basePath={streamFieldPath("parameterizedRequests")}
        emptyItem={EMPTY_PARAMETERIZED_REQUEST}
      >
        {({ buildPath }) => (
          <GroupControls>
            <BuilderOneOf<BuilderParameterizedRequests["values"]>
              path={buildPath("values")}
              manifestPath="ListPartitionRouter.properties.values"
              label={formatMessage({ id: "connectorBuilder.parameterizedRequests.values" })}
              options={[
                {
                  label: formatMessage({ id: "connectorBuilder.parameterizedRequests.values.list" }),
                  default: { type: "list", value: [] },
                  children: (
                    <BuilderField
                      type="array"
                      path={buildPath("values.value")}
                      label={formatMessage({ id: "connectorBuilder.parameterizedRequests.values.list" })}
                    />
                  ),
                },
                {
                  label: formatMessage({ id: "connectorBuilder.parameterizedRequests.values.userInput" }),
                  default: { type: "variable", value: "" },
                  children: (
                    <BuilderFieldWithInputs
                      type="string"
                      path={buildPath("values.value")}
                      label={formatMessage({
                        id: "connectorBuilder.parameterizedRequests.values.userInput.value.label",
                      })}
                      tooltip={
                        <ReactMarkdown>
                          {formatMessage({
                            id: "connectorBuilder.parameterizedRequests.values.userInput.value.tooltip",
                          })}
                        </ReactMarkdown>
                      }
                      pattern={formatMessage({
                        id: "connectorBuilder.parameterizedRequests.values.userInput.value.pattern",
                      })}
                    />
                  ),
                },
              ]}
            />
            <BuilderFieldWithInputs
              type="string"
              path={buildPath("cursor_field")}
              label={formatMessage({ id: "connectorBuilder.parameterizedRequests.cursorField.label" })}
              tooltip={
                <ReactMarkdown>
                  {formatMessage({ id: "connectorBuilder.parameterizedRequests.cursorField.tooltip" })}
                </ReactMarkdown>
              }
            />
            <ToggleGroupField<RequestOption>
              label={formatMessage({ id: "connectorBuilder.parameterizedRequests.requestOption.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.parameterizedRequests.requestOption.tooltip" })}
              fieldPath={buildPath("request_option")}
              initialValues={{
                inject_into: "request_parameter",
                type: "RequestOption",
                field_name: "",
              }}
            >
              <BuilderRequestInjection
                path={buildPath("request_option")}
                descriptor={formatMessage({ id: "connectorBuilder.parameterizedRequests.requestOption.descriptor" })}
                excludeValues={["path"]}
              />
            </ToggleGroupField>
          </GroupControls>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
