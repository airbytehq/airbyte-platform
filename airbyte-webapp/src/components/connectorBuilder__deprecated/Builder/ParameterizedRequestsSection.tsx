import { useIntl } from "react-intl";
import ReactMarkdown from "react-markdown";

import GroupControls from "components/GroupControls";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { ToggleGroupField } from "./ToggleGroupField";
import { manifestListPartitionRouterToBuilder } from "../convertManifestToBuilderForm";
import {
  LIST_PARTITION_ROUTER,
  BuilderParameterizedRequests,
  builderParameterizedRequestsToManifest,
  CreationRequesterPathFn,
  AnyDeclarativeStreamPathFn,
  StreamId,
} from "../types";
import { StreamFieldPath } from "../utils";

interface ParameterizedRequestsSectionProps {
  streamFieldPath: AnyDeclarativeStreamPathFn | CreationRequesterPathFn;
  streamId: StreamId;
}

const EMPTY_PARAMETERIZED_REQUEST: BuilderParameterizedRequests = {
  type: LIST_PARTITION_ROUTER,
  values: { type: "list", value: [] },
  cursor_field: "",
};

export const ParameterizedRequestsSection: React.FC<ParameterizedRequestsSectionProps> = ({
  streamFieldPath,
  streamId,
}) => {
  const { formatMessage } = useIntl();
  const label = formatMessage({ id: "connectorBuilder.parameterizedRequests.label" });

  return (
    <BuilderCard
      docLink={links.connectorBuilderParameterizedRequests}
      label={label}
      tooltip={formatMessage({ id: "connectorBuilder.parameterizedRequests.tooltip" })}
      inputsConfig={{
        toggleable: true,
        path: streamFieldPath("parameterizedRequests"),
        defaultValue: [EMPTY_PARAMETERIZED_REQUEST],
        yamlConfig: {
          builderToManifest: builderParameterizedRequestsToManifest,
          manifestToBuilder: manifestListPartitionRouterToBuilder,
        },
      }}
      copyConfig={
        streamId.type === "stream"
          ? {
              path: streamFieldPath("parameterizedRequests") as StreamFieldPath,
              currentStreamIndex: streamId.index,
              componentName: label,
            }
          : undefined
      }
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
                    <BuilderField
                      type="jinja"
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
            <BuilderField
              type="jinja"
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
