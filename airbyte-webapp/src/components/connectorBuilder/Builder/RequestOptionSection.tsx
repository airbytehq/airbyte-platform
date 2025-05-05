import { useIntl } from "react-intl";

import { AssistButton } from "./Assist/AssistButton";
import { AuthPath } from "./AuthenticationSection";
import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { KeyValueListField } from "./KeyValueListField";
import { BuilderRequestBody, concatPath, StreamId } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";
import { StreamFieldPath } from "../utils";

type RequestOptionSectionProps =
  | {
      inline: false;
      basePath:
        | `formValues.streams.${number}.requestOptions`
        | `formValues.streams.${number}.creationRequester.requestOptions`
        | `formValues.streams.${number}.pollingRequester.requestOptions`
        | `formValues.streams.${number}.downloadRequester.requestOptions`
        | `formValues.dynamicStreams.${number}.streamTemplate.requestOptions`
        | `formValues.generatedStreams.${string}.${number}.requestOptions`;
      streamId: StreamId;
    }
  | {
      inline: true;
      basePath: `${AuthPath}.login_requester.requestOptions`;
    };

export const RequestOptionSection: React.FC<RequestOptionSectionProps> = (props) => {
  const { formatMessage } = useIntl();

  const bodyValue = useBuilderWatch(concatPath(props.basePath, "requestBody")) as BuilderRequestBody;

  const getBodyOptions = (): Array<OneOfOption<BuilderRequestBody>> => [
    {
      label: formatMessage({ id: "connectorBuilder.requestOptions.jsonList" }),
      default: {
        type: "json_list",
        values: [],
      },
      children: (
        <KeyValueListField
          path={concatPath(props.basePath, "requestBody.values")}
          key="json_list"
          manifestPath="HttpRequester.properties.request_body_json"
          optional
        />
      ),
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOptions.formList" }),
      default: {
        type: "form_list",
        values: [],
      },
      children: (
        <KeyValueListField
          path={concatPath(props.basePath, "requestBody.values")}
          key="form_list"
          manifestPath="HttpRequester.properties.request_body_data"
          optional
        />
      ),
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOptions.jsonFreeform" }),
      default: {
        type: "json_freeform",
        value: bodyValue.type === "json_list" ? JSON.stringify(Object.fromEntries(bodyValue.values)) : "{}",
      },
      children: (
        <BuilderField
          type="jsoneditor"
          path={concatPath(props.basePath, "requestBody.value")}
          manifestPath="HttpRequester.properties.request_body_json"
        />
      ),
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOptions.stringFreeform" }),
      default: {
        type: "string_freeform",
        value: "",
      },
      children: (
        <BuilderField
          type="textarea"
          path={concatPath(props.basePath, "requestBody.value")}
          label={formatMessage({ id: "connectorBuilder.requestOptions.stringFreeform.value" })}
          manifestPath="HttpRequester.properties.request_body_data"
        />
      ),
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOptions.graphqlQuery" }),
      default: {
        type: "graphql",
        value: "query {\n  resource {\n    field \n  }\n}",
      },
      children: (
        <BuilderField
          type="graphql"
          path={concatPath(props.basePath, "requestBody.value")}
          label={formatMessage({ id: "connectorBuilder.requestOptions.graphqlQuery.value" })}
          tooltip={formatMessage({ id: "connectorBuilder.requestOptions.graphqlQuery.tooltip" })}
        />
      ),
    },
  ];

  const content = (
    <>
      <KeyValueListField
        path={concatPath(props.basePath, "requestParameters")}
        manifestPath="HttpRequester.properties.request_parameters"
        optional
      />
      <KeyValueListField
        path={concatPath(props.basePath, "requestHeaders")}
        manifestPath="HttpRequester.properties.request_headers"
        optional
      />
      <BuilderOneOf<BuilderRequestBody>
        path={concatPath(props.basePath, "requestBody")}
        label={formatMessage({ id: "connectorBuilder.requestOptions.requestBody" })}
        options={getBodyOptions()}
      />
    </>
  );

  return props.inline ? (
    content
  ) : (
    <BuilderCard
      copyConfig={
        props.streamId.type === "stream"
          ? {
              path: props.basePath as StreamFieldPath,
              currentStreamIndex: props.streamId.index,
              componentName: formatMessage({ id: "connectorBuilder.requestOptions.label" }),
            }
          : undefined
      }
      labelAction={<AssistButton assistKey="request_options" streamId={props.streamId} />}
      label={formatMessage({ id: "connectorBuilder.requestOptions.label" })}
      tooltip={formatMessage({ id: "connectorBuilder.requestOptions.tooltip" })}
    >
      {content}
    </BuilderCard>
  );
};

RequestOptionSection.displayName = "RequestOptionSection";
