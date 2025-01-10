import { useIntl } from "react-intl";

import { AssistButton } from "./Assist/AssistButton";
import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { KeyValueListField } from "./KeyValueListField";
import { BuilderRequestBody, concatPath, useBuilderWatch } from "../types";

type RequestOptionSectionProps =
  | {
      inline: false;
      basePath: `formValues.streams.${number}.requestOptions`;
      currentStreamIndex: number;
    }
  | {
      inline: true;
      basePath: "formValues.global.authenticator.login_requester.requestOptions";
    };

export const RequestOptionSection: React.FC<RequestOptionSectionProps> = (props) => {
  const { formatMessage } = useIntl();
  const bodyValue = useBuilderWatch(concatPath(props.basePath, "requestBody"));

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
      copyConfig={{
        path: "requestOptions",
        currentStreamIndex: props.currentStreamIndex,
        componentName: formatMessage({ id: "connectorBuilder.requestOptions.label" }),
      }}
      labelAction={<AssistButton assistKey="request_options" streamNum={props.currentStreamIndex} />}
      label="Request Options"
    >
      {content}
    </BuilderCard>
  );
};

RequestOptionSection.displayName = "RequestOptionSection";
