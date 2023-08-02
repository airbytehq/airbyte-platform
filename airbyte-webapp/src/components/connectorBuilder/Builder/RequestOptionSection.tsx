import { useIntl } from "react-intl";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { KeyValueListField } from "./KeyValueListField";
import { BuilderRequestBody, concatPath, useBuilderWatch } from "../types";

type RequestOptionSectionProps = { omitInterpolationContext?: boolean } & (
  | {
      inline: false;
      basePath: `streams.${number}.requestOptions`;
      currentStreamIndex: number;
    }
  | {
      inline: true;
      basePath: "global.authenticator.login_requester.requestOptions";
    }
);

export const RequestOptionSection: React.FC<RequestOptionSectionProps> = (props) => {
  const { formatMessage } = useIntl();

  const bodyValue = useBuilderWatch(concatPath(props.basePath, "requestBody"));

  const getBodyOptions = (): Array<OneOfOption<BuilderRequestBody>> => [
    {
      label: "JSON (key-value pairs)",
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
          omitInterpolationContext={props.omitInterpolationContext}
        />
      ),
    },
    {
      label: "Form encoded (key-value pairs)",
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
          omitInterpolationContext={props.omitInterpolationContext}
        />
      ),
    },
    {
      label: "JSON (free form)",
      default: {
        type: "json_freeform",
        value: bodyValue.type === "json_list" ? JSON.stringify(Object.fromEntries(bodyValue.values)) : "{}",
      },
      children: (
        <BuilderField
          type="jsoneditor"
          path={concatPath(props.basePath, "requestBody.value")}
          manifestPath="HttpRequester.properties.request_body_json"
          omitInterpolationContext={props.omitInterpolationContext}
        />
      ),
    },
    {
      label: "Text (Free form)",
      default: {
        type: "string_freeform",
        value: "",
      },
      children: (
        <BuilderField
          type="textarea"
          path={concatPath(props.basePath, "requestBody.value")}
          label="Request body as string"
          manifestPath="HttpRequester.properties.request_body_data"
          omitInterpolationContext={props.omitInterpolationContext}
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
        omitInterpolationContext={props.omitInterpolationContext}
      />
      <KeyValueListField
        path={concatPath(props.basePath, "requestHeaders")}
        manifestPath="HttpRequester.properties.request_headers"
        optional
        omitInterpolationContext={props.omitInterpolationContext}
      />
      <BuilderOneOf<BuilderRequestBody>
        path={concatPath(props.basePath, "requestBody")}
        label="Request Body"
        options={getBodyOptions()}
        omitInterpolationContext={props.omitInterpolationContext}
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
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromRequestOptionsTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToRequestOptionsTitle" }),
      }}
    >
      {content}
    </BuilderCard>
  );
};

RequestOptionSection.displayName = "RequestOptionSection";
