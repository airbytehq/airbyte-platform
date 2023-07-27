import { useIntl } from "react-intl";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { KeyValueListField } from "./KeyValueListField";
import { concatPath, useBuilderWatch } from "../types";

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

  const getBodyOptions = (): OneOfOption[] => [
    {
      label: "JSON (key-value pairs)",
      typeValue: "json_list",
      default: {
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
      typeValue: "form_list",
      default: {
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
      typeValue: "json_freeform",
      default: {
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
      typeValue: "string_freeform",
      default: {
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
      <BuilderOneOf
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
