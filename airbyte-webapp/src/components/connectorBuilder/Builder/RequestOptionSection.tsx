import { useIntl } from "react-intl";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { KeyValueListField } from "./KeyValueListField";
import { useBuilderWatch } from "../types";

interface RequestOptionSectionProps {
  streamFieldPath: <T extends string>(fieldPath: T) => `streams.${number}.${T}`;
  currentStreamIndex: number;
}
export const RequestOptionSection: React.FC<RequestOptionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();

  const bodyValue = useBuilderWatch(streamFieldPath("requestOptions.requestBody"));

  const getBodyOptions = (): OneOfOption[] => [
    {
      label: "JSON (key-value pairs)",
      typeValue: "json_list",
      default: {
        values: [],
      },
      children: (
        <KeyValueListField
          path={streamFieldPath("requestOptions.requestBody.values")}
          key="json_list"
          manifestPath="HttpRequester.properties.request_body_json"
          optional
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
          path={streamFieldPath("requestOptions.requestBody.values")}
          key="form_list"
          manifestPath="HttpRequester.properties.request_body_data"
          optional
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
          path={streamFieldPath("requestOptions.requestBody.value")}
          manifestPath="HttpRequester.properties.request_body_json"
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
          path={streamFieldPath("requestOptions.requestBody.value")}
          label="Request body as string"
          manifestPath="HttpRequester.properties.request_body_data"
        />
      ),
    },
  ];

  return (
    <BuilderCard
      copyConfig={{
        path: "requestOptions",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromRequestOptionsTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToRequestOptionsTitle" }),
      }}
    >
      <KeyValueListField
        path={streamFieldPath("requestOptions.requestParameters")}
        manifestPath="HttpRequester.properties.request_parameters"
        optional
      />
      <KeyValueListField
        path={streamFieldPath("requestOptions.requestHeaders")}
        manifestPath="HttpRequester.properties.request_headers"
        optional
      />
      <BuilderOneOf
        path={streamFieldPath("requestOptions.requestBody")}
        label="Request Body"
        options={getBodyOptions()}
      />
    </BuilderCard>
  );
};

RequestOptionSection.displayName = "RequestOptionSection";
