import { parse } from "graphql";
import { useMemo, useState } from "react";
import { get, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";
import { formatJson, getStreamName } from "components/connectorBuilder/utils";
import {
  FormControlFooter,
  FormLabel,
  FormControlErrorMessage,
  FormControlFooterError,
  FormControlFooterInfo,
  FormControl,
} from "components/forms/FormControl";
import { ControlGroup } from "components/forms/SchemaForm/Controls/ControlGroup";
import { LabelInfo } from "components/Label";
import { CodeEditor } from "components/ui/CodeEditor";
import { GraphQLEditor } from "components/ui/CodeEditor/GraphqlEditor";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { Option } from "components/ui/ListBox/Option";
import { TagInput } from "components/ui/TagInput";

import { RequestOptionInjectInto } from "core/api/types/ConnectorManifest";
import { useConnectorBuilderPermission } from "services/connectorBuilder/ConnectorBuilderStateService";

import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import styles from "./overrides.module.scss";

export const ParentStreamSelector = ({ path, currentStreamName }: { path: string; currentStreamName?: string }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();

  const streams = useBuilderWatch("manifest.streams");
  const streamNameToIndex = useMemo(() => {
    return (
      streams?.reduce(
        (acc, stream, index) => {
          acc[getStreamName(stream, index)] = index;
          return acc;
        },
        {} as Record<string, number>
      ) ?? {}
    );
  }, [streams]);

  const value = useBuilderWatch(path) as { $ref?: string } | undefined;

  // The normal default value for this field is an empty stream definition, which is invalid for the UI,
  // so do a one-time check for this and set it to undefined if so, as that is the "empty" value that the
  // UI handles well.
  useEffectOnce(() => {
    if (value && !("$ref" in value)) {
      setValue(path, undefined);
    }
  });

  const selectedValue = useMemo(() => {
    if (value && value.$ref) {
      const match = value.$ref.match(/#\/streams\/(\d+)/);
      if (match) {
        const streamIndex = Number(match[1]);
        return getStreamName(streams?.[streamIndex], streamIndex);
      }
    }
    return undefined;
  }, [value, streams]);

  const options = useMemo(() => {
    return (
      streams
        ?.map((stream, index) => ({ label: getStreamName(stream, index), value: getStreamName(stream, index) }))
        ?.filter(({ value }) => value !== currentStreamName) ?? []
    );
  }, [streams, currentStreamName]);

  const { errors, touchedFields } = useFormState({ name: path });
  const hasError = !!get(errors, path) && !!get(touchedFields, path);

  return (
    <FlexContainer direction="column" gap="none" className={styles.controlContainer} data-field-path={path}>
      <FormLabel
        htmlFor={path}
        label={formatMessage({ id: "connectorBuilder.parentStream.label" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.parentStream.tooltip" })}
      />
      <ListBox
        id={path}
        options={options}
        selectedValue={selectedValue}
        onSelect={(value) => {
          setValue(path, {
            $ref: `#/streams/${streamNameToIndex[value]}`,
          });
        }}
        hasError={hasError}
      />
      <FormControlFooter>{hasError && <FormControlErrorMessage name={path} />}</FormControlFooter>
    </FlexContainer>
  );
};

export const RequestBodyGraphQL = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const value = useBuilderWatch(path) as Record<string, unknown> | undefined;
  const { query, ...additionalProperties } = value ?? {};
  const queryPath = `${path}.query`;
  const queryValue = query as string | undefined;
  const permission = useConnectorBuilderPermission();
  const isDisabled = useMemo(
    () => path.startsWith("generatedStreams") || permission === "readOnly",
    [path, permission]
  );

  useEffectOnce(() => {
    if (!queryValue) {
      setValue(queryPath, "query {\n\n}");
    }
  });

  const [isQueryValid, setIsQueryValid] = useState(true);
  const [isAdditionalPropertiesValid, setIsAdditionalPropertiesValid] = useState(
    validateJSON(formatJson(additionalProperties))
  );

  return (
    <FlexContainer direction="column" gap="none">
      <ControlGroup
        data-field-path={path}
        path={queryPath}
        title={formatMessage({ id: "connectorBuilder.graphQL.query.title" })}
        tooltip={
          <LabelInfo
            description={formatMessage({ id: "connectorBuilder.graphQL.query.tooltip" })}
            examples={["query { countries { code } }"]}
          />
        }
      >
        <div className={styles.editorContainer}>
          <div className={styles.editor} data-field-path={queryPath}>
            <GraphQLEditor
              value={queryValue ? String(queryValue) : ""}
              onChange={(newValue) => {
                setValue(queryPath, newValue);
                setIsQueryValid(validateGraphQL(newValue));
              }}
              disabled={isDisabled}
            />
          </div>
          <FormControlFooter>
            {isQueryValid ? (
              <FormControlFooterInfo>
                <FormattedMessage id="connectorBuilder.graphQL.query.enterValid" />
              </FormControlFooterInfo>
            ) : (
              <FormControlFooterError>
                <FormattedMessage id="connectorBuilder.graphQL.query.invalidSyntax" />
              </FormControlFooterError>
            )}
          </FormControlFooter>
        </div>
      </ControlGroup>
      <ControlGroup
        path={path}
        title={formatMessage({ id: "connectorBuilder.graphQL.additionalProperties.title" })}
        tooltip={
          <LabelInfo
            description={formatMessage({ id: "connectorBuilder.graphQL.additionalProperties.tooltip" })}
            examples='{"variables": { "continent": "Europe" }}'
          />
        }
        toggleConfig={{
          isEnabled: Object.keys(additionalProperties).length > 0,
          onToggle: (newEnabledState: boolean) => {
            if (newEnabledState) {
              setValue(path, {
                query: queryValue,
                variables: {},
              });
            } else {
              setValue(path, {
                query: queryValue,
              });
            }
          },
        }}
      >
        <div className={styles.editorContainer}>
          <div className={styles.editor}>
            <GraphQLAdditionalPropertiesEditor
              value={additionalProperties}
              onChange={(val) => {
                try {
                  const parsedValue = JSON.parse(val || "{}");
                  setValue(path, {
                    ...parsedValue,
                    query: queryValue,
                  });
                  setIsAdditionalPropertiesValid(true);
                } catch (error) {
                  setIsAdditionalPropertiesValid(false);
                }
              }}
              isDisabled={isDisabled}
            />
          </div>
          <FormControlFooter>
            {isAdditionalPropertiesValid ? (
              <FormControlFooterInfo>
                <FormattedMessage id="form.enterValidJson" />
              </FormControlFooterInfo>
            ) : (
              <FormControlFooterError>
                <FormattedMessage id="connectorBuilder.invalidJSON" />
              </FormControlFooterError>
            )}
          </FormControlFooter>
        </div>
      </ControlGroup>
    </FlexContainer>
  );
};

const GraphQLAdditionalPropertiesEditor = ({
  value,
  onChange,
  isDisabled,
}: {
  value: unknown;
  onChange: (value: string) => void;
  isDisabled: boolean;
}) => {
  const [textValue, setTextValue] = useState(formatJson(value));
  return (
    <CodeEditor
      value={textValue}
      language="json"
      readOnly={isDisabled}
      onChange={(val: string | undefined) => {
        setTextValue(val || "");
        onChange(val || "");
      }}
    />
  );
};

const validateGraphQL = (value: string | undefined) => {
  if (!value) {
    return true;
  }
  try {
    parse(value);
    return true;
  } catch (error) {
    return false;
  }
};

const validateJSON = (value: string | undefined) => {
  if (!value) {
    return true;
  }
  try {
    JSON.parse(value);
    return true;
  } catch (error) {
    return false;
  }
};

export type RequestOptionInjectIntoValues = (typeof RequestOptionInjectInto)[keyof typeof RequestOptionInjectInto];

export const RequestOptionFieldName = () => {
  return null;
};

export const RequestOptionFieldPath = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();

  const fieldNamePath = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.field_name`, [path]);
  const injectIntoPath = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.inject_into`, [path]);

  const fieldPathValue = useBuilderWatch(path) as string[] | undefined;
  const fieldNameValue = useBuilderWatch(fieldNamePath) as string | undefined;
  const injectIntoValue = useBuilderWatch(injectIntoPath) as RequestOptionInjectIntoValues | undefined;

  const fieldLabel = useMemo(() => {
    switch (injectIntoValue) {
      case "request_parameter":
        return formatMessage({ id: "connectorBuilder.requestOption.parameter.label" });
      case "header":
        return formatMessage({ id: "connectorBuilder.requestOption.header.label" });
      case "body_data":
        return formatMessage({ id: "connectorBuilder.requestOption.body_data.label" });
      default:
        return formatMessage({ id: "connectorBuilder.requestOption.fieldPath.label" });
    }
  }, [formatMessage, injectIntoValue]);

  return (
    <FlexContainer direction="column" gap="none" className={styles.controlContainer}>
      <FormLabel
        htmlFor={path}
        label={fieldLabel}
        labelTooltip={formatMessage({ id: "connectorBuilder.requestOption.field.tooltip" })}
      />
      {injectIntoValue === "request_parameter" || injectIntoValue === "header" || injectIntoValue === "body_data" ? (
        <Input
          value={fieldNameValue}
          onChange={(e) => {
            setValue(fieldNamePath, e.target.value);
            setValue(path, undefined);
          }}
        />
      ) : (
        <TagInput
          name={path}
          fieldValue={fieldPathValue ?? []}
          onChange={(newValue: string[]) => {
            setValue(path, newValue);
            setValue(fieldNamePath, undefined);
          }}
        />
      )}
    </FlexContainer>
  );
};

export const RequestOptionInjectSelector = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  const injectIntoOptions: Array<Option<string>> = [
    {
      label: formatMessage({ id: "connectorBuilder.requestOption.injectInto.requestParameter" }),
      value: "request_parameter",
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOption.injectInto.header" }),
      value: "header",
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOption.injectInto.bodyData" }),
      value: "body_data",
    },
    {
      label: formatMessage({ id: "connectorBuilder.requestOption.injectInto.bodyJson" }),
      value: "body_json",
    },
  ];

  return (
    <FormControl
      fieldType="dropdown"
      name={path}
      options={injectIntoOptions}
      label={getLabelByManifest("RequestOption.properties.inject_into")}
      labelTooltip={getDescriptionByManifest("RequestOption.properties.inject_into")}
      onlyShowErrorIfTouched
    />
  );
};
