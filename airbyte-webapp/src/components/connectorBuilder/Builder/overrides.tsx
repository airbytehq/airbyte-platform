import classNames from "classnames";
import { parse } from "graphql";
import { useCallback, useMemo, useState } from "react";
import { get, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce, useUpdateEffect } from "react-use";

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
import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { BaseControlProps } from "components/forms/SchemaForm/Controls/types";
import { LabelInfo } from "components/Label";
import { CodeEditor } from "components/ui/CodeEditor";
import { GraphQLEditor } from "components/ui/CodeEditor/GraphqlEditor";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { Option } from "components/ui/ListBox/Option";
import { Message } from "components/ui/Message";

import { RequestOptionInjectInto } from "core/api/types/ConnectorManifest";
import {
  useConnectorBuilderPermission,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderDeclarativeOAuth } from "./BuilderDeclarativeOAuth";
import { JinjaInput } from "./JinjaInput";
import { getDescriptionByManifest, getLabelAndTooltip, getLabelByManifest } from "./manifestHelpers";
import styles from "./overrides.module.scss";
import { BuilderStreamTab, StreamId } from "../types";
import { useStreamNames } from "../useStreamNames";

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

export const RequestOptionFieldName = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  const injectIntoPath = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.inject_into`, [path]);
  const injectIntoValue = useBuilderWatch(injectIntoPath) as RequestOptionInjectIntoValues | undefined;

  if (injectIntoValue === "body_json") {
    return null;
  }

  return (
    <SchemaFormControl
      titleOverride={
        injectIntoValue === "request_parameter"
          ? formatMessage({ id: "connectorBuilder.requestOption.parameter.label" })
          : injectIntoValue === "header"
          ? formatMessage({ id: "connectorBuilder.requestOption.header.label" })
          : injectIntoValue === "body_data"
          ? formatMessage({ id: "connectorBuilder.requestOption.body_data.label" })
          : formatMessage({ id: "connectorBuilder.requestOption.fieldName.label" })
      }
      isRequired
      path={path}
    />
  );
};

export const RequestOptionFieldPath = ({ path }: { path: string }) => {
  const injectIntoPath = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.inject_into`, [path]);
  const injectIntoValue = useBuilderWatch(injectIntoPath) as RequestOptionInjectIntoValues | undefined;

  if (injectIntoValue !== "body_json") {
    return null;
  }

  return <SchemaFormControl path={path} isRequired />;
};

export const RequestOptionInjectSelector = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();

  const fieldNamePath = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.field_name`, [path]);
  const fieldPathPath = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.field_path`, [path]);

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

  const { label, tooltip } = getLabelAndTooltip("RequestOption.properties.inject_into");

  return (
    <FormControl
      fieldType="dropdown"
      name={path}
      options={injectIntoOptions}
      label={label}
      labelTooltip={tooltip}
      onlyShowErrorIfTouched
      onSelect={(value) => {
        // clear the unused field's value when the inject_into value changes
        if (value === "body_json") {
          setValue(fieldNamePath, undefined, { shouldDirty: true, shouldTouch: true, shouldValidate: true });
        } else {
          setValue(fieldPathPath, undefined, { shouldDirty: true, shouldTouch: true, shouldValidate: true });
        }
      }}
    />
  );
};

export const DeclarativeOAuthWithClientId = ({ clientIdPath }: { clientIdPath: string }) => {
  const authFieldPath = useCallback(
    (field: string) => `${clientIdPath.split(".").slice(0, -1).join(".")}.${field}`,
    [clientIdPath]
  );

  const activeStreamTab = useBuilderWatch("streamTab");

  const shouldRenderDeclarativeOAuth = useMemo(() => {
    // Always render declarative oauth for components_resolver, since that does not depend on the stream tab
    if (clientIdPath.includes("components_resolver")) {
      return true;
    }

    // Only render the BuilderDeclarativeOAuth if this path is in the currently active stream tab
    // to avoid rendering BuilderDeclarativeOAuth multiple times simultaneously, as this confuses
    // react-hook-form.
    const requesterMatch = clientIdPath.match(/^manifest\..*\.retriever\.([^.]+)\.authenticator\..*$/)?.[1];
    const parentStreamTab: BuilderStreamTab | undefined = requesterMatch
      ? requesterMatch === "requester" || requesterMatch === "creation_requester"
        ? "requester"
        : requesterMatch === "polling_requester"
        ? "polling"
        : requesterMatch === "download_requester"
        ? "download"
        : undefined
      : undefined;
    return parentStreamTab === activeStreamTab;
  }, [clientIdPath, activeStreamTab]);

  return (
    <FlexContainer direction="column" gap="none">
      {shouldRenderDeclarativeOAuth && <BuilderDeclarativeOAuth authFieldPath={authFieldPath} />}
      <SchemaFormControl path={clientIdPath} />
    </FlexContainer>
  );
};

const REFRESH_TOKEN_GRANT_TYPE = "refresh_token";
const CLIENT_CREDENTIALS_GRANT_TYPE = "client_credentials";
export const GrantTypeSelector = ({ path }: { path: string }) => {
  const grantTypeOptions: Array<Option<string>> = [
    {
      label: REFRESH_TOKEN_GRANT_TYPE,
      value: REFRESH_TOKEN_GRANT_TYPE,
    },
    {
      label: CLIENT_CREDENTIALS_GRANT_TYPE,
      value: CLIENT_CREDENTIALS_GRANT_TYPE,
    },
  ];

  return (
    <FormControl
      fieldType="dropdown"
      name={path}
      options={grantTypeOptions}
      label={getLabelByManifest("OAuthAuthenticator.properties.grant_type")}
      labelTooltip={getDescriptionByManifest("OAuthAuthenticator.properties.grant_type")}
    />
  );
};

export const JinjaBuilderField = ({
  name,
  label,
  labelTooltip,
  optional,
  header,
  containerControlClassName,
  onlyShowErrorIfTouched,
  "data-field-path": dataFieldPath,
  disabled,
  interpolationContext,
}: BaseControlProps) => {
  const value = useBuilderWatch(name) as string | undefined;
  const { setValue } = useFormContext();

  const { errors, touchedFields } = useFormState({ name });
  const error = !!get(errors, name) && (onlyShowErrorIfTouched ? !!get(touchedFields, name) : true);

  return (
    <FlexContainer
      direction="column"
      gap="none"
      className={classNames(styles.controlContainer, containerControlClassName)}
      data-field-path={dataFieldPath}
    >
      {label && (
        <FormLabel htmlFor={name} label={label} labelTooltip={labelTooltip} optional={optional} header={header} />
      )}
      <JinjaInput
        name={name}
        value={value || ""}
        onChange={(newValue) =>
          setValue(name, newValue, { shouldValidate: true, shouldTouch: true, shouldDirty: true })
        }
        disabled={disabled}
        interpolationContext={interpolationContext}
        error={!!error}
      />
      {!!error && (
        <FormControlFooter>
          <FormControlErrorMessage name={name} />
        </FormControlFooter>
      )}
    </FlexContainer>
  );
};

export const StreamNameField = ({ path, streamType }: { path: string; streamType: StreamId["type"] }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const { streamNames, dynamicStreamNames } = useStreamNames();
  const value = useBuilderWatch(path) as string | undefined;

  // Maintain a local value so that user can see their changes even if the name is
  // a duplicate and is therefore not being saved to the form.
  const [localValue, setLocalValue] = useState(value);

  const [isDuplicate, setIsDuplicate] = useState(false);

  const error = useMemo(() => {
    if (!value) {
      return <FormattedMessage id="form.empty.error" />;
    }
    if (isDuplicate) {
      return <FormattedMessage id="connectorBuilder.duplicateStreamName" />;
    }
    return undefined;
  }, [value, isDuplicate]);

  // If the form value changes for any reason, update the local value to match it.
  useUpdateEffect(() => {
    if (localValue !== value) {
      setLocalValue(value);
    }
  }, [value]);

  return (
    <div className={styles.streamNameContainer}>
      <FlexContainer direction="column" gap="none" className={styles.streamNameContainerInner}>
        <Input
          className={styles.streamNameInput}
          data-field-path={path}
          value={localValue}
          error={!!error}
          placeholder={
            streamType === "dynamic_stream"
              ? formatMessage({ id: "connectorBuilder.streamTemplateName.placeholder" })
              : formatMessage({ id: "connectorBuilder.streamName.placeholder" })
          }
          onChange={(e) => {
            const newValue = e.target.value;
            setLocalValue(newValue);

            let numMatchingNames =
              streamNames.filter((name) => name === newValue).length +
              dynamicStreamNames.filter((name) => name === newValue).length;

            // If the current form value for this field is the same as the new value, don't count it towards the duplicate check
            if (newValue === value) {
              numMatchingNames -= 1;
            }

            // If there are matching names, set the duplicate state to true and don't commit the new value to the form
            // to avoid unwanted effects.
            if (numMatchingNames > 0) {
              setIsDuplicate(true);
              return;
            }

            setIsDuplicate(false);
            setValue(path, newValue);
          }}
        />
        {error && (
          <FormControlFooter>
            <FormControlFooterError>{error}</FormControlFooterError>
          </FormControlFooter>
        )}
      </FlexContainer>
    </div>
  );
};

export const BackoffStrategies = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer direction="column" gap="xl">
      <Message text={formatMessage({ id: "connectorBuilder.errorHandlerWarning" })} type="warning" />
      <SchemaFormControl path={path} />
    </FlexContainer>
  );
};

export const CursorField = ({ path }: { path: string }) => {
  const { formatMessage } = useIntl();
  const {
    streamRead: { data },
  } = useConnectorBuilderTestRead();

  const datetimeFields = Object.keys(data?.inferred_datetime_formats || {});
  if (datetimeFields.length > 0) {
    const { label, tooltip } = getLabelAndTooltip("DatetimeBasedCursor.properties.cursor_field");
    return (
      <FormControl
        name={path}
        label={label}
        labelTooltip={tooltip}
        fieldType="combobox"
        allowCustomValue
        options={[
          {
            sectionTitle: formatMessage({ id: "form.suggestions" }),
            innerOptions: datetimeFields.map((datetimeField) => ({
              label: datetimeField,
              value: datetimeField,
            })),
          },
        ]}
      />
    );
  }
  return <SchemaFormControl path={path} />;
};

export const CursorDatetimeFormats = ({ path }: { path: string }) => {
  const {
    streamRead: { data },
  } = useConnectorBuilderTestRead();
  const cursorFieldPath: string = useMemo(() => `${path.split(".").slice(0, -1).join(".")}.cursor_field`, [path]);
  const cursorField = useBuilderWatch(cursorFieldPath) as string | undefined;
  const rawDetectedFormat = cursorField ? data?.inferred_datetime_formats?.[cursorField] : undefined;
  const detectedFormat = typeof rawDetectedFormat === "string" ? rawDetectedFormat : null;

  return <SchemaFormControl path={path} suggestionsOverride={detectedFormat ? [detectedFormat] : undefined} />;
};
