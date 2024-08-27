import { UseQueryResult } from "@tanstack/react-query";
import getValueByPath from "lodash/get";
import isEqual from "lodash/isEqual";
import merge from "lodash/merge";
import pick from "lodash/pick";
import { MouseEventHandler, useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { IconColor } from "components/ui/Icon/types";
import { Tooltip } from "components/ui/Tooltip";

import {
  BuilderAssistFindStreamsResponse,
  BuilderAssistManifestResponse,
  useBuilderAssistFindAuth,
  useBuilderAssistFindUrlBase,
  useBuilderAssistFindStreamPaginator,
  useBuilderAssistStreamMetadata,
  useBuilderAssistStreamResponse,
  HttpError,
} from "core/api";
import { KnownExceptionInfo } from "core/api/types/AirbyteClient";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { AssistKey, convertToAssistFormValuesSync } from "./assist";
import { AssistData, BuilderFormInput, BuilderFormValues, useBuilderWatch } from "../../types";

/**
 * HELPERS
 */

const mergeInputs = (currentInputs: BuilderFormInput[], previewInputs: BuilderFormInput[]): BuilderFormInput[] => {
  return Object.values({
    ...Object.fromEntries(previewInputs.map((input) => [input.key, input])),
    ...Object.fromEntries(currentInputs.map((input) => [input.key, input])),
  });
};

const isConfigEqual = (found?: object, currentValue?: object): boolean => {
  // return false if either is null
  if (!found || !currentValue) {
    return false;
  }

  // TODO: In the future, we likely only want to compare a specific subset of the config.
  // This is because the assist data may contain more information than what is currently in the form.
  // Or the form may contain more information than what is in the assist data.
  // For now, we'll just compare the entire object.
  return isEqual(found, currentValue);
};

const replaceStreamsSubpath = (subpath: string): string => {
  // Regular expression to match streams.<number>
  const pattern = /^streams\.\d+/g;
  // Replace matched patterns with streams.0
  return subpath.replace(pattern, "streams.0");
};

interface AssistState {
  tooltipContent?: string;
  iconColor: IconColor;
  disabled: boolean;
}

interface AssistStateParams {
  isFetching?: boolean;
  errorMessage?: string;
  hasRequiredData: boolean;
  suggestion?: object;
  currentValue?: object;
}

const getAssistButtonState = ({
  isFetching,
  errorMessage,
  suggestion,
  currentValue,
  hasRequiredData,
}: AssistStateParams): AssistState => {
  const agreesWithCurrentConfig = isConfigEqual(suggestion, currentValue);
  const hasNoSuggestions = !suggestion || Object.keys(suggestion).length === 0;

  if (isFetching) {
    return {
      tooltipContent: "connectorBuilder.assist.tooltip.fetching",
      iconColor: "disabled",
      disabled: true,
    };
  }

  if (!hasRequiredData) {
    return {
      tooltipContent: "connectorBuilder.assist.tooltip.needMoreInfo",
      iconColor: "disabled",
      disabled: true,
    };
  }

  if (errorMessage) {
    return {
      tooltipContent: "connectorBuilder.assist.tooltip.defaultError",
      iconColor: "error",
      disabled: true,
    };
  }

  if (hasNoSuggestions) {
    return {
      tooltipContent: "connectorBuilder.assist.tooltip.noSuggestions",
      iconColor: "disabled",
      disabled: true,
    };
  }

  if (agreesWithCurrentConfig) {
    return {
      tooltipContent: "connectorBuilder.assist.tooltip.agreeWithConfig",
      iconColor: "disabled",
      disabled: true,
    };
  }

  return { iconColor: "primary", disabled: false };
};

export interface AssistButtonProps {
  assistKey: AssistKey;
  streamNum?: number;
}

interface AssistButtonConfig {
  useHook: (params: object) => UseQueryResult<BuilderAssistManifestResponse, HttpError<KnownExceptionInfo>>;
  formPathToSet: string | ((streamNum: number) => string);
  propertiesToPluck?: string[];
}

const assistButtonConfigs = {
  urlbase: {
    useHook: useBuilderAssistFindUrlBase,
    formPathToSet: "global.urlBase",
  },
  auth: {
    useHook: useBuilderAssistFindAuth,
    formPathToSet: "global.authenticator",
  },
  metadata: {
    useHook: useBuilderAssistStreamMetadata,
    formPathToSet: (streamNum: number) => `streams.${streamNum}`,
    propertiesToPluck: ["urlPath", "httpMethod", "primaryKey"],
  },
  record_selector: {
    useHook: useBuilderAssistStreamResponse,
    formPathToSet: (streamNum: number) => `streams.${streamNum}.recordSelector`,
  },
  paginator: {
    useHook: useBuilderAssistFindStreamPaginator,
    formPathToSet: (streamNum: number) => `streams.${streamNum}.paginator`,
  },
} as const;

export const AssistButton: React.FC<AssistButtonProps> = ({ assistKey, streamNum }) => {
  const assistData: AssistData = useBuilderWatch("formValues.assist");
  const app_name = useWatch({ name: "name" }) || "Connector";
  const url_base = useWatch({ name: "formValues.global.urlBase" });
  const stream_name = useWatch({ name: `formValues.streams.${streamNum}.name` });

  const { assistEnabled } = useConnectorBuilderFormState();
  if (!assistData || !assistEnabled) {
    return null;
  }

  // TODO: this is a hack to get the docs_url and openapi_spec_url from the assistData
  // Before we bring this to our customers we should hoist this error higher.
  const docs_url = assistData?.docsUrl?.trim();
  const openapi_spec_url = assistData?.openApiSpecUrl?.trim();

  const config = assistButtonConfigs[assistKey] as AssistButtonConfig | undefined;
  if (!config) {
    console.error(`Unknown assist key: ${assistKey}`);
    return null;
  }

  const hookParams = {
    docs_url,
    openapi_spec_url,
    app_name,
    url_base,
    stream_name,
    streamNum,
  };

  return <InternalAssistButton hookParams={hookParams} streamNum={streamNum} {...config} />;
};

interface AssistAddStreamProps {
  assistData: AssistData;
  data?: BuilderAssistFindStreamsResponse;
  isError?: boolean;
  error?: { message: string } | null;
  isFetching?: boolean;
  streamOptions: Array<{ value: string }>;
  onMouseOver?: MouseEventHandler<HTMLElement>;
  onMouseOut?: MouseEventHandler<HTMLElement>;
  onClick?: MouseEventHandler<HTMLElement>;
}

export const AssistAddStreamButton: React.FC<AssistAddStreamProps> = ({
  assistData,
  isError,
  error,
  isFetching,
  streamOptions,
  onMouseOver,
  onMouseOut,
  onClick,
}) => {
  const { assistEnabled } = useConnectorBuilderFormState();
  const { formatMessage } = useIntl();

  const docs_url = assistData?.docsUrl?.trim();
  const openapi_spec_url = assistData?.openApiSpecUrl?.trim();
  const hasRequiredData = !!docs_url || !!openapi_spec_url;

  const errorMessage =
    isError || error ? error?.message || formatMessage({ id: "connectorBuilder.assist.tooltip.defaultError" }) : "";
  const assistButtonState = useMemo(
    () =>
      getAssistButtonState({
        isFetching,
        errorMessage,
        hasRequiredData,
        suggestion: streamOptions,
        currentValue: undefined,
      }),
    [errorMessage, hasRequiredData, isFetching, streamOptions]
  );

  if (!assistData || !assistEnabled) {
    return null;
  }

  const button = (
    <Button
      type="button"
      variant="highlight"
      onClick={onClick}
      icon="aiStars"
      disabled={assistButtonState.disabled}
      isLoading={isFetching}
      iconColor={assistButtonState.iconColor}
      onMouseOver={onMouseOver}
      onMouseOut={onMouseOut}
    />
  );

  // TODO: This should use InternalAssistButton
  if (assistButtonState.tooltipContent) {
    return <Tooltip control={button}>{formatMessage({ id: assistButtonState.tooltipContent })}</Tooltip>;
  }

  return button;
};

interface InternalAssistButtonProps extends AssistButtonConfig {
  streamNum?: number;
  hookParams: object;
}

const InternalAssistButton: React.FC<InternalAssistButtonProps> = ({
  useHook,
  hookParams,
  propertiesToPluck,
  streamNum,
  formPathToSet,
}) => {
  const { setValue } = useFormContext();
  const { formatMessage } = useIntl();

  const { data, isError, error, isFetching } = useHook(hookParams);
  const formPath = typeof formPathToSet === "function" ? formPathToSet(streamNum as number) : formPathToSet;

  const updatedFullForm: BuilderFormValues | null = useMemo(() => {
    if (data) {
      return convertToAssistFormValuesSync(data);
    }
    return null;
  }, [data]);

  const assistData: AssistData = useBuilderWatch("formValues.assist");
  const docs_url = assistData?.docsUrl?.trim();
  const openapi_spec_url = assistData?.openApiSpecUrl?.trim();
  const hasRequiredData = !!docs_url || !!openapi_spec_url;

  const formKey = `formValues.${formPath}`;
  const previewKey = `previewValues.${formPath}`;

  const currentInputs = useWatch({ name: "formValues.inputs", defaultValue: [] });
  const currentValue = useWatch({ name: formKey });

  const previewSubpath = replaceStreamsSubpath(formPath); // it's always on streams.0
  const pathAssistValue = (updatedFullForm ? getValueByPath(updatedFullForm, previewSubpath) : currentValue) || {};

  // TODO: update propertiesToPluck to be confirgurable to remove or keep undefined values on the existing form
  // WHY: The current implementation will keep the existing form values if the assist data is undefined
  // This may not always the desired behavior. For example, if they are transitioning from one auth type to another
  // and the new auth type doesn't have the same properties as the old one, we would want to remove the old properties
  const previewValue = propertiesToPluck ? pick(pathAssistValue, propertiesToPluck) : pathAssistValue;
  const valueToSet = propertiesToPluck ? merge({}, currentValue, previewValue) : previewValue;

  const showDataPreview = () => setValue(previewKey, previewValue);
  const hideDataPreview = () => setValue(previewKey, undefined);

  const buttonClickHandler = () => {
    hideDataPreview();
    setValue(formKey, valueToSet, {
      shouldValidate: true,
      shouldDirty: true,
      shouldTouch: true,
    });
    if (updatedFullForm?.inputs) {
      setValue("formValues.inputs", mergeInputs(currentInputs, updatedFullForm.inputs));
    }
  };

  const errorMessage =
    isError || error ? error?.message || formatMessage({ id: "connectorBuilder.assist.tooltip.defaultError" }) : "";
  const assistButtonState = useMemo(
    () =>
      getAssistButtonState({
        isFetching,
        errorMessage,
        hasRequiredData,
        suggestion: valueToSet,
        currentValue,
      }),
    [valueToSet, currentValue, errorMessage, hasRequiredData, isFetching]
  );

  const button = (
    <Button
      type="button"
      variant="highlight"
      onClick={buttonClickHandler}
      icon="aiStars"
      disabled={assistButtonState.disabled}
      isLoading={isFetching}
      iconColor={assistButtonState.iconColor}
      onMouseOver={assistButtonState.disabled ? undefined : showDataPreview}
      onMouseOut={assistButtonState.disabled ? undefined : hideDataPreview}
    />
  );

  if (assistButtonState.tooltipContent) {
    return <Tooltip control={button}>{formatMessage({ id: assistButtonState.tooltipContent })}</Tooltip>;
  }
  return button;
};
