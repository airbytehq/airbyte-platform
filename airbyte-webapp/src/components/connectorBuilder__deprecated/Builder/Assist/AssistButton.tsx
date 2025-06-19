import { UseQueryResult } from "@tanstack/react-query";
import getValueByPath from "lodash/get";
import isEqual from "lodash/isEqual";
import merge from "lodash/merge";
import pick from "lodash/pick";
import { MouseEventHandler, useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useIntl } from "react-intl";

import { useBuilderWatch } from "components/connectorBuilder__deprecated/useBuilderWatch";
import { Button } from "components/ui/Button";
import { IconColor } from "components/ui/Icon/types";
import { Tooltip } from "components/ui/Tooltip";

import { HttpError } from "core/api";
import { KnownExceptionInfo } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { removeEmptyProperties } from "core/utils/form";
import { useConnectorBuilderFormState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import {
  AssistKey,
  convertToAssistFormValuesSync,
  BuilderAssistFindStreamsResponse,
  BuilderAssistInputAllParams,
  BuilderAssistManifestResponse,
  useBuilderAssistFindAuth,
  useBuilderAssistFindRequestOptions,
  useBuilderAssistFindUrlBase,
  useBuilderAssistFindStreamPaginator,
  useBuilderAssistStreamMetadata,
  useBuilderAssistStreamResponse,
  parseAssistErrorToFormErrors,
  computeStreamResponse,
  useBuilderAssistFindIncrementalSync,
} from "./assist";
import { AssistData, BuilderFormInput, BuilderFormValues, StreamId } from "../../types";

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
  return isEqual(removeEmptyProperties(found), removeEmptyProperties(currentValue));
};

const replaceStreamsSubpath = (subpath: string): string => {
  // Regular expression to match streams.<number>
  const pattern = /^streams\.\d+/g;
  // Replace matched patterns with streams.0
  return subpath.replace(pattern, "streams.0");
};

type AssistStateKey = "fetching" | "needMoreInfo" | "defaultError" | "noSuggestions" | "agreeWithConfig" | "success";

interface AssistState {
  stateKey: AssistStateKey;
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
      stateKey: "fetching",
      tooltipContent: "connectorBuilder.assist.tooltip.fetching",
      iconColor: "disabled",
      disabled: true,
    };
  }

  if (!hasRequiredData) {
    return {
      stateKey: "needMoreInfo",
      tooltipContent: "connectorBuilder.assist.tooltip.needMoreInfo",
      iconColor: "disabled",
      disabled: true,
    };
  }

  if (errorMessage) {
    return {
      stateKey: "defaultError",
      tooltipContent: errorMessage,
      iconColor: "error",
      disabled: true,
    };
  }

  if (hasNoSuggestions) {
    return {
      stateKey: "noSuggestions",
      tooltipContent: "connectorBuilder.assist.tooltip.noSuggestions",
      iconColor: "disabled",
      disabled: true,
    };
  }

  if (agreesWithCurrentConfig) {
    return {
      stateKey: "agreeWithConfig",
      tooltipContent: "connectorBuilder.assist.tooltip.agreeWithConfig",
      iconColor: "disabled",
      disabled: true,
    };
  }

  return {
    stateKey: "success",
    iconColor: "primary",
    disabled: false,
  };
};

export interface AssistButtonProps {
  assistKey: AssistKey;
  streamId?: StreamId;
}

interface AssistButtonConfig {
  useHook: (
    input: BuilderAssistInputAllParams
  ) => UseQueryResult<BuilderAssistManifestResponse, HttpError<KnownExceptionInfo>>;
  useHookParams: Array<keyof BuilderAssistInputAllParams>;
  formPathToSet: string | ((streamNum: number) => string);
  propertiesToPluck?: string[];
}

const assistButtonConfigs: { [key in AssistKey]: AssistButtonConfig } = {
  urlbase: {
    useHook: useBuilderAssistFindUrlBase,
    useHookParams: [],
    formPathToSet: "global.urlBase",
  },
  auth: {
    useHook: useBuilderAssistFindAuth,
    useHookParams: [],
    formPathToSet: "global.authenticator",
  },
  metadata: {
    useHook: useBuilderAssistStreamMetadata,
    useHookParams: ["stream_name", "stream_response"],
    formPathToSet: (streamNum: number) => `streams.${streamNum}`,
    propertiesToPluck: ["urlPath", "httpMethod", "primaryKey"],
  },
  record_selector: {
    useHook: useBuilderAssistStreamResponse,
    useHookParams: ["stream_name", "stream_response"],
    formPathToSet: (streamNum: number) => `streams.${streamNum}.recordSelector`,
  },
  paginator: {
    useHook: useBuilderAssistFindStreamPaginator,
    useHookParams: ["stream_name", "stream_response"],
    formPathToSet: (streamNum: number) => `streams.${streamNum}.paginator`,
  },
  request_options: {
    useHook: useBuilderAssistFindRequestOptions,
    useHookParams: ["stream_name", "stream_response"],
    formPathToSet: (streamNum: number) => `streams.${streamNum}.requestOptions`,
  },
  incremental_sync: {
    useHook: useBuilderAssistFindIncrementalSync,
    useHookParams: ["stream_name", "stream_response"],
    formPathToSet: (streamNum: number) => `streams.${streamNum}.incrementalSync`,
  },
};

/**
 * This is a helper hook that allows us to use the stream data without having to specify the stream number.
 * If the stream number is not specified, the stream data will be undefined.
 */
const useOptionalStreamData = (streamNum?: number) => {
  const noStreamSpecified = streamNum === undefined;
  const stream_name = useWatch({ name: `formValues.streams.${streamNum}.name`, disabled: noStreamSpecified });
  const stream_response_json_string = useWatch({
    name: `formValues.streams.${streamNum}.schema`,
    disabled: noStreamSpecified,
  });
  const stream_response = !noStreamSpecified ? computeStreamResponse(stream_response_json_string) : undefined;

  return { stream_name, stream_response };
};

export const AssistButton: React.FC<AssistButtonProps> = ({ assistKey, streamId }) => {
  const streams = useBuilderWatch("formValues.streams");
  const { stream_name, stream_response } = useOptionalStreamData(streamId?.index);

  const config = assistButtonConfigs[assistKey];
  const hookParams = useMemo(
    () => pick({ stream_name, stream_response }, config.useHookParams),
    [stream_name, stream_response, config.useHookParams]
  );

  const { assistEnabled } = useConnectorBuilderFormState();
  if (!assistEnabled) {
    return null;
  }

  if (streamId && streamId.type !== "stream") {
    return null;
  }

  const streamNum = streamId?.index;

  if (streamNum && streams[streamNum].requestType === "async") {
    return null;
  }

  return <InternalAssistButton assistKey={assistKey} hookParams={hookParams} streamNum={streamNum} {...config} />;
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
  const openapi_spec_url = assistData?.openapiSpecUrl?.trim();
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
      variant="magic"
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
  assistKey: AssistKey;
  hookParams: BuilderAssistInputAllParams;
}

const InternalAssistButton: React.FC<InternalAssistButtonProps> = ({
  useHook,
  hookParams,
  propertiesToPluck,
  streamNum,
  formPathToSet,
  assistKey,
}) => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();

  const { projectId } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();

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
  const openapi_spec_url = assistData?.openapiSpecUrl?.trim();
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

  // Process the error from the assist mutation
  const errorMessage = useMemo(() => {
    const formErrors = parseAssistErrorToFormErrors(error);
    if (formErrors.length > 0) {
      return formErrors[0].errorMessage;
    }

    if (isError) {
      return error?.message || "connectorBuilder.assist.tooltip.defaultError";
    }

    return undefined;
  }, [error, isError]);

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

  const showDataPreview = () => {
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONNECTOR_BUILDER_ASSIST_SUGGESTION_VIEWED, {
      projectId,
      assistKey,
      disabled: assistButtonState.disabled,
      assistState: assistButtonState.stateKey,
      metadata: data?.metadata,
    });

    if (assistButtonState.disabled) {
      return;
    }

    setValue(previewKey, previewValue);
  };
  const hideDataPreview = () => {
    setValue(previewKey, undefined);
  };

  const buttonClickHandler = () => {
    if (assistButtonState.disabled) {
      return;
    }
    hideDataPreview();
    setValue(formKey, valueToSet, {
      shouldValidate: true,
      shouldDirty: true,
      shouldTouch: true,
    });
    if (updatedFullForm?.inputs) {
      setValue("formValues.inputs", mergeInputs(currentInputs, updatedFullForm.inputs));
    }
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONNECTOR_BUILDER_ASSIST_SUGGESTION_ACCEPTED, {
      projectId,
      assistKey,
      disabled: assistButtonState.disabled,
      assistState: assistButtonState.stateKey,
      metadata: data?.metadata,
    });
  };

  const button = (
    <Button
      type="button"
      variant="magic"
      onClick={buttonClickHandler}
      icon="aiStars"
      disabled={assistButtonState.disabled}
      isLoading={isFetching}
      iconColor={assistButtonState.iconColor}
      onMouseOver={showDataPreview}
      onMouseOut={hideDataPreview}
    />
  );

  if (assistButtonState.tooltipContent) {
    return <Tooltip control={button}>{formatMessage({ id: assistButtonState.tooltipContent })}</Tooltip>;
  }
  return button;
};
