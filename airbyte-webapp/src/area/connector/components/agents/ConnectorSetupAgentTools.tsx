import { useCallback, useEffect, useMemo, useRef } from "react";
import { useFormContext } from "react-hook-form";

import { useCheckConfigurationTool } from "area/connector/components/agents/tools/hooks/useCheckConfigurationTool";
import { useRequestOAuthTool } from "area/connector/components/agents/tools/hooks/useRequestOAuthTool";
import { useRequestSecretInputTool } from "area/connector/components/agents/tools/hooks/useRequestSecretInputTool";
import { useSaveConfigurationTool } from "area/connector/components/agents/tools/hooks/useSaveConfigurationTool";
import { useSubmitConfigurationTool } from "area/connector/components/agents/tools/hooks/useSubmitConfigurationTool";
import { TOOL_NAMES } from "area/connector/components/agents/tools/toolNames";
import { type ClientTools } from "area/connector/components/chat/hooks/useChatMessages";
import { useConnectorForm } from "area/connector/components/ConnectorForm/connectorFormContext";
import { type ConnectorCardValues, type ConnectorFormValues } from "area/connector/components/ConnectorForm/types";
import { ActorType } from "core/api/types/AirbyteClient";
import { type ConnectorT } from "core/domain/connector";

type AgentConnectorValues = ConnectorCardValues & { createAsDraft?: true; setupFlow: "agent" };

interface ConnectorSetupAgentToolsProps<T extends ConnectorT> {
  actorDefinitionId?: string;
  actorType: ActorType;
  onSubmitStep: (values: {
    name: string;
    serviceType: string;
    connectionConfiguration: Record<string, unknown>;
  }) => Promise<void> | void;
  onClientToolsReady: (tools: ClientTools) => void;
  onSecretInputStateChange: (state: {
    isSecretInputActive: boolean;
    secretFieldPath: string[];
    secretFieldName: string | undefined;
    isMultiline: boolean;
    submitSecret: (message: string) => void;
    dismissSecret: (reason?: string) => void;
  }) => void;
  onOAuthStateChange?: (state: {
    isOAuthPendingUserAction: boolean;
    startOAuth: () => void;
    cancelOAuth: () => void;
  }) => void;
  onFormValuesReady?: (getFormValues: () => Record<string, unknown>) => void;
  onCheckComplete?: (success: boolean) => void;
  onSaveDraft?: (values: AgentConnectorValues, existingDraft?: T) => Promise<T>;
  onDraftPromoted?: (draft: T, values: AgentConnectorValues) => Promise<void> | void;
  touchedSecretFieldsRef: React.MutableRefObject<Set<string>>;
  addTouchedSecretField: (path: string) => void;
}

/**
 * Component that sets up chat tools for the connector setup agent.
 * Must be rendered inside FormProvider as saveDraftTool needs form context.
 */
export const ConnectorSetupAgentTools = <T extends ConnectorT>({
  actorDefinitionId,
  actorType,
  onSubmitStep,
  onClientToolsReady,
  onSecretInputStateChange,
  onOAuthStateChange,
  onFormValuesReady,
  onCheckComplete,
  onSaveDraft,
  onDraftPromoted,
  touchedSecretFieldsRef,
  addTouchedSecretField,
}: ConnectorSetupAgentToolsProps<T>) => {
  const { getValues: getRawValues } = useFormContext<ConnectorFormValues>();
  const { castValues } = useConnectorForm();
  const draftConnectorRef = useRef<T>();
  const draftCheckSucceededRef = useRef(false);

  // Create callback that returns cleaned form values (empty strings removed, schema-cast)
  const getFormValues = useCallback(() => castValues(getRawValues()), [getRawValues, castValues]);

  // Setup client tools - form is single source of truth
  const submitTool = useSubmitConfigurationTool<T>({
    actorDefinitionId,
    onSubmitSourceStep: onSubmitStep,
    getFormValues,
    getDraftConnector: () => draftConnectorRef.current,
    wasDraftCheckSuccessful: () => draftCheckSucceededRef.current,
    onDraftPromoted,
  });
  const saveDraftTool = useSaveConfigurationTool(touchedSecretFieldsRef.current);

  const {
    handler: secretInputTool,
    isSecretInputActive,
    secretFieldPath,
    secretFieldName,
    isMultiline,
    submitSecret,
    dismissSecret,
  } = useRequestSecretInputTool(addTouchedSecretField);

  const {
    handler: oauthTool,
    isOAuthPendingUserAction,
    startOAuth,
    cancelOAuth,
  } = useRequestOAuthTool({
    actorDefinitionId,
    actorType,
  });

  const checkTool = useCheckConfigurationTool<T>({
    actorDefinitionId,
    actorType,
    getFormValues,
    onCheckComplete,
    onSaveDraft,
    draftConnectorRef,
    draftCheckSucceededRef,
  });

  const clientTools: ClientTools = useMemo(
    () => ({
      [TOOL_NAMES.SUBMIT_CONFIGURATION]: submitTool,
      [TOOL_NAMES.SAVE_DRAFT_CONFIGURATION]: saveDraftTool,
      [TOOL_NAMES.REQUEST_SECRET_INPUT]: secretInputTool,
      [TOOL_NAMES.REQUEST_OAUTH_AUTHENTICATION]: oauthTool,
      [TOOL_NAMES.CHECK_CONFIGURATION]: checkTool,
    }),
    [submitTool, saveDraftTool, secretInputTool, oauthTool, checkTool]
  );

  // Use a ref to track if we've already initialized tools
  const initializedRef = useRef(false);

  // Notify parent when tools are ready (only once on mount)
  useEffect(() => {
    if (!initializedRef.current) {
      initializedRef.current = true;
      onClientToolsReady(clientTools);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Run only once on mount

  // Notify parent whenever secret input state changes
  useEffect(() => {
    onSecretInputStateChange({
      isSecretInputActive,
      secretFieldPath,
      secretFieldName,
      isMultiline,
      submitSecret,
      dismissSecret,
    });
  }, [
    isSecretInputActive,
    secretFieldPath,
    secretFieldName,
    isMultiline,
    submitSecret,
    dismissSecret,
    onSecretInputStateChange,
  ]);

  // Notify parent whenever OAuth state changes
  useEffect(() => {
    if (onOAuthStateChange) {
      onOAuthStateChange({
        isOAuthPendingUserAction,
        startOAuth,
        cancelOAuth,
      });
    }
  }, [isOAuthPendingUserAction, startOAuth, cancelOAuth, onOAuthStateChange]);

  // Notify parent when form getValues is ready
  useEffect(() => {
    if (onFormValuesReady) {
      onFormValuesReady(() => castValues(getRawValues()));
    }
  }, [onFormValuesReady, castValues, getRawValues]);

  return null;
};
