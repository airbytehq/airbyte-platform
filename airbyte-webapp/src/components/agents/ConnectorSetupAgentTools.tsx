import { useCallback, useEffect, useMemo, useRef } from "react";
import { useFormContext } from "react-hook-form";

import { useCheckConfigurationTool } from "components/agents/tools/hooks/useCheckConfigurationTool";
import { useRequestSecretInputTool } from "components/agents/tools/hooks/useRequestSecretInputTool";
import { useSaveConfigurationTool } from "components/agents/tools/hooks/useSaveConfigurationTool";
import { useSubmitConfigurationTool } from "components/agents/tools/hooks/useSubmitConfigurationTool";
import { TOOL_NAMES } from "components/agents/tools/toolNames";
import { type ClientTools } from "components/chat/hooks/useChatMessages";

import { ActorType } from "core/api/types/AirbyteClient";
import { type ConnectorFormValues } from "views/Connector/ConnectorForm/types";

interface ConnectorSetupAgentToolsProps {
  actorDefinitionId?: string;
  actorType: ActorType;
  onSubmitStep: (values: {
    name: string;
    serviceType: string;
    connectionConfiguration: Record<string, unknown>;
  }) => void;
  onClientToolsReady: (tools: ClientTools) => void;
  onSecretInputStateChange: (state: {
    isSecretInputActive: boolean;
    secretFieldPath: string[];
    secretFieldName: string | undefined;
    isMultiline: boolean;
    submitSecret: (message: string) => void;
    dismissSecret: (reason?: string) => void;
  }) => void;
  onFormValuesReady?: (getFormValues: () => Record<string, unknown>) => void;
  touchedSecretFieldsRef: React.MutableRefObject<Set<string>>;
  addTouchedSecretField: (path: string) => void;
}

/**
 * Component that sets up chat tools for the connector setup agent.
 * Must be rendered inside FormProvider as saveDraftTool needs form context.
 */
export const ConnectorSetupAgentTools: React.FC<ConnectorSetupAgentToolsProps> = ({
  actorDefinitionId,
  actorType,
  onSubmitStep,
  onClientToolsReady,
  onSecretInputStateChange,
  onFormValuesReady,
  touchedSecretFieldsRef,
  addTouchedSecretField,
}) => {
  const { getValues } = useFormContext();

  // Create callback that returns form values with proper typing
  const getFormValues = useCallback(() => getValues() as ConnectorFormValues, [getValues]);

  // Setup client tools - form is single source of truth
  const submitTool = useSubmitConfigurationTool({
    actorDefinitionId,
    onSubmitSourceStep: onSubmitStep,
    getFormValues,
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

  const checkTool = useCheckConfigurationTool({
    actorDefinitionId,
    actorType,
    getFormValues,
  });

  const clientTools: ClientTools = useMemo(
    () => ({
      [TOOL_NAMES.SUBMIT_CONFIGURATION]: submitTool,
      [TOOL_NAMES.SAVE_DRAFT_CONFIGURATION]: saveDraftTool,
      [TOOL_NAMES.REQUEST_SECRET_INPUT]: secretInputTool,
      [TOOL_NAMES.CHECK_CONFIGURATION]: checkTool,
    }),
    [submitTool, saveDraftTool, secretInputTool, checkTool]
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

  // Notify parent when form getValues is ready
  useEffect(() => {
    if (onFormValuesReady) {
      onFormValuesReady(() => getValues());
    }
  }, [onFormValuesReady, getValues]);

  return null;
};
