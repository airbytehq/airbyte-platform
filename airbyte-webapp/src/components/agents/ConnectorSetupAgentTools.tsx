import { useEffect, useMemo, useRef } from "react";

import { useCheckConfigurationTool } from "components/agents/tools/hooks/useCheckConfigurationTool";
import { useRequestSecretInputTool } from "components/agents/tools/hooks/useRequestSecretInputTool";
import { useSaveConfigurationTool } from "components/agents/tools/hooks/useSaveConfigurationTool";
import { useSubmitConfigurationTool } from "components/agents/tools/hooks/useSubmitConfigurationTool";
import { TOOL_NAMES } from "components/agents/tools/toolNames";
import { type SecretsMap } from "components/agents/types";
import { type ClientTools } from "components/chat/hooks/useChatMessages";

interface ConnectorSetupAgentToolsProps {
  actorDefinitionId?: string;
  actorType: "source" | "destination";
  onSubmitStep: (values: {
    name: string;
    serviceType: string;
    connectionConfiguration: Record<string, unknown>;
  }) => void;
  setSecrets: React.Dispatch<React.SetStateAction<SecretsMap>>;
  getSecrets: () => SecretsMap;
  onClientToolsReady: (tools: ClientTools) => void;
  onSecretInputStateChange: (state: {
    isSecretInputActive: boolean;
    secretFieldPath: string[];
    secretFieldName: string | undefined;
    isMultiline: boolean;
    submitSecret: (message: string) => void;
  }) => void;
}

/**
 * Component that sets up chat tools for the connector setup agent.
 * Must be rendered inside FormProvider as saveDraftTool needs form context.
 */
export const ConnectorSetupAgentTools: React.FC<ConnectorSetupAgentToolsProps> = ({
  actorDefinitionId,
  actorType,
  onSubmitStep,
  setSecrets,
  getSecrets,
  onClientToolsReady,
  onSecretInputStateChange,
}) => {
  // Setup client tools - saveDraftTool uses useFormContext()
  const submitTool = useSubmitConfigurationTool({
    actorDefinitionId,
    onSubmitSourceStep: onSubmitStep,
    getSecrets,
  });
  const saveDraftTool = useSaveConfigurationTool({ getSecrets });
  const {
    handler: secretInputTool,
    isSecretInputActive,
    secretFieldPath,
    secretFieldName,
    isMultiline,
    submitSecret,
  } = useRequestSecretInputTool({ setSecrets });
  const checkTool = useCheckConfigurationTool({
    actorDefinitionId,
    actorType,
    getSecrets,
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
    });
  }, [isSecretInputActive, secretFieldPath, secretFieldName, isMultiline, submitSecret, onSecretInputStateChange]);

  return null;
};
