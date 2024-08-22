import { useIntl } from "react-intl";

import { Switch } from "components/ui/Switch/Switch";

import { useExperiment } from "hooks/services/Experiment";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderCard } from "./BuilderCard";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";

type ChangeEventFunction = (e: React.ChangeEvent<HTMLInputElement>) => void;

export const AssistSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const { assistEnabled, setAssistEnabled } = useConnectorBuilderFormState();
  const onChange: ChangeEventFunction = (e) => {
    setAssistEnabled(e.currentTarget.checked);
  };
  const isAIEnabled = useExperiment("connectorBuilder.aiAssist.enabled", false);
  if (!isAIEnabled) {
    return null;
  }

  const rightComponent = (
    <AssistSwitch
      title={formatMessage({ id: "connectorBuilder.assist.section.title" })}
      assistEnabled={assistEnabled}
      onChange={onChange}
    />
  );
  return (
    <BuilderCard label="Metadata" rightComponent={rightComponent}>
      <CollapsedChildren enabled={assistEnabled}>
        <BuilderFieldWithInputs
          type="string"
          label={formatMessage({ id: "connectorBuilder.globalConfiguration.assist.docsUrl" })}
          optional
          manifestPath="metadata.assist.docsUrl"
          path="formValues.assist.docsUrl"
        />
        <BuilderFieldWithInputs
          type="string"
          label={formatMessage({ id: "connectorBuilder.globalConfiguration.assist.openApiSpecUrl" })}
          optional
          manifestPath="metadata.assist.openApiSpecUrl"
          path="formValues.assist.openApiSpecUrl"
        />
      </CollapsedChildren>
    </BuilderCard>
  );
};

const AssistSwitch: React.FC<{
  title: string;
  assistEnabled: boolean;
  onChange: ChangeEventFunction;
}> = ({ title, assistEnabled, onChange }) => {
  return (
    <>
      <span>{title}</span>
      <Switch checked={assistEnabled} onChange={onChange} />
    </>
  );
};

const CollapsedChildren: React.FC<React.PropsWithChildren<{ enabled: boolean }>> = ({ children, enabled }) => {
  if (enabled) {
    return <>{children}</>;
  }
  return null;
};
