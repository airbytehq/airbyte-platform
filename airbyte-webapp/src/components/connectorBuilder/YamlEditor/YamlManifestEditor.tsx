import classNames from "classnames";
import debounce from "lodash/debounce";
import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";

import { FlexItem } from "components/ui/Flex";
import { ButtonTab, Tabs } from "components/ui/Tabs";

import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { useExperiment } from "hooks/services/Experiment";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { YamlEditor } from "./YamlEditor";
import styles from "./YamlManifestEditor.module.scss";
import { CustomComponentsEditor } from "../CustomComponentsEditor/CustomComponentsEditor";
import { Sidebar } from "../Sidebar";
import { TestingValuesMenu } from "../StreamTestingPanel/TestingValuesMenu";
import { useBuilderWatch } from "../useBuilderWatch";

const TAB_MANIFEST = "manifest.yaml";
const TAB_COMPONENTS = "components.py";

const tabs = {
  yaml: TAB_MANIFEST,
  components: TAB_COMPONENTS,
};

export const YamlManifestEditor: React.FC = () => {
  const {
    setYamlEditorIsMounted,
    setYamlIsValid,
    updateJsonManifest,
    undoRedo: { clearHistory },
  } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const yamlManifestValue = useBuilderWatch("yaml");
  // debounce the setJsonManifest calls so that it doesnt result in a network call for every keystroke
  const debouncedUpdateJsonManifest = useMemo(() => debounce(updateJsonManifest, 200), [updateJsonManifest]);

  const areCustomComponentsEnabled = useExperiment("connectorBuilder.customComponents");
  const [selectedTab, setSelectedTab] = useState(TAB_MANIFEST);

  return (
    <div className={styles.container}>
      <Sidebar yamlSelected>
        <FlexItem grow>
          <TestingValuesMenu />
        </FlexItem>
      </Sidebar>
      <div className={styles.editorContainer}>
        {areCustomComponentsEnabled && (
          <Tabs gap="none" className={styles.tabContainer}>
            {Object.values(tabs).map((tab) => (
              <ButtonTab
                key={tab}
                id={tab}
                name={tab}
                className={classNames(styles.editorTab, { [styles.activeTab]: selectedTab === tab })}
                isActive={selectedTab === tab}
                onSelect={() => {
                  setSelectedTab(tab);
                }}
              />
            ))}
          </Tabs>
        )}
        {selectedTab === TAB_MANIFEST && (
          <YamlEditor
            value={yamlManifestValue}
            onChange={(value: string | undefined) => {
              setValue("yaml", value ?? "");
              clearHistory();
            }}
            onSuccessfulLoad={(json: unknown) => {
              setYamlIsValid(true);
              debouncedUpdateJsonManifest(json as ConnectorManifest);
            }}
            onYamlException={(_) => setYamlIsValid(false)}
            onMount={(_) => {
              setYamlEditorIsMounted(true);
            }}
            lineNumberCharacterWidth={6}
            paddingTop
          />
        )}
        {selectedTab === TAB_COMPONENTS && <CustomComponentsEditor />}
      </div>
    </div>
  );
};
