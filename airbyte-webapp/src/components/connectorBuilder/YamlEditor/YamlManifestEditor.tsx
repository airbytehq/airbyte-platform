import classNames from "classnames";
import debounce from "lodash/debounce";
import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexItem } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCustomComponentsEnabled } from "core/api";
import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";
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

  const areCustomComponentsEnabled = useCustomComponentsEnabled();
  const customComponentsCodeValue = useBuilderWatch("customComponentsCode");

  // We want to show the custom components tab any time the custom components code is set.
  // This is to ensure a user can still remove the custom components code if they want to (in the event of a fork).
  const showCustomComponentsTab = areCustomComponentsEnabled || customComponentsCodeValue;

  const [selectedTab, setSelectedTab] = useState(TAB_MANIFEST);

  return (
    <div className={styles.container}>
      <Sidebar yamlSelected>
        <FlexItem grow>
          <TestingValuesMenu />
        </FlexItem>
      </Sidebar>
      <div className={styles.editorContainer}>
        {showCustomComponentsTab && (
          <Tabs gap="none" className={styles.tabContainer}>
            {Object.values(tabs).map((tab) => (
              <ButtonTab
                key={tab}
                id={tab}
                name={
                  <>
                    {tab}
                    {tab === TAB_COMPONENTS && (
                      <InfoTooltip placement="top">
                        <FormattedMessage
                          id="connectorBuilder.customComponents.tooltip"
                          values={{
                            lnk: (...lnk: React.ReactNode[]) => (
                              <ExternalLink href={links.connectorBuilderCustomComponents}>{lnk}</ExternalLink>
                            ),
                          }}
                        />
                      </InfoTooltip>
                    )}
                  </>
                }
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
