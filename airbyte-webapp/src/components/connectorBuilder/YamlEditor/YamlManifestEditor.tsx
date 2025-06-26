import classNames from "classnames";
import debounce from "lodash/debounce";
import isEqual from "lodash/isEqual";
import React, { useMemo, useRef, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexItem } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCustomComponentsEnabled } from "core/api";
import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { useConnectorBuilderResolve } from "core/services/connectorBuilder/ConnectorBuilderResolveContext";
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
    undoRedo: { clearHistory },
    setYamlIsDirty,
  } = useConnectorBuilderFormState();
  const { resolveManifest } = useConnectorBuilderResolve();
  const { setValue } = useFormContext();
  const yamlManifestValue = useBuilderWatch("yaml");

  // Add a simple counter reference to track the latest call
  const lastCallIdRef = useRef(0);

  const debouncedResolveAndUpdateManifest: (newManifest: ConnectorManifest) => void = useMemo(
    () =>
      debounce(async (newManifest: ConnectorManifest) => {
        // Increment counter to track this call
        const thisCallId = ++lastCallIdRef.current;

        resolveManifest(newManifest).then((resolvedManifest) => {
          // Only update state if this is still the latest call
          // This prevents yamlIsDirty from being set back to false when subsequent YAML changes are made
          // while previous resolve calls are still in progress.
          if (thisCallId === lastCallIdRef.current) {
            setValue("manifest", resolvedManifest.manifest);
            setYamlIsDirty(false);
          }
        });
      }, 500),
    [resolveManifest, setValue, setYamlIsDirty]
  );

  const areCustomComponentsEnabled = useCustomComponentsEnabled();
  const customComponentsCodeValue = useBuilderWatch("customComponentsCode");

  // We want to show the custom components tab any time the custom components code is set.
  // This is to ensure a user can still remove the custom components code if they want to (in the event of a fork).
  const showCustomComponentsTab = areCustomComponentsEnabled || customComponentsCodeValue;

  const [selectedTab, setSelectedTab] = useState(TAB_MANIFEST);
  const lastLoadedManifest = useRef<ConnectorManifest | null>(null);

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
              const newManifest = json as ConnectorManifest;
              if (!lastLoadedManifest.current) {
                lastLoadedManifest.current = newManifest;
              } else if (!isEqual(lastLoadedManifest.current, newManifest)) {
                setYamlIsDirty(true);
                lastLoadedManifest.current = newManifest;
                debouncedResolveAndUpdateManifest(newManifest);
              }
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
