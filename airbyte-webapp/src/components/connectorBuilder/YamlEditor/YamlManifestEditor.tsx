import debounce from "lodash/debounce";
import React, { useMemo } from "react";
import { useFormContext } from "react-hook-form";

import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { YamlEditor } from "./YamlEditor";
import styles from "./YamlManifestEditor.module.scss";
import { Sidebar } from "../Sidebar";
import { useBuilderWatch } from "../types";

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

  return (
    <div className={styles.container}>
      <Sidebar yamlSelected />
      <div className={styles.editorContainer}>
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
      </div>
    </div>
  );
};
