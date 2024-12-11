import { useMonaco } from "@monaco-editor/react";
import { load, Mark, YAMLException } from "js-yaml";
import { editor } from "monaco-editor/esm/vs/editor/editor.api";
import React, { useRef, useCallback } from "react";
import { useUpdateEffect } from "react-use";

import { CodeEditor } from "components/ui/CodeEditor";

interface YamlEditorProps {
  value: string;
  onChange: (value: string | undefined) => void;
  onSuccessfulLoad?: (json: unknown, yaml: string) => void;
  onYamlException?: (e: YAMLException) => void;
  onMount?: (editor: editor.IStandaloneCodeEditor) => void;
  lineNumberCharacterWidth?: number;
  paddingTop?: boolean;
  bubbleUpUndoRedo?: boolean;
  readOnly?: boolean;
}

export const YamlEditor: React.FC<YamlEditorProps> = ({
  value,
  onChange,
  onSuccessfulLoad,
  onYamlException,
  onMount,
  lineNumberCharacterWidth,
  paddingTop,
  bubbleUpUndoRedo,
  readOnly,
}) => {
  const yamlEditorRef = useRef<editor.IStandaloneCodeEditor>();
  const monaco = useMonaco();
  const monacoRef = useRef(monaco);
  monacoRef.current = monaco;

  const validateAndSetMarkers = useCallback(() => {
    if (monacoRef.current && yamlEditorRef.current && value) {
      const errOwner = "yaml";
      const yamlEditorModel = yamlEditorRef.current.getModel();

      try {
        const json = load(value);
        onSuccessfulLoad?.(json, value);

        // clear editor error markers
        if (yamlEditorModel) {
          monacoRef.current.editor.setModelMarkers(yamlEditorModel, errOwner, []);
        }
      } catch (err) {
        if (err instanceof YAMLException) {
          onYamlException?.(err);
          const mark: Mark | undefined = err.mark;

          // set editor error markers
          if (yamlEditorModel && mark) {
            monacoRef.current.editor.setModelMarkers(yamlEditorModel, errOwner, [
              {
                startLineNumber: mark.line + 1,
                startColumn: mark.column + 1,
                endLineNumber: mark.line + 1,
                endColumn: mark.column + 2,
                message: err.message,
                severity: monacoRef.current.MarkerSeverity.Error,
              },
            ]);
          }
        }
      }
    }
  }, [onSuccessfulLoad, onYamlException, value]);

  useUpdateEffect(() => {
    validateAndSetMarkers();
  }, [validateAndSetMarkers]);

  return (
    <CodeEditor
      value={value}
      language="yaml"
      onChange={onChange}
      onMount={(editor) => {
        yamlEditorRef.current = editor;
        validateAndSetMarkers();
        onMount?.(editor);
      }}
      lineNumberCharacterWidth={lineNumberCharacterWidth}
      paddingTop={paddingTop}
      bubbleUpUndoRedo={bubbleUpUndoRedo}
      readOnly={readOnly}
    />
  );
};
