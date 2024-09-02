import Editor, { Monaco, useMonaco } from "@monaco-editor/react";
import { KeyCode, KeyMod, editor } from "monaco-editor/esm/vs/editor/editor.api";
import React, { useCallback, useEffect } from "react";

import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./CodeEditor.module.scss";
import { Spinner } from "../Spinner";

interface CodeEditorProps {
  value: string;
  language?: string;
  readOnly?: boolean;
  onChange?: (value: string | undefined) => void;
  height?: string;
  lineNumberCharacterWidth?: number;
  onMount?: (editor: editor.IStandaloneCodeEditor) => void;
  automaticLayout?: boolean;
  showSuggestions?: boolean;
  paddingTop?: boolean;
  disabled?: boolean;
  bubbleUpUndoRedo?: boolean;
}

function hslToHex(hue: number, saturation: number, lightness: number) {
  lightness /= 100;
  const chroma = (saturation * Math.min(lightness, 1 - lightness)) / 100;
  const convertWithOffset = (offset: number) => {
    const normalizedHue = (offset + hue / 30) % 12;
    const color = lightness - chroma * Math.max(Math.min(normalizedHue - 3, 9 - normalizedHue, 1), -1);
    // convert to Hex and prefix "0" if needed
    return Math.round(255 * color)
      .toString(16)
      .padStart(2, "0");
  };
  return `#${convertWithOffset(0)}${convertWithOffset(8)}${convertWithOffset(4)}`;
}

function cssCustomPropToHex(hslString: string) {
  const [, h, s, l] = /^hsl\(([0-9]+), ([0-9]+)%, ([0-9]+)%\)$/.exec(hslString)?.map(Number) ?? [0, 0, 0, 0];
  return hslToHex(h, s, l);
}

export const CodeEditor: React.FC<CodeEditorProps> = ({
  value,
  language,
  readOnly,
  onChange,
  height,
  lineNumberCharacterWidth,
  onMount,
  automaticLayout,
  paddingTop,
  showSuggestions = true,
  bubbleUpUndoRedo,
  disabled,
}) => {
  const monaco = useMonaco();
  const { colorValues } = useAirbyteTheme();

  const setAirbyteTheme = useCallback(
    (monaco: Monaco | null) => {
      monaco?.editor.defineTheme("airbyte", {
        base: "vs",
        inherit: true,
        rules: [
          { token: "", foreground: cssCustomPropToHex(colorValues[styles.string]) },
          { token: "string", foreground: cssCustomPropToHex(colorValues[styles.string]) },
          { token: "string.yaml", foreground: cssCustomPropToHex(colorValues[styles.string]) },
          { token: "string.value.json", foreground: cssCustomPropToHex(colorValues[styles.string]) },
          { token: "string.key.json", foreground: cssCustomPropToHex(colorValues[styles.type]) },
          { token: "type", foreground: cssCustomPropToHex(colorValues[styles.type]) },
          { token: "number", foreground: cssCustomPropToHex(colorValues[styles.number]) },
          { token: "delimiter", foreground: cssCustomPropToHex(colorValues[styles.delimiter]) },
          { token: "keyword", foreground: cssCustomPropToHex(colorValues[styles.keyword]) },
          { token: "comment", foreground: cssCustomPropToHex(colorValues[styles.comment]) },
        ],
        colors: {
          "editor.background": "#00000000", // transparent, so that parent background is shown instead
          "editorLineNumber.foreground": cssCustomPropToHex(colorValues[styles.lineNumber]),
          "editorLineNumber.activeForeground": cssCustomPropToHex(colorValues[styles.lineNumberActive]),
          "editorIndentGuide.background": cssCustomPropToHex(colorValues[styles.line]),
          "editor.lineHighlightBorder": cssCustomPropToHex(colorValues[styles.line]),
          "editorCursor.foreground": cssCustomPropToHex(colorValues[styles.cursor]),
          "scrollbar.shadow": cssCustomPropToHex(colorValues[styles.scrollShadow]),
          "editor.selectionBackground": cssCustomPropToHex(colorValues[styles.selection]),
          "editor.inactiveSelectionBackground": cssCustomPropToHex(colorValues[styles.inactiveSelection]),
        },
      });
    },
    [colorValues]
  );
  const paddingTopNum = Number(styles.paddingTop.replace("px", ""));
  const paddingTopValue = paddingTopNum.toString() === "NaN" ? 15 : paddingTopNum;

  useEffect(() => {
    setAirbyteTheme(monaco);
  }, [setAirbyteTheme, monaco]);

  return (
    <Editor
      beforeMount={setAirbyteTheme}
      onMount={(editor: editor.IStandaloneCodeEditor) => {
        // In cases like the Builder, we have our own undo/redo framework in place, so we want to bubble up the
        // undo/redo keyboard commands to the surrounding page when the user presses those keys, rather than triggering
        // monaco's internal undo/redo implementation.
        editor.addCommand(KeyMod.CtrlCmd | KeyCode.KeyZ, () =>
          bubbleUpUndoRedo ? bubbleUpUndoRedoEvent("undo", editor) : editor.trigger(undefined, "undo", undefined)
        );
        editor.addCommand(KeyMod.CtrlCmd | KeyCode.KeyY, () =>
          bubbleUpUndoRedo ? bubbleUpUndoRedoEvent("redo", editor) : editor.trigger(undefined, "redo", undefined)
        );
        editor.addCommand(KeyMod.CtrlCmd | KeyMod.Shift | KeyCode.KeyZ, () =>
          bubbleUpUndoRedo ? bubbleUpUndoRedoEvent("redo", editor) : editor.trigger(undefined, "redo", undefined)
        );

        onMount?.(editor);
      }}
      loading={<Spinner />}
      value={value}
      onChange={onChange}
      language={language}
      theme="airbyte"
      height={height}
      options={{
        lineNumbersMinChars: lineNumberCharacterWidth ?? 2,
        readOnly: (readOnly || disabled) ?? false,
        automaticLayout,
        matchBrackets: "always",
        minimap: {
          enabled: false,
        },
        suggestOnTriggerCharacters: showSuggestions,
        padding: paddingTop
          ? {
              top: paddingTopValue,
            }
          : {},
        fixedOverflowWidgets: true,
      }}
    />
  );
};

const bubbleUpUndoRedoEvent = (type: "undo" | "redo", editor: editor.IStandaloneCodeEditor) => {
  const event = new KeyboardEvent("keydown", {
    key: "z",
    code: "KeyZ",
    ctrlKey: true,
    shiftKey: type === "redo",
    bubbles: true,
    cancelable: true,
  });
  editor.getDomNode()?.dispatchEvent(event);
};
