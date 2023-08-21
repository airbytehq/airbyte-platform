import Editor, { Monaco, useMonaco } from "@monaco-editor/react";
import { editor } from "monaco-editor/esm/vs/editor/editor.api";
import React, { useEffect } from "react";

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

function cssCustomPropToHex(cssCustomProperty: string) {
  const varName = cssCustomProperty.replace(/var\(|\)/g, "");
  const bodyStyles = window.getComputedStyle(document.body);
  const hslString = bodyStyles.getPropertyValue(varName).trim();
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
  showSuggestions = true,
}) => {
  const monaco = useMonaco();
  const { theme: airbyteTheme } = useAirbyteTheme();

  const setAirbyteTheme = (monaco: Monaco | null) => {
    monaco?.editor.defineTheme("airbyte", {
      base: "vs",
      inherit: true,
      rules: [
        { token: "", foreground: cssCustomPropToHex(styles.string) },
        { token: "string", foreground: cssCustomPropToHex(styles.string) },
        { token: "string.yaml", foreground: cssCustomPropToHex(styles.string) },
        { token: "string.value.json", foreground: cssCustomPropToHex(styles.string) },
        { token: "string.key.json", foreground: cssCustomPropToHex(styles.type) },
        { token: "type", foreground: cssCustomPropToHex(styles.type) },
        { token: "number", foreground: cssCustomPropToHex(styles.number) },
        { token: "delimiter", foreground: cssCustomPropToHex(styles.delimiter) },
        { token: "keyword", foreground: cssCustomPropToHex(styles.keyword) },
        { token: "comment", foreground: cssCustomPropToHex(styles.comment) },
      ],
      colors: {
        "editor.background": "#00000000", // transparent, so that parent background is shown instead
        "editorLineNumber.foreground": cssCustomPropToHex(styles.lineNumber),
        "editorLineNumber.activeForeground": cssCustomPropToHex(styles.lineNumberActive),
        "editorIndentGuide.background": cssCustomPropToHex(styles.line),
        "editor.lineHighlightBorder": cssCustomPropToHex(styles.line),
        "editorCursor.foreground": cssCustomPropToHex(styles.cursor),
        "scrollbar.shadow": cssCustomPropToHex(styles.scrollShadow),
        "editor.selectionBackground": cssCustomPropToHex(styles.selection),
        "editor.inactiveSelectionBackground": cssCustomPropToHex(styles.inactiveSelection),
      },
    });
  };

  useEffect(() => {
    setAirbyteTheme(monaco);
  }, [airbyteTheme, monaco]);

  return (
    <Editor
      beforeMount={setAirbyteTheme}
      onMount={onMount}
      loading={<Spinner />}
      value={value}
      onChange={onChange}
      language={language}
      theme="airbyte"
      height={height}
      options={{
        lineNumbersMinChars: lineNumberCharacterWidth ?? 2,
        readOnly: readOnly ?? false,
        automaticLayout,
        matchBrackets: "always",
        minimap: {
          enabled: false,
        },
        suggestOnTriggerCharacters: showSuggestions,
      }}
    />
  );
};
