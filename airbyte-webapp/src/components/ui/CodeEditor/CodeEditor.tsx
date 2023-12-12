import Editor, { Monaco, useMonaco } from "@monaco-editor/react";
import { editor } from "monaco-editor/esm/vs/editor/editor.api";
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
  showSuggestions = true,
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

  useEffect(() => {
    setAirbyteTheme(monaco);
  }, [setAirbyteTheme, monaco]);

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
