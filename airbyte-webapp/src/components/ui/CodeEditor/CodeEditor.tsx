import Editor, { Monaco, useMonaco } from "@monaco-editor/react";
import { KeyCode, KeyMod, editor } from "monaco-editor/esm/vs/editor/editor.api";
import React, { useCallback, useEffect } from "react";

import {
  JINJA_TOKEN,
  NON_JINJA_TOKEN,
  JINJA_STRING_TOKEN,
  JINJA_OTHER_TOKEN,
  JINJA_CLOSING_BRACKET_TOKEN,
  JINJA_FIRST_BRACKET_TOKEN,
} from "components/connectorBuilder/Builder/jinja";

import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./CodeEditor.module.scss";
import { Spinner } from "../Spinner";

interface CodeEditorProps {
  className?: string;
  value: string;
  name?: string;
  language?: string;
  readOnly?: boolean;
  onChange?: (value: string | undefined) => void;
  onBlur?: (value: string) => void;
  height?: string;
  lineNumberCharacterWidth?: number;
  onMount?: (editor: editor.IStandaloneCodeEditor, monaco: Monaco) => void;
  showSuggestions?: boolean;
  paddingTop?: boolean;
  disabled?: boolean;
  bubbleUpUndoRedo?: boolean;
  beforeMount?: (monaco: Monaco) => void;
  options?: editor.IStandaloneEditorConstructionOptions;
  tabFocusMode?: boolean;
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

export function cssCustomPropToHex(hslString: string) {
  const [, h, s, l] = /^hsl\(([0-9]+), ([0-9]+)%, ([0-9]+)%\)$/.exec(hslString)?.map(Number) ?? [0, 0, 0, 0];
  return hslToHex(h, s, l);
}

let isTabFocusModeOn = false;

export const CodeEditor: React.FC<CodeEditorProps> = ({
  className,
  name,
  value,
  language,
  readOnly,
  onChange,
  onBlur,
  height,
  lineNumberCharacterWidth,
  onMount,
  paddingTop,
  showSuggestions = true,
  bubbleUpUndoRedo,
  disabled,
  beforeMount,
  options,
  tabFocusMode,
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
          { token: "keyword.python", foreground: cssCustomPropToHex(colorValues[styles.keywordPython]) },
          { token: "tag", foreground: cssCustomPropToHex(colorValues[styles.tag]) },
          { token: "comment", foreground: cssCustomPropToHex(colorValues[styles.comment]) },
          { token: NON_JINJA_TOKEN, foreground: cssCustomPropToHex(colorValues[styles.string]) },
          { token: JINJA_TOKEN, foreground: cssCustomPropToHex(colorValues[styles.jinja]) },
          { token: JINJA_STRING_TOKEN, foreground: cssCustomPropToHex(colorValues[styles.jinja]) },
          { token: JINJA_OTHER_TOKEN, foreground: cssCustomPropToHex(colorValues[styles.jinja]) },
          { token: JINJA_FIRST_BRACKET_TOKEN, foreground: cssCustomPropToHex(colorValues[styles.jinja]) },
          { token: JINJA_CLOSING_BRACKET_TOKEN, foreground: cssCustomPropToHex(colorValues[styles.jinja]) },
          { token: "keyword.gql", foreground: cssCustomPropToHex(colorValues[styles.keyword]) },
          { token: "key.identifier.gql", foreground: cssCustomPropToHex(colorValues[styles.type]) },
          { token: "number.gql", foreground: cssCustomPropToHex(colorValues[styles.number]) },
          { token: "delimiter.curly.gql", foreground: cssCustomPropToHex(colorValues[styles.delimiter]) },
          { token: "invalid.gql", foreground: cssCustomPropToHex(colorValues[styles.invalid]) },
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
      className={className}
      wrapperProps={{ name }}
      beforeMount={(monaco: Monaco) => {
        setAirbyteTheme(monaco);
        beforeMount?.(monaco);
      }}
      onMount={(editor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
        editor.addAction({
          id: "ctrl-z",
          label: "Undo (Ctrl + Z)",
          keybindings: [KeyMod.CtrlCmd | KeyCode.KeyZ],
          run: () => {
            bubbleUpUndoRedo ? bubbleUpUndoRedoEvent("undo", editor) : editor.trigger(undefined, "undo", undefined);
          },
        });

        editor.addAction({
          id: "ctrl-y",
          label: "Redo (Ctrl + Y)",
          keybindings: [KeyMod.CtrlCmd | KeyCode.KeyY],
          run: () => {
            bubbleUpUndoRedo ? bubbleUpUndoRedoEvent("redo", editor) : editor.trigger(undefined, "redo", undefined);
          },
        });

        editor.addAction({
          id: "ctrl-shift-z",
          label: "Redo (Ctrl + Shift + Z)",
          keybindings: [KeyMod.CtrlCmd | KeyMod.Shift | KeyCode.KeyZ],
          run: () => {
            bubbleUpUndoRedo ? bubbleUpUndoRedoEvent("redo", editor) : editor.trigger(undefined, "redo", undefined);
          },
        });

        editor.onDidBlurEditorWidget(() => {
          onBlur?.(editor.getValue());
        });

        // Triggering editor.action.toggleTabFocusMode is the only working way to maintain the behavior
        // of focusing the next element when pressing tab instead of inserting a tab character.
        // Since this is only a "toggle" command, and the state defaults to false, we keep track if its
        // state through a javascript variable, and toggle it accordingly when the editor is focused,
        // based on tabFocusMode prop.
        editor.onDidFocusEditorWidget(() => {
          if (tabFocusMode && !isTabFocusModeOn) {
            isTabFocusModeOn = true;
            editor.trigger("", "editor.action.toggleTabFocusMode", {});
          } else if (!tabFocusMode && isTabFocusModeOn) {
            isTabFocusModeOn = false;
            editor.trigger("", "editor.action.toggleTabFocusMode", {});
          }
        });

        onMount?.(editor, monaco);
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
        ...options,
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
