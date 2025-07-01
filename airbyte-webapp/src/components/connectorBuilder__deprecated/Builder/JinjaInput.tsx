import { Monaco } from "@monaco-editor/react";
import classNames from "classnames";
import isString from "lodash/isString";
import { IRange, languages, Range, editor, Position } from "monaco-editor";
import React, { useCallback, useState } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { CodeEditor } from "components/ui/CodeEditor";

import { useConnectorBuilderFormManagementState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { JINJA_TOKEN, NON_JINJA_TOKEN, conf, language } from "./jinja";
import styles from "./JinjaInput.module.scss";
import { getInterpolationValues, getInterpolationVariablesByManifest, InterpolationValue } from "./manifestHelpers";
import { BuilderFormInput } from "../types";
import { formatJson } from "../utils";

const ADD_NEW_USER_INPUT_COMMAND = "addNewUserInput";
const HIDDEN_INTERPOLATION_VALUES = ["parameters", "stream_slice"];
const interpolationValues = getInterpolationValues();

let isSuggestionDetailShown = false;

interface JinjaInputProps {
  name: string;
  value: string;
  onChange: (value: string | undefined) => void;
  onBlur: (value: string) => void;
  disabled?: boolean;
  readOnly?: boolean;
  error?: boolean;
  manifestPath?: string;
  bubbleUpUndoRedo?: boolean;
}

type SuggestableValue =
  | {
      suggestionType: "userInput";
      input: BuilderFormInput;
    }
  | {
      suggestionType: "interpolation";
      value: InterpolationValue;
    }
  | {
      suggestionType: "newUserInput";
    };

export const JinjaInput: React.FC<JinjaInputProps> = ({
  name,
  value,
  onChange,
  onBlur,
  disabled,
  readOnly,
  error,
  manifestPath,
  bubbleUpUndoRedo = true,
}) => {
  const { formatMessage } = useIntl();
  const { getValues } = useFormContext();
  const { setNewUserInputContext } = useConnectorBuilderFormManagementState();
  const [focused, setFocused] = useState(false);

  const formatExamples = useCallback(
    (examples: string[] | object[]): string => {
      if (!examples || examples.length === 0) {
        return "";
      }
      const title = formatMessage({ id: "jinjaInput.suggest.examples" }, { count: examples.length });
      const content = examples
        .map((example) => {
          return `<pre>${isString(example) ? example : formatJson(example)}</pre>`;
        })
        .join("\n");
      return `#### ${title}\n${content}`;
    },
    [formatMessage]
  );

  const buildDocumentation = useCallback(
    (value: InterpolationValue, label: string) => {
      return `### ${label}\n${value.description}\n${formatExamples(value.examples)}`;
    },
    [formatExamples]
  );

  const convertInterpolationValueToCompletionItem = useCallback(
    (value: InterpolationValue, range: IRange, index: number): languages.CompletionItem => {
      const [descriptionFirstSentence] = value.description.split(".");

      const { label, hasArguments } = buildLabel(value);

      return {
        insertText:
          value.type === "variable"
            ? `${value.title}['\${0}']`
            : value.type === "macro"
            ? hasArguments
              ? `${value.title}(\${0})`
              : `${value.title}()`
            : hasArguments
            ? `| ${value.title}(\${0})`
            : `| ${value.title}`,
        insertTextRules: languages.CompletionItemInsertTextRule.InsertAsSnippet,
        kind:
          value.type === "variable"
            ? languages.CompletionItemKind.Field
            : value.type === "macro"
            ? languages.CompletionItemKind.Function
            : languages.CompletionItemKind.Variable,
        label: {
          label,
          description: descriptionFirstSentence,
        },
        documentation: {
          value: buildDocumentation(value, label),
          isTrusted: true,
          supportHtml: true,
        },
        // enforce custom sorting based on index, which only works with letters
        sortText: numberToLetter(index),
        range,
      };
    },
    [buildDocumentation]
  );

  const convertUserInputToCompletionItem = useCallback(
    (userInput: BuilderFormInput, range: IRange, index: number): languages.CompletionItem => {
      const label = userInput.definition.title || userInput.key;
      const testingValue = getValues("testingValues")?.[userInput.key];
      const formattedTestingValue = isString(testingValue) ? testingValue : formatJson(testingValue);

      return {
        insertText: `config['${userInput.key}']`,
        insertTextRules: languages.CompletionItemInsertTextRule.InsertAsSnippet,
        kind: languages.CompletionItemKind.User,
        label: {
          label,
          description: formattedTestingValue,
        },
        documentation: {
          value: `### ${label}\n${formatMessage(
            { id: "jinjaInput.suggest.userInput.description" },
            { label }
          )}\n#### ${formatMessage({
            id: "jinjaInput.suggest.userInput.currentTestingValue",
          })}\n<pre>${formattedTestingValue}</pre>`,
          isTrusted: true,
          supportHtml: true,
        },
        // enforce custom sorting based on index, which only works with letters
        sortText: numberToLetter(index),
        range,
      };
    },
    [formatMessage, getValues]
  );

  const getSuggestions = useCallback(
    (
      model: editor.ITextModel,
      position: Position,
      monaco: Monaco,
      localManifestPath: string | undefined
    ): languages.CompletionItem[] => {
      // Get the non-whitespace text before and after the cursor
      const word = model.getWordAtPosition(position);
      const range: IRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word?.startColumn ?? position.column,
        endColumn: word?.endColumn ?? position.column,
      };

      const cursorToken = getTokenAtPosition(monaco, model, position);
      if (cursorToken?.type !== JINJA_TOKEN) {
        return [];
      }

      const supportedVariables = localManifestPath ? getInterpolationVariablesByManifest(localManifestPath) : undefined;
      const validInterpolationValues = interpolationValues
        .filter((value) => !HIDDEN_INTERPOLATION_VALUES.includes(value.title))
        .filter(
          (value) => value.type !== "variable" || !supportedVariables || supportedVariables.includes(value.title)
        );

      const userInputs: BuilderFormInput[] = getValues("formValues.inputs");
      const suggestableNewUserInput: SuggestableValue[] = [{ suggestionType: "newUserInput" as const }];
      const suggestableValues: SuggestableValue[] = suggestableNewUserInput
        .concat(
          userInputs.map((input) => ({
            input,
            suggestionType: "userInput" as const,
          }))
        )
        .concat(
          validInterpolationValues.map((value) => {
            return { value, suggestionType: "interpolation" as const };
          })
        );
      const suggestionsWithTitles: Array<{ suggestion: languages.CompletionItem; titleFromManifestSchema: string }> =
        suggestableValues.map((value, index) => {
          if (value.suggestionType === "userInput") {
            return {
              suggestion: convertUserInputToCompletionItem(value.input, range, index),
              titleFromManifestSchema: value.input.definition.title || value.input.key,
            };
          } else if (value.suggestionType === "interpolation") {
            return {
              suggestion: convertInterpolationValueToCompletionItem(value.value, range, index),
              titleFromManifestSchema: value.value.title,
            };
          }
          return {
            suggestion: {
              insertText: "",
              kind: languages.CompletionItemKind.User,
              label: formatMessage({ id: "jinjaInput.suggest.userInput.createNew.label" }),
              documentation: {
                value: formatMessage({ id: "jinjaInput.suggest.userInput.createNew.doc" }),
              },
              range,
              // enforce custom sorting based on index, which only works with letters
              sortText: numberToLetter(index),
              command: {
                id: ADD_NEW_USER_INPUT_COMMAND,
                title: "",
                arguments: [model, position],
              },
            },
            titleFromManifestSchema: "",
          };
        });

      return (
        word
          ? suggestionsWithTitles.filter(({ titleFromManifestSchema, suggestion }) => {
              const label = isString(suggestion.label) ? suggestion.label : suggestion.label.label;
              return (
                (label.startsWith(word.word) || titleFromManifestSchema.startsWith(word.word)) &&
                label !== word.word &&
                titleFromManifestSchema !== word.word
              );
            })
          : suggestionsWithTitles
      ).map(({ suggestion }) => suggestion);
    },
    [convertInterpolationValueToCompletionItem, convertUserInputToCompletionItem, formatMessage, getValues]
  );

  return (
    <CodeEditor
      className={classNames(styles.editor, styles.container, {
        [styles.disabled]: disabled || readOnly,
        [styles.error]: error,
        [styles.focused]: focused,
      })}
      name={name}
      value={value}
      onChange={onChange}
      onBlur={onBlur}
      disabled={disabled ?? false}
      readOnly={readOnly ?? false}
      beforeMount={(monaco) => {
        if (monaco.languages.getLanguages().find((lang) => lang.id === "jinja")) {
          return;
        }
        monaco.languages.register({ id: "jinja" });
        monaco.languages.registerCompletionItemProvider("jinja", {
          provideCompletionItems: (model, position) => {
            // See ! HACK ! comment below for explanation
            // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unnecessary-type-assertion
            return { suggestions: getSuggestions(model, position, monaco, (model as any).manifestPath) };
          },
        });
        monaco.languages.registerHoverProvider("jinja", {
          provideHover: (model, position) => {
            const token = getTokenAtPosition(monaco, model, position);
            if (token?.type !== JINJA_TOKEN) {
              return { contents: [] };
            }
            const word = model.getWordAtPosition(position);
            const value = interpolationValues.find(
              (value) => value.title === word?.word || buildLabel(value).label === word?.word
            );
            if (!value) {
              return { contents: [] };
            }
            return {
              contents: [
                { value: buildDocumentation(value, buildLabel(value).label), isTrusted: true, supportHtml: true },
              ],
            };
          },
        });
        monaco.languages.setLanguageConfiguration("jinja", conf);
        monaco.languages.setMonarchTokensProvider("jinja", language);

        monaco.editor.registerCommand(
          ADD_NEW_USER_INPUT_COMMAND,
          (_accessor, model: editor.ITextModel, position: Position) => {
            setNewUserInputContext({ model, position });
          }
        );
      }}
      onMount={(editor, monaco) => {
        const model = editor.getModel();
        // ! HACK ! - this attaches the manifestPath to the model, so that the provideCompletionItems call
        // above pulls the value from the model.
        // This is needed because the provideCompletionItems function is set on the language, not individual
        // editor instances, so it can't change from one instance to the next. Therefore the only way to have
        // it produce different results is to shove the unique value into the model itself.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (model as any).manifestPath = manifestPath;

        // Prevent newlines
        editor.onDidChangeModelContent(() => {
          const value = editor.getValue();
          if (value.includes("\n")) {
            editor.setValue(value.replace(/\n/g, "")); // Prevent newlines
          }
        });

        editor.onDidChangeCursorSelection((e) => {
          const model = editor.getModel();
          if (!model) {
            return;
          }
          const position = e.selection.getPosition();

          // Check suggestions length, because we don't want to trigger the suggest widget
          // when there are none, as then it would show "No suggestions"
          const suggestions = getSuggestions(model, position, monaco, manifestPath);
          if (suggestions.length > 0) {
            editor.trigger("cursorChange", "editor.action.triggerSuggest", {});
            // In order to have suggestion details shown by default, we need to manually trigger the
            // "toggleSuggestionDetails" command. But, this command can only be triggered if the suggest
            // widget is open, so we need to wait for the next tick to trigger it.
            // Since we only have a "toggle" command and not a "show" command, we keep track of the state
            // of this setting in a javascript variable, and only trigger it if it's not already shown,
            // so that it is only triggered one time and left on.
            setTimeout(() => {
              if (!isSuggestionDetailShown) {
                editor.trigger("cursorChange", "toggleSuggestionDetails", {});
                isSuggestionDetailShown = true;
              }
            }, 0);
          } else {
            editor.trigger("editor", "hideSuggestWidget", {});
          }
        });

        editor.onDidFocusEditorWidget(() => {
          setFocused(true);
        });

        editor.onDidBlurEditorWidget(() => {
          setFocused(false);
        });

        editor.addAction({
          id: "insertBracketsLeft",
          label: "Insert Brackets (Left)",
          keybindings: [monaco.KeyMod.Shift | monaco.KeyCode.BracketLeft],
          run: async () => {
            insertBrackets("{", editor, monaco);
          },
        });

        editor.addAction({
          id: "insertBracketsRight",
          label: "Insert Brackets (Right)",
          keybindings: [monaco.KeyMod.Shift | monaco.KeyCode.BracketRight],
          run: async () => {
            insertBrackets("}", editor, monaco);
          },
        });
      }}
      height="35px"
      options={{
        cursorStyle: "line-thin",
        lineNumbers: "off",
        suggest: {
          showWords: false,
          snippetsPreventQuickSuggestions: true,
          preview: true,
        },
        renderLineHighlight: "none",
        scrollbar: {
          vertical: "hidden",
          horizontal: "hidden",
        },
        overviewRulerLanes: 0,
        folding: false,
        lineDecorationsWidth: 8,
        padding: {
          top: 5,
        },
        fontFamily: styles.fontFamily,
        fontSize: 14,
        scrollBeyondLastLine: false,
        fixedOverflowWidgets: true,
      }}
      language="jinja"
      bubbleUpUndoRedo={bubbleUpUndoRedo}
      tabFocusMode
    />
  );
};

const numberToLetter = (number: number): string => {
  return String.fromCharCode("a".charCodeAt(0) + number);
};

const buildLabel = (value: InterpolationValue): { label: string; hasArguments: boolean } => {
  const hasArguments = (value.type === "macro" || value.type === "filter") && Object.keys(value.arguments).length > 0;
  const label = value.type === "macro" || hasArguments ? `${value.title}()` : value.title;
  if (value.type === "filter") {
    return { label: `| ${label}`, hasArguments };
  }
  return { label, hasArguments };
};

const getTokenAtPosition = (monaco: Monaco, model: editor.ITextModel, position: Position) => {
  const currentLine = model.getLineContent(position.lineNumber);
  const tokens = monaco.editor.tokenize(currentLine, "jinja")[0];
  const currentColumn = position.column - 1; // Adjust column to zero-based index
  let currentTokenIndex = 0;
  while (currentTokenIndex < tokens.length - 1 && tokens[currentTokenIndex + 1].offset < currentColumn) {
    currentTokenIndex += 1;
  }

  const token = tokens[currentTokenIndex];
  if (!token) {
    return null;
  }
  return token;
};

const insertBrackets = (pressedKey: "}" | "{", editor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
  const position = editor.getPosition();
  if (!position) {
    return;
  }

  const model = editor.getModel();
  if (!model) {
    return;
  }

  // if inside a jinja expression, just insert the character as normal
  const cursorToken = getTokenAtPosition(monaco, model, position);
  if (cursorToken && cursorToken.type !== NON_JINJA_TOKEN) {
    editor.executeEdits(null, [
      {
        range: new Range(position.lineNumber, position.column, position.lineNumber, position.column),
        text: pressedKey,
        forceMoveMarkers: true,
      },
    ]);
    return;
  }

  // Insert `{{  }}`
  editor.executeEdits(null, [
    {
      range: new Range(position.lineNumber, position.column, position.lineNumber, position.column),
      text: "{{  }}",
      forceMoveMarkers: true,
    },
  ]);

  // Move the cursor to between the braces
  editor.setPosition({
    lineNumber: position.lineNumber,
    column: position.column + 3,
  });

  // Show the suggestions widget
  editor.trigger("cursorChange", "editor.action.triggerSuggest", {});
};
