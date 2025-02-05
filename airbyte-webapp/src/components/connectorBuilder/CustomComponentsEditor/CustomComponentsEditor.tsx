import { useFormContext } from "react-hook-form";

import { CodeEditor } from "components/ui/CodeEditor";

import { useBuilderWatch } from "../useBuilderWatch";

export const CustomComponentsEditor = () => {
  const { setValue } = useFormContext();
  const customComponentsCodeValue = useBuilderWatch("customComponentsCode");

  return (
    <CodeEditor
      language="python"
      value={customComponentsCodeValue || ""}
      onChange={(value: string | undefined) => {
        setValue("customComponentsCode", value || undefined);
      }}
      lineNumberCharacterWidth={6}
      paddingTop
    />
  );
};
