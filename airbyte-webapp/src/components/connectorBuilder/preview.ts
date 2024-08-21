import cloneDeep from "lodash/cloneDeep";
import { useWatch, FieldValues, FieldPath, FieldPathValue, Control } from "react-hook-form";

import { BuilderState, useBuilderWatch } from "./types";

type UseWatchParameters<
  TFieldValues extends FieldValues = FieldValues,
  TFieldName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
> = [
  {
    name: TFieldName;
    defaultValue?: FieldPathValue<TFieldValues, TFieldName>;
    control?: Control<TFieldValues>;
    disabled?: boolean;
    exact?: boolean;
  },
];

type UseWatchReturnType<
  TFieldValues extends FieldValues = FieldValues,
  TFieldName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
> = FieldPathValue<TFieldValues, TFieldName>;

interface UseWatchWithPreviewOutput {
  fieldValue: UseWatchReturnType | undefined | null;
  isPreview: boolean;
}

const getPreviewPath = (regularPath: string): string | null => {
  return regularPath.startsWith("formValues.") ? regularPath.replace("formValues.", "previewValues.") : null;
};

const getPreviewWatch = (args: UseWatchParameters): UseWatchParameters => {
  const [arg] = args;

  if (typeof arg !== "object" || !("name" in arg) || typeof arg.name !== "string") {
    return [
      {
        name: "",
        disabled: true,
      },
    ];
  }

  const previewPath = getPreviewPath(arg.name);
  if (!previewPath) {
    return [
      {
        name: "",
        disabled: true,
      },
    ];
  }

  return [
    {
      ...cloneDeep(arg),
      name: previewPath,
      defaultValue: null,
    },
  ];
};

export const useWatchWithPreview = (...args: UseWatchParameters): UseWatchWithPreviewOutput => {
  const previewArgs = getPreviewWatch(args);
  const originalValue = useWatch(...args);
  const previewValue = useWatch(...previewArgs);

  if (!previewArgs[0]?.disabled && previewValue !== null) {
    return { fieldValue: previewValue, isPreview: true };
  }

  return { fieldValue: originalValue, isPreview: false };
};

export const useBuilderWatchWithPreview = <TPath extends FieldPath<BuilderState>>(
  path: TPath,
  options?: { exact: boolean }
): UseWatchWithPreviewOutput => {
  const previewPath = getPreviewPath(path);
  const originalValue = useBuilderWatch(path, options);
  const previewValue = useBuilderWatch((previewPath as TPath) || path, options);

  if (previewPath && previewValue) {
    return { fieldValue: previewValue, isPreview: true };
  }

  return { fieldValue: originalValue, isPreview: false };
};
