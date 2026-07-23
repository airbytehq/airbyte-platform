import cloneDeep from "lodash/cloneDeep";
import {
  useWatch,
  FieldValues,
  FieldPath,
  FieldPathValue,
  Control,
  useFieldArray,
  UseFieldArrayReturn,
} from "react-hook-form";

import { BuilderState } from "./types";

// ============ NOTE TO DEVELOPER ============
// If you have this file open in your editor, it may cause slowness due to the large type unions being used.
// The `useBuilderWatch` hook variants have been refactored to NOT cause slowness in the files that use them,
// as long as strings are passed into the `path` parameter.

// If you notice slowness in your editor, try closing this file and make sure you are only passing strings to
// the `path` parameter of these hooks.
// ============ END DEVELOPER NOTE ===========

// Converts union types (e.g. "mode" | "view") to intersections (e.g. "mode" & "view")
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (k: infer I) => void ? I : never;
//     e.g.      "mode" | "view"  -->  (k: "mode") => void | (k: "view") => void  -->  "mode" & "view"

// Enforces that T is NOT a union type, returning `T` if it is not, and `never` if it is.
// It does this by first trying to convert T from a union to an intersection type.
// If the resulting type is still the same as T, that means T is not a union.
type EnforceNotUnion<T> = [T] extends [UnionToIntersection<T>] ? T : never;

export const useBuilderWatch: {
  // First overload: accepts only non-union literal strings in FieldPath<BuilderState>, e.g. "formValues.global.urlBase"
  // If a union (e.g. FieldPath<BuilderState>) is passed in, EnforceNotUnion evaluates to `never`, resulting in falling
  // back to the second overload, which sidesteps the huge union type expansion that slows down typescript intellisense.
  <TPath extends FieldPath<BuilderState>>(path: EnforceNotUnion<TPath>): FieldPathValue<BuilderState, TPath>;

  // Second overload: accepts any string or FieldPath<BuilderState> typed param, and returns unknown
  (path: string): unknown;
} = <TPath extends FieldPath<BuilderState>>(path: TPath) => {
  return useWatch<BuilderState, TPath>({
    name: path,
  });
};

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

export const useBuilderWatchWithPreview: {
  // First overload: accepts only non-union literal strings in FieldPath<BuilderState>, e.g. "formValues.global.urlBase"
  // If a union (e.g. FieldPath<BuilderState>) is passed in, EnforceNotUnion evaluates to `never`, resulting in falling
  // back to the second overload, which sidesteps the huge union type expansion that slows down typescript intellisense.
  <TPath extends FieldPath<BuilderState>>(
    path: EnforceNotUnion<TPath>
  ): { fieldValue: FieldPathValue<BuilderState, TPath>; isPreview: boolean };

  // Second overload: accepts any string or FieldPath<BuilderState> typed param and returns unknown for the fieldValue
  (path: string): { fieldValue: unknown; isPreview: boolean };
} = <TPath extends FieldPath<BuilderState>>(path: TPath) => {
  const previewPath = getPreviewPath(path);
  const originalValue = useBuilderWatch(path);
  const previewValue = useBuilderWatch((previewPath as TPath) || path);

  if (previewPath && previewValue) {
    return { fieldValue: previewValue, isPreview: true };
  }

  return { fieldValue: originalValue, isPreview: false };
};

export const useBuilderWatchArrayWithPreview = <TPath extends FieldPath<BuilderState>>(
  path: TPath
): UseWatchWithPreviewOutput & Omit<UseFieldArrayReturn<FieldValues, TPath, "id">, "fields"> => {
  const previewPath = getPreviewPath(path);
  const { fields: originalValue, ...rest } = useFieldArray({ name: path });
  const previewValue = useBuilderWatch((previewPath as TPath) || path);

  if (previewPath && previewValue && Array.isArray(previewValue)) {
    return {
      // make compatible with format of useFieldArray
      fieldValue: previewValue.map((field: string[], index: number) => ({
        ...Object.entries(field).map(([key, value]) => ({ [key]: value })),
        id: index,
      })),
      isPreview: true,
      ...rest,
    };
  }

  return { fieldValue: originalValue, isPreview: false, ...rest };
};
