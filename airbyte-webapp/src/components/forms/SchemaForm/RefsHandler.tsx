import cloneDeep from "lodash/cloneDeep";
import isBoolean from "lodash/isBoolean";
import isEqual from "lodash/isEqual";
import React, { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import { FieldValues, get, set, useFormContext } from "react-hook-form";

import { removeEmptyProperties } from "core/utils/form";

import { useSchemaForm } from "./SchemaForm";
import { convertPathToRef, convertRefToPath, resolveRefs } from "./utils";

// Types for reference management
export type ReferenceInfo =
  | {
      type: "source";
      target: string;
    }
  | {
      type: "target";
      sources: string[];
    }
  | {
      type: "none";
    };

// Types for menu actions
export interface LinkAction {
  action: "link";
  path: string;
}

export interface UnlinkAction {
  action: "unlink";
}

export type ReferenceAction = LinkAction | UnlinkAction;

// Result of finding compatible fields
export interface CompatibleFields {
  directFields: string[];
  refGroups: Map<string, string[]>;
  hasAnyOptions: boolean;
}

// Define the RefsHandler context type
export interface RefsHandlerContextValue {
  getReferenceInfo: (path: string) => ReferenceInfo;
  getRefTargetPathForField: (path: string) => string | null;
  addRef: (sourcePath: string, targetPath: string) => void;
  removeRef: (sourcePath: string, targetPath: string) => void;
  handleLinkAction: (path: string, overwriteTarget?: boolean) => void;
  handleUnlinkAction: (path: string) => void;
  exportValuesWithRefs: () => FieldValues;
  resetFormAndRefState: (values: FieldValues) => void;
}

// Create the context
const RefsHandlerContext = createContext<RefsHandlerContextValue | undefined>(undefined);

// Create the hook to use the context
export const useRefsHandler = () => {
  const context = useContext(RefsHandlerContext);
  if (!context) {
    throw new Error("useRefsHandler must be used within a RefsHandlerProvider");
  }
  return context;
};

// Provider props type
export interface RefsHandlerProviderProps {
  values: unknown;
  refBasePath?: string;
  refTargetPath?: string;
  children: React.ReactNode;
}

// Create the provider component
export const RefsHandlerProvider: React.FC<RefsHandlerProviderProps> = ({
  children,
  values,
  refBasePath,
  refTargetPath,
}) => {
  const { setValue, getValues, watch, reset } = useFormContext();
  const [{ refTargetToSources, refSourceToTarget }, setRefMappings] = useState<{
    refTargetToSources: Map<string, string[]>;
    refSourceToTarget: Map<string, string>;
  }>(extractRefMappings(values, refTargetPath, refBasePath));
  const { getSchemaAtPath } = useSchemaForm();

  // Get information about a reference at a path
  const getReferenceInfo = useCallback(
    (path: string): ReferenceInfo => {
      // Check if direct source
      if (refSourceToTarget.has(path)) {
        return {
          type: "source",
          target: refSourceToTarget.get(path)!,
        };
      }

      // Check if direct target
      if (refTargetToSources.has(path)) {
        return {
          type: "target",
          sources: refTargetToSources.get(path) || [],
        };
      }

      return { type: "none" };
    },
    [refSourceToTarget, refTargetToSources]
  );

  // REFERENCE MANAGEMENT

  // Add a reference from source to target
  const addRef = useCallback((sourcePath: string, targetPath: string) => {
    setRefMappings((previousMappings) => {
      const updateRefTargetToSources = (prev: Map<string, string[]>) => {
        const refs = prev.get(targetPath) || [];
        if (!refs.includes(sourcePath)) {
          refs.push(sourcePath);
          return new Map(prev).set(targetPath, refs);
        }
        return prev;
      };

      const updateRefSourceToTarget = (prev: Map<string, string>) => new Map(prev).set(sourcePath, targetPath);

      return {
        refTargetToSources: updateRefTargetToSources(previousMappings.refTargetToSources),
        refSourceToTarget: updateRefSourceToTarget(previousMappings.refSourceToTarget),
      };
    });
  }, []);

  // Remove a reference from source to target
  const removeRef = useCallback(
    (sourcePath: string, targetPath: string) => {
      // First check if this is the only source for this target
      const currentSources = refTargetToSources.get(targetPath) || [];
      const isLastReference = currentSources.length === 1 && currentSources[0] === sourcePath;
      const shouldClearTarget = refTargetPath && targetPath.startsWith(refTargetPath) && isLastReference;

      setRefMappings((previousMappings) => {
        const updateRefTargetToSources = (prev: Map<string, string[]>) => {
          const refs = prev.get(targetPath) || [];
          if (refs.includes(sourcePath)) {
            const updatedMap = new Map(prev);
            const updatedRefs = refs.filter((ref) => ref !== sourcePath);
            if (updatedRefs.length === 0) {
              updatedMap.delete(targetPath);
            } else {
              updatedMap.set(targetPath, updatedRefs);
            }
            return updatedMap;
          }
          return prev;
        };

        const updateRefSourceToTarget = (prev: Map<string, string>) => {
          const updatedMap = new Map(prev);
          if (prev.get(sourcePath) === targetPath) {
            updatedMap.delete(sourcePath);
          }
          return updatedMap;
        };

        return {
          refTargetToSources: updateRefTargetToSources(previousMappings.refTargetToSources),
          refSourceToTarget: updateRefSourceToTarget(previousMappings.refSourceToTarget),
        };
      });

      // After updating the ref state, process cleanup actions
      if (shouldClearTarget) {
        // Use setTimeout to ensure this happens after the state updates are processed
        setTimeout(() => {
          // First, clear the specific target path
          setValue(targetPath, undefined, { shouldValidate: true, shouldDirty: true, shouldTouch: true });
          // Then check if the refTargetPath is now empty, and clear it if so
          const refTargetPathValue = getValues(refTargetPath);
          if (Object.keys(refTargetPathValue).length === 0) {
            setValue(refTargetPath, undefined, { shouldValidate: true, shouldDirty: true, shouldTouch: true });
          }
        }, 0);
      }
    },
    [refTargetToSources, refTargetPath, setValue, getValues]
  );

  // Find if a path is within a reference source
  const findRefSourceAndTargetForPath = useCallback(
    (path: string): { sourcePath: string; targetPath: string } | null => {
      // Check if path itself is a reference
      if (refSourceToTarget.has(path)) {
        return { sourcePath: path, targetPath: refSourceToTarget.get(path)! };
      }

      // Check if path is within a reference
      const parentPath = getRefParentPath(path, refSourceToTarget);
      if (parentPath) {
        return { sourcePath: parentPath, targetPath: refSourceToTarget.get(parentPath)! };
      }

      return null;
    },
    [refSourceToTarget]
  );

  // REFERENCE SYNCHRONIZATION

  const previousValues = useRef(cloneDeep(getValues()));
  // Watch for changes and propagate to references and targets
  useEffect(() => {
    const subscription = watch((data, { name }) => {
      if (!name) {
        return;
      }

      // skip if the value did not change
      if (isEqual(get(previousValues.current, name), get(data, name))) {
        return;
      }

      // skip if name is not part of any reference mapping
      if (
        ![...refSourceToTarget.keys()].some((key: string) => key.startsWith(name) || name.startsWith(key)) &&
        ![...refTargetToSources.keys()].some((key) => name.startsWith(key))
      ) {
        return;
      }

      const newData = cloneDeep(data);

      // Prevent infinite loops
      const updatedPaths = new Set<string>([name]);

      const updateFieldValue = (pathToUpdate: string, newValue: unknown) => {
        if (updatedPaths.has(pathToUpdate)) {
          return;
        }

        const currentValue = get(data, pathToUpdate);
        if (!isEqual(currentValue, newValue)) {
          set(newData, pathToUpdate, newValue);
          updatedPaths.add(pathToUpdate);
        }
      };

      // Case 1: Target field changed - update all references
      for (const [targetPath, refPaths] of refTargetToSources.entries()) {
        if (name === targetPath || name.startsWith(`${targetPath}.`)) {
          const pathSuffix = name.slice(targetPath.length);
          const newValue = get(data, name);

          // Update all references with the new value
          refPaths.forEach((refPath) => {
            const pathToUpdate = pathSuffix ? `${refPath}${pathSuffix}` : refPath;
            updateFieldValue(pathToUpdate, newValue);
          });
        }
      }

      // Case 2: Reference source changed - update target and other sources
      const referenceInfo = findRefSourceAndTargetForPath(name);
      if (referenceInfo) {
        const { sourcePath, targetPath } = referenceInfo;
        const refSuffix = name.slice(sourcePath.length);
        const fieldTargetPath = refSuffix ? `${targetPath}${refSuffix}` : targetPath;
        const newValue = get(data, name);

        // if newValue is empty, remove the ref to avoid clearing out all other references
        if (newValue === undefined || newValue === null || newValue === "") {
          removeRef(sourcePath, targetPath);
        } else {
          // Update target
          updateFieldValue(fieldTargetPath, newValue);

          // Update other reference sources
          const refsToUpdate = refTargetToSources.get(targetPath) || [];
          refsToUpdate.forEach((otherRefPath) => {
            if (otherRefPath !== sourcePath) {
              const pathToUpdate = refSuffix ? `${otherRefPath}${refSuffix}` : otherRefPath;
              updateFieldValue(pathToUpdate, newValue);
            }
          });
        }
      }

      // Case 3: Parent of a source changed - remove the ref if no longer equal
      for (const [sourcePath, targetPath] of refSourceToTarget.entries()) {
        if (name !== sourcePath && sourcePath.startsWith(name)) {
          const sourceValue = getValues(sourcePath);
          const targetValue = getValues(targetPath);
          if (!isEqual(removeEmptyProperties(sourceValue), removeEmptyProperties(targetValue))) {
            removeRef(sourcePath, targetPath);
          }
        }
      }

      // Write updated data to form
      reset(newData, { keepDirty: true, keepErrors: true, keepTouched: true });

      previousValues.current = cloneDeep(newData);
    });

    return () => subscription.unsubscribe();
  }, [
    watch,
    setValue,
    findRefSourceAndTargetForPath,
    refTargetToSources,
    refSourceToTarget,
    removeRef,
    getValues,
    reset,
  ]);

  // REFERENCE ACTIONS

  const getRefTargetPathForField = useCallback(
    (path: string) => {
      const pathParts = path.split(".");
      const fieldName = pathParts.at(-1) ?? undefined;
      const parentPath = pathParts.slice(0, -1).join(".");

      if (!fieldName || !refTargetPath) {
        return null;
      }

      const parentSchema = getSchemaAtPath(parentPath, true);

      // ~ declarative_component_schema type handling ~
      if (
        parentSchema?.properties?.type &&
        !isBoolean(parentSchema.properties.type) &&
        parentSchema.properties.type.type === "string" &&
        parentSchema.properties.type.enum &&
        Array.isArray(parentSchema.properties.type.enum) &&
        parentSchema.properties.type.enum.length === 1
      ) {
        return `${refTargetPath}.${parentSchema.properties.type.enum[0]}.${fieldName}`;
      }

      // If there is no sibling type field, use the field name.
      // This has a higher chance of causing collisions, but it's the best we can do for now.
      return `${refTargetPath}.${fieldName}`;
    },
    [refTargetPath, getSchemaAtPath]
  );

  const handleLinkAction = useCallback(
    (path: string, overwriteTarget: boolean = false) => {
      const targetPath = getRefTargetPathForField(path);
      if (!targetPath) {
        return;
      }

      const targetValue = getValues(targetPath);
      const sourceValue = getValues(path);
      if ((targetValue && overwriteTarget) || !targetValue) {
        // Set the target path to the source value
        setValue(targetPath, sourceValue, { shouldValidate: true, shouldDirty: true, shouldTouch: true });
      } else {
        // Set the source path to the target value
        setValue(path, targetValue, { shouldValidate: true, shouldDirty: true, shouldTouch: true });
      }

      addRef(path, targetPath);
    },
    [getRefTargetPathForField, getValues, addRef, setValue]
  );

  const handleUnlinkAction = useCallback(
    (path: string) => {
      const refInfo = getReferenceInfo(path);

      if (refInfo.type === "source") {
        removeRef(path, refInfo.target);
      } else if (refInfo.type === "target") {
        refInfo.sources?.forEach((source) => removeRef(source, path));
      }
    },
    [getReferenceInfo, removeRef]
  );

  const exportValuesWithRefs = useCallback(() => {
    const values = cloneDeep(getValues());

    for (const [refSource, refTarget] of refSourceToTarget.entries()) {
      set(values, refSource, {
        $ref: convertPathToRef(refTarget, refBasePath),
      });
    }
    return removeEmptyProperties(values, true);
  }, [getValues, refBasePath, refSourceToTarget]);

  // Useful for when the values of the form need to be fully replaced.
  // This re-extracts the ref mappings and resets the form state to the new
  // values with refs pointing to refTargetPath resolved so that linking
  // behavior continues to work as expected.
  const resetFormAndRefState = useCallback(
    (values: FieldValues) => {
      setRefMappings(extractRefMappings(values, refTargetPath, refBasePath));
      reset(resolveRefs(values, values, refBasePath, refTargetPath));
    },
    [refTargetPath, refBasePath, reset]
  );

  return (
    <RefsHandlerContext.Provider
      value={{
        getReferenceInfo,
        getRefTargetPathForField,
        addRef,
        removeRef,
        handleLinkAction,
        handleUnlinkAction,
        exportValuesWithRefs,
        resetFormAndRefState,
      }}
    >
      {children}
    </RefsHandlerContext.Provider>
  );
};

// Split a path into its parts and return various forms
const getPathParts = (path: string) => {
  const parts = path.split(".");
  return {
    parts,
    parentPaths: parts.map((_, i) => parts.slice(0, i).join(".")).filter((p) => p !== ""),
  };
};

// Check if a field is within a reference source or target
const getRefParentPath = (path: string, pathMap: Map<string, unknown>): string | null => {
  const { parentPaths } = getPathParts(path);

  for (const parentPath of parentPaths) {
    if (pathMap.has(parentPath)) {
      return parentPath;
    }
  }

  return null;
};

const extractRefMappings = (values: unknown, refTargetPath?: string, refBasePath?: string) => {
  if (!refTargetPath) {
    return { refTargetToSources: new Map(), refSourceToTarget: new Map() };
  }

  const refTargetToSources = new Map<string, string[]>();
  const refSourceToTarget = new Map<string, string>();

  // Function to extract $refs from an object
  const extractRefs = (obj: unknown, path = "") => {
    if (!obj || typeof obj !== "object") {
      return;
    }

    if (Array.isArray(obj)) {
      obj.forEach((item, index) => {
        extractRefs(item, path ? `${path}.${index}` : `${index}`);
      });
      return;
    }

    // Non-array object
    const objRecord = obj as Record<string, unknown>;

    if ("$ref" in objRecord && typeof objRecord.$ref === "string") {
      const targetPath = convertRefToPath(objRecord.$ref, refBasePath);
      if (!targetPath.startsWith(refTargetPath)) {
        return;
      }
      const refs = refTargetToSources.get(targetPath) || [];
      if (!refs.includes(path)) {
        refs.push(path);
        refTargetToSources.set(targetPath, refs);
      }
      refSourceToTarget.set(path, targetPath);
      return;
    }

    // Continue traversing
    for (const [key, value] of Object.entries(objRecord)) {
      const newPath = path ? `${path}.${key}` : key;
      extractRefs(value, newPath);
    }
  };

  extractRefs(values);
  return { refTargetToSources, refSourceToTarget };
};
