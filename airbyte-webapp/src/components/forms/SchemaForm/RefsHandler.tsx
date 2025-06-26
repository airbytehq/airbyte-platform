import isBoolean from "lodash/isBoolean";
import isEqual from "lodash/isEqual";
import React, { createContext, useCallback, useContext, useEffect, useState } from "react";
import { get, useFormContext } from "react-hook-form";
import { useEffectOnce } from "react-use";

import { useSchemaForm } from "./SchemaForm";
import { convertRefToPath } from "./utils";

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
  refTargetPath?: string;
  children: React.ReactNode;
}

// Create the provider component
export const RefsHandlerProvider: React.FC<RefsHandlerProviderProps> = ({ children, values, refTargetPath }) => {
  const { setValue, getValues, watch } = useFormContext();
  const [refTargetToSources, setRefTargetToSources] = useState<Map<string, string[]>>(new Map());
  const [refSourceToTarget, setRefSourceToTarget] = useState<Map<string, string>>(new Map());
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

  // Extract $ref mappings from the initial values
  // This should only run once when the form is first rendered
  useEffectOnce(() => {
    if (!refTargetPath) {
      return;
    }

    const mapping = new Map<string, string[]>();
    const reverseMapping = new Map<string, string>();

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
        const targetPath = convertRefToPath(objRecord.$ref);

        // Only process $refs that point within the refTargetPath
        // and have a value at the target path
        if (!targetPath.startsWith(refTargetPath) || !getValues(targetPath)) {
          return;
        }
        const refs = mapping.get(targetPath) || [];
        if (!refs.includes(path)) {
          refs.push(path);
          mapping.set(targetPath, refs);
        }
        reverseMapping.set(path, targetPath);
        return;
      }

      // Continue traversing
      for (const [key, value] of Object.entries(objRecord)) {
        const newPath = path ? `${path}.${key}` : key;
        extractRefs(value, newPath);
      }
    };

    extractRefs(values);
    setRefTargetToSources(mapping);
    setRefSourceToTarget(reverseMapping);
  });

  // REFERENCE MANAGEMENT

  // Add a reference from source to target
  const addRef = useCallback((sourcePath: string, targetPath: string) => {
    setRefTargetToSources((prev) => {
      const refs = prev.get(targetPath) || [];
      if (!refs.includes(sourcePath)) {
        refs.push(sourcePath);
        return new Map(prev).set(targetPath, refs);
      }
      return prev;
    });
    setRefSourceToTarget((prev) => new Map(prev).set(sourcePath, targetPath));
  }, []);

  // Remove a reference from source to target
  const removeRef = useCallback(
    (sourcePath: string, targetPath: string) => {
      // First check if this is the only source for this target
      const currentSources = refTargetToSources.get(targetPath) || [];
      const isLastReference = currentSources.length === 1 && currentSources[0] === sourcePath;
      const shouldClearTarget = refTargetPath && targetPath.startsWith(refTargetPath) && isLastReference;

      // Update references state
      setRefTargetToSources((prev) => {
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
      });

      setRefSourceToTarget((prev) => {
        const updatedMap = new Map(prev);
        if (prev.get(sourcePath) === targetPath) {
          updatedMap.delete(sourcePath);
        }
        return updatedMap;
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

  // Watch for changes and propagate to references and targets
  useEffect(() => {
    const subscription = watch((data, { name }) => {
      if (!name) {
        return;
      }

      // Prevent infinite loops
      const updatedPaths = new Set<string>([name]);

      // Helper to update field values safely
      const updateFieldValue = (pathToUpdate: string, newValue: unknown) => {
        if (updatedPaths.has(pathToUpdate)) {
          return;
        }

        const currentValue = get(data, pathToUpdate);
        if (!isEqual(currentValue, newValue)) {
          setValue(pathToUpdate, newValue, { shouldValidate: true });
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
    });

    return () => subscription.unsubscribe();
  }, [watch, setValue, findRefSourceAndTargetForPath, refTargetToSources]);

  // REFERENCE ACTIONS

  const getRefTargetPathForField = useCallback(
    (path: string) => {
      const pathParts = path.split(".");
      const fieldName = pathParts.at(-1) ?? undefined;
      const parentPath = pathParts.slice(0, -1).join(".");

      if (!fieldName || !refTargetPath) {
        return null;
      }

      const parentSchema = getSchemaAtPath(parentPath);

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

  return (
    <RefsHandlerContext.Provider
      value={{
        getReferenceInfo,
        getRefTargetPathForField,
        addRef,
        removeRef,
        handleLinkAction,
        handleUnlinkAction,
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
