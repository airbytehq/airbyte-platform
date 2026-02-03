import React, { PropsWithChildren, createContext, useCallback, useContext, useEffect, useState } from "react";
import { v4 as uuidv4 } from "uuid";

import { useCurrentConnection, useValidateMappers } from "core/api";
import {
  AirbyteStream,
  HashingMapperConfigurationMethod,
  StreamDescriptor,
  StreamMapperType,
} from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useNotificationService } from "core/services/Notification";

import { StreamMapperWithId } from "./types";
import { useGetMappingsForCurrentConnection } from "./useGetMappingsForCurrentConnection";
import { useUpdateMappingsForCurrentConnection } from "./useUpdateMappingsForCurrentConnection";

interface MappingContextType {
  streamsWithMappings: Record<string, StreamMapperWithId[]>;
  updateLocalMapping: (
    streamDescriptorKey: string,
    mappingId: string,
    updatedMapping: Partial<StreamMapperWithId>,
    skipDirty?: boolean
  ) => void;
  reorderMappings: (streamDescriptorKey: string, newOrder: StreamMapperWithId[]) => void;
  clear: () => void;
  submitMappings: () => Promise<void>;
  removeMapping: (streamDescriptorKey: string, mappingId: string) => void;
  addStreamToMappingsList: (streamDescriptorKey: string) => void;
  addMappingForStream: (streamDescriptorKey: string) => void;
  validateMappings: (streamDescriptorKey: string) => void;
  validatingStreams: Set<string>;
  key: number;
  hasMappingsChanged: boolean;
  isMappingsFeatureEnabled: boolean;
}

export const MAPPING_VALIDATION_ERROR_KEY = "mapping-validation-error";

const MappingContext = createContext<MappingContextType | undefined>(undefined);

export const getKeyForStream = (stream: AirbyteStream) => `${stream.namespace ?? "null"}:::${stream.name}`;

export const getStreamDescriptorForKey = (key: string): StreamDescriptor => {
  const [namespace, name] = key.split(":::");

  return {
    namespace: namespace === "null" ? undefined : namespace,
    name,
  };
};

export const MappingContextProvider: React.FC<PropsWithChildren> = ({ children }) => {
  const connection = useCurrentConnection();
  const savedStreamsWithMappings = useGetMappingsForCurrentConnection();
  const { fetchQuery: validateMappingsFunc } = useValidateMappers();
  const { updateMappings } = useUpdateMappingsForCurrentConnection(connection.connectionId);
  const [streamsWithMappings, setStreamsWithMappings] = useState(savedStreamsWithMappings);
  const [validatingStreams, setValidatingStreams] = useState<Set<string>>(new Set());
  const [hasMappingsChanged, setHasMappingsChanged] = useState(false);
  const isMappingsFeatureEnabled = useFeature(FeatureItem.MappingsUI);

  // Key is used to force mapping forms to re-render if a user chooses to reset the form state
  const [key, setKey] = useState(1);
  const { unregisterNotificationById } = useNotificationService();

  const validateMappings = useCallback(
    async (streamDescriptorKey: string) => {
      const streamDescriptor = getStreamDescriptorForKey(streamDescriptorKey);

      const validationResults = await validateMappingsFunc(streamDescriptor, streamsWithMappings[streamDescriptorKey]);

      setValidatingStreams((prev) => {
        if (!prev.has(streamDescriptorKey)) {
          return prev;
        }
        const newSet = new Set(prev);
        newSet.delete(streamDescriptorKey);
        return newSet;
      });

      setStreamsWithMappings((prevMappings) => ({
        ...prevMappings,
        [streamDescriptorKey]: prevMappings[streamDescriptorKey].map((mapping) => ({
          ...mapping,
          validationError: validationResults.mappers.find((validationResult) => validationResult.id === mapping.id)
            ?.validationError,
        })),
      }));
    },
    [streamsWithMappings, validateMappingsFunc]
  );

  useEffect(() => {
    if (validatingStreams.size > 0) {
      validatingStreams.forEach((streamDescriptorKey) => validateMappings(streamDescriptorKey));
    }
  }, [validateMappings, validatingStreams]);

  // Updates a specific mapping in the local state
  const updateLocalMapping = useCallback(
    (
      streamDescriptorKey: string,
      mappingId: string,
      updatedMapping: Partial<StreamMapperWithId>,
      skipDirty?: boolean
    ) => {
      setStreamsWithMappings((prevMappings) => ({
        ...prevMappings,
        [streamDescriptorKey]: prevMappings[streamDescriptorKey].map((mapping) => {
          if (mapping.id === mappingId) {
            if (updatedMapping.type && updatedMapping.type !== mapping.type) {
              return updatedMapping as StreamMapperWithId;
            }
            return { ...mapping, ...updatedMapping };
          }
          return mapping;
        }),
      }));

      if (skipDirty !== true) {
        setHasMappingsChanged(true);
      }
      setValidatingStreams((prev) => new Set(prev).add(streamDescriptorKey));
    },
    []
  );

  const addMappingForStream = useCallback((streamDescriptorKey: string) => {
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamDescriptorKey]: [
        ...prevMappings[streamDescriptorKey],
        {
          type: StreamMapperType.hashing,
          id: uuidv4(),
          validationCallback: () => Promise.reject(false),
          mapperConfiguration: {
            fieldNameSuffix: "_hashed",
            method: HashingMapperConfigurationMethod["SHA-256"],
            targetField: "",
          },
        },
      ],
    }));
    setHasMappingsChanged(true);
  }, []);

  // Reorders the mappings for a specific stream
  const reorderMappings = useCallback((streamDescriptorKey: string, newOrder: StreamMapperWithId[]) => {
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamDescriptorKey]: newOrder,
    }));
    setHasMappingsChanged(true);
    setValidatingStreams((prev) => new Set(prev).add(streamDescriptorKey));
  }, []);

  // Clears the mappings back to the saved state
  const clear = () => {
    setKey((prevKey) => prevKey + 1);
    setHasMappingsChanged(false);
    setStreamsWithMappings(savedStreamsWithMappings);
    unregisterNotificationById(MAPPING_VALIDATION_ERROR_KEY);
  };

  const removeMapping = (streamDescriptorKey: string, mappingId: string) => {
    const mappingsForStream = streamsWithMappings[streamDescriptorKey].filter((mapping) => mapping.id !== mappingId);

    setStreamsWithMappings((prevMappings) => {
      return {
        ...prevMappings,
        [streamDescriptorKey]: mappingsForStream,
      };
    });
    setHasMappingsChanged(true);
    setValidatingStreams((prev) => new Set(prev).add(streamDescriptorKey));
  };

  // Submits the current mappings state to the backend
  const submitMappings = async () => {
    const result = await updateMappings(streamsWithMappings);
    if (result.success === true) {
      setHasMappingsChanged(false);
    }
    return Promise.resolve();
  };

  const addStreamToMappingsList = (streamDescriptorKey: string) => {
    const newMapping: Record<string, StreamMapperWithId[]> = {
      [streamDescriptorKey]: [
        {
          type: StreamMapperType.hashing,
          id: uuidv4(),
          validationCallback: () => Promise.reject(false),
          mapperConfiguration: {
            fieldNameSuffix: "_hashed",
            method: HashingMapperConfigurationMethod["SHA-256"],
            targetField: "",
          },
        },
      ],
    };

    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      ...newMapping,
    }));
    setHasMappingsChanged(true);
  };

  return (
    <MappingContext.Provider
      value={{
        streamsWithMappings,
        updateLocalMapping,
        reorderMappings,
        clear,
        submitMappings,
        removeMapping,
        addStreamToMappingsList,
        addMappingForStream,
        validateMappings,
        validatingStreams,
        key,
        hasMappingsChanged,
        isMappingsFeatureEnabled,
      }}
    >
      {children}
    </MappingContext.Provider>
  );
};

export const useMappingContext = () => {
  const context = useContext(MappingContext);
  if (!context) {
    throw new Error("useMappingContext must be used within a MappingContextProvider");
  }
  return context;
};
