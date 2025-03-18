import { useMutation } from "@tanstack/react-query";

import {
  mockConfigTemplateAlsoFaker,
  mockConfigTemplateFakerOne,
  mockConfigTemplateList,
  mockTemplateForGDrive,
} from "test-utils/mock-data/mockConfigTemplates";

import { SCOPE_ORGANIZATION } from "../scopes";
import { ConfigTemplateList, ConfigTemplateRead, PartialUserConfigCreate } from "../types/AirbyteClient";
import { useSuspenseQuery } from "../useSuspenseQuery";

const configTemplates = {
  all: [SCOPE_ORGANIZATION, "configTemplates"] as const,
  lists: () => [...configTemplates.all, "list"],
  detail: (configTemplateId: string) => [...configTemplates.all, "details", configTemplateId] as const,
};

/**
 * these are all mocked versions of these hooks as the actual endpoints
 * are all no ops.
 *
 * they return the mocks imported above instead of real data
 */
export const useListConfigTemplates = (organizationId: string): ConfigTemplateList => {
  console.log(organizationId);
  return useSuspenseQuery(configTemplates.lists(), () => {
    return mockConfigTemplateList;
  });
};

export const useGetConfigTemplate = (configTemplateId: string) => {
  return useSuspenseQuery(configTemplates.detail(configTemplateId), (): ConfigTemplateRead => {
    if (configTemplateId === "1") {
      return mockConfigTemplateFakerOne;
    }
    if (configTemplateId === "2") {
      return mockConfigTemplateAlsoFaker;
    }
    if (configTemplateId === "3") {
      return mockTemplateForGDrive;
    }
    return mockConfigTemplateFakerOne;
  });
};

export const useCreatePartialUserConfig = () => {
  return useMutation(async (partialUserConfigCreate: PartialUserConfigCreate) => {
    console.log(partialUserConfigCreate);
    try {
      // Try to create source
      return alert(`${JSON.stringify(partialUserConfigCreate)}`);
    } catch (e) {
      throw e;
    }
  });
};
