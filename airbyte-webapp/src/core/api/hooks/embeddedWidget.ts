import { useMutation } from "@tanstack/react-query";

import {
  mockActorTemplateAlsoFaker,
  mockActorTemplateFakerOneWithStreams,
  mockActorTemplateList,
  mockTemplateForGDrive,
} from "test-utils/mock-data/mockActorTemplate";

import { SCOPE_ORGANIZATION } from "../scopes";
import { ActorMaskCreateRequestBody, ActorTemplateList, ActorTemplateRead } from "../types/AirbyteClient";
import { useSuspenseQuery } from "../useSuspenseQuery";

const actorTemplates = {
  all: [SCOPE_ORGANIZATION, "actorConnectionTemplates"] as const,
  lists: () => [...actorTemplates.all, "list"],
  detail: (actorConnectionTemplateId: string) => [...actorTemplates.all, "details", actorConnectionTemplateId] as const,
};

/**
 * these are all mocked versions of these hooks as the actual endpoints
 * are all no ops.
 *
 * they return the mocks imported above instead of real data
 */
export const useListActorTemplates = (organizationId: string): ActorTemplateList => {
  console.log(organizationId);
  return useSuspenseQuery(actorTemplates.lists(), () => {
    return mockActorTemplateList;
  });
};

export const useGetActorConnectionTemplate = (actorConnectionTemplateId: string) => {
  return useSuspenseQuery(actorTemplates.detail(actorConnectionTemplateId), (): ActorTemplateRead => {
    if (actorConnectionTemplateId === "1") {
      return mockActorTemplateFakerOneWithStreams;
    }
    if (actorConnectionTemplateId === "2") {
      return mockActorTemplateAlsoFaker;
    }
    if (actorConnectionTemplateId === "3") {
      return mockTemplateForGDrive;
    }
    return mockActorTemplateFakerOneWithStreams;
  });
};

export const useCreateActorConnectionMask = () => {
  return useMutation(async (actorConnectionMaskCreate: ActorMaskCreateRequestBody) => {
    console.log(actorConnectionMaskCreate);
    try {
      // Try to create source
      return alert(`${JSON.stringify(actorConnectionMaskCreate)}`);
    } catch (e) {
      throw e;
    }
  });
};
