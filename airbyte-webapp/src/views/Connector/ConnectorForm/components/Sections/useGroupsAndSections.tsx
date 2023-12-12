import get from "lodash/get";
import isEqual from "lodash/isEqual";
import { useMemo } from "react";
import { useFormContext } from "react-hook-form";

import { FormBlock, GroupDetails } from "core/form/types";
import { naturalComparator } from "core/utils/objects";

import { useAuthentication } from "../../useAuthentication";
import { OrderComparator } from "../../utils";

export interface Section {
  blocks: FormBlock[];
  displayType: DisplayType;
  hasError?: boolean;
  initiallyOpen?: boolean;
}

interface SectionGroup {
  sections: Section[];
  title?: string;
}

interface BlockGroup {
  blocks: FormBlock[];
  title?: string;
}

export type DisplayType = "expanded" | "collapsed-inline" | "collapsed-footer" | "collapsed-group";

/**
 * Takes form blocks and splits them into groups with each group containing potentially multiple sections.
 * The grouping is only done in case `rootLevel` is true, whereas the sections are always generated.
 *
 * Algorithm for determining groups (for root level form blocks):
 * * Group form blocks by the `group` attribute (undefined is treated as a separate group)
 * * Groups are ordered first by the index of their id in the groupStructure, then by regular string sorting on group id
 * * For each group, execute the section algorithm (see below)
 *
 * Section algorithm:
 * * Order form blocks by the following rules:
 *   * Order required form blocks in front of optional form blocks
 *   * Within required and optional sections, if `order` is defined, respect it
 *   * Use regular string sorting on the `fieldKey` of the form block as tie breaker if order is not defined
 * * Step through the form blocks and determine the display type for each - if consecutive blocks have the same display type, they go into the same section:
 *   * If a block is required, it is of type `expanded`
 *   * If an optional block has `always_show` set, it is of type `expanded` and it is ordered along with required fields according to its `order`
 *   * If a block is a condition block, it is of type `expanded`
 *   * Otherwise it is `collapsed-footer`
 *   * If a `collapsed-footer` directly follows a `collapsed-inline`, the footer is merged into the inline section
 * * If there is just one section and it's not `expanded`, turn it into a `collapsed-group` section
 * * If there are multiple sections and the last is `collapsed-inline`, turn it into a `collapsed-footer`
 * * If not at the root level, treat all `collapsed-footer` sections as `collapsed-inline`
 *
 * Together, these rules result in a form which is respecting defined groups and explicit ordering within groups, putting optional fields into collapsed sections
 * where possible; either by reordering fields without explicit order or by collapsing consecutive fields.
 */
export function useGroupsAndSections(
  blocks: FormBlock | FormBlock[],
  groupStructure: GroupDetails[],
  rootLevel: boolean
) {
  const { formState } = useFormContext();
  const { isHiddenAuthField } = useAuthentication();

  const sectionGroups = useMemo(
    () => generateGroupsAndSections(blocks, groupStructure, rootLevel, isHiddenAuthField),
    [blocks, groupStructure, rootLevel, isHiddenAuthField]
  );

  const sectionGroupsWithMetadata = useMemo(() => {
    return sectionGroups.map((sectionGroup) => {
      const sections = sectionGroup.sections.map((section) => {
        if (section.displayType === "expanded") {
          return section;
        }
        const hasError = section.blocks.some((block) => Boolean(get(formState.errors, block.path)));
        const initiallyOpen = section.blocks
          .filter((block) => block.const === undefined) // ignore const blocks because they are hidden
          .some((block) => {
            const initialValue = get(formState.defaultValues, block.path);
            // check if both are empty first, because the "unchanged" value that gets saved for an optional field with no default can vary (undefined, false, [], etc.)
            return isEmpty(block.default) && isEmpty(initialValue) ? false : !isEqual(block.default, initialValue);
          });
        return {
          ...section,
          hasError,
          initiallyOpen,
        };
      });
      return {
        ...sectionGroup,
        sections,
      };
    });
  }, [formState, sectionGroups]); // have to depend on full formState object to get this to be recalculated when errors change

  return sectionGroupsWithMetadata;
}

export function generateGroupsAndSections(
  blocks: FormBlock | FormBlock[],
  groupStructure: GroupDetails[],
  rootLevel: boolean,
  isHiddenAuthField: (fieldPath: string) => boolean
): SectionGroup[] {
  const blocksArray = [blocks].flat();

  const shouldSplitGroups = rootLevel && blocksArray.length > 0;
  const blockGroups = shouldSplitGroups ? splitGroups(blocksArray, groupStructure) : [{ blocks: blocksArray }];

  return blockGroups.map(splitSections(isHiddenAuthField, rootLevel));
}

function splitGroups(blocks: FormBlock[], groupStructure: GroupDetails[]): BlockGroup[] {
  const groupMap = new Map<string | undefined, FormBlock[]>();
  blocks
    .filter((block) => !block.airbyte_hidden)
    .forEach((block) => {
      if (!groupMap.has(block.group)) {
        groupMap.set(block.group, []);
      }
      groupMap.get(block.group)?.push(block);
    });
  const groups = [...groupMap.entries()];

  const groupIdToStructure = Object.fromEntries(
    groupStructure.map((groupDetails, index) => [groupDetails.id, { index, title: groupDetails.title }])
  );

  groups.sort(([a], [b]) => {
    if (a === undefined) {
      return 1;
    }
    if (b === undefined) {
      return -1;
    }
    if (groupIdToStructure[a] && groupIdToStructure[b]) {
      return groupIdToStructure[a].index - groupIdToStructure[b].index;
    }
    if (groupIdToStructure[a]) {
      return -1;
    }
    if (groupIdToStructure[b]) {
      return 1;
    }
    return naturalComparator(a, b);
  });

  return groups.map(([groupId, blocks]) => {
    return {
      blocks,
      title: groupId ? groupIdToStructure[groupId]?.title : undefined,
    };
  });
}

function splitSections(
  isHiddenAuthField: (fieldPath: string) => boolean,
  rootLevel: boolean
): (value: BlockGroup) => SectionGroup {
  return ({ blocks, title }) => {
    const sortedBlocks: FormBlock[] = blocks
      .sort(OrderComparator)
      .filter(
        (formField) => !formField.airbyte_hidden && (!isHiddenAuthField(formField.path) || formField.always_show)
      );

    const sections: Section[] = [];
    let currentSection: Section | undefined = undefined;
    for (const block of sortedBlocks) {
      // const FormBlocks are used to render Auth buttons in cloud, so they must always be required so that
      // the Auth buttons are not hidden inside a collapsed optional section
      const displayType = block.const !== undefined ? "expanded" : getDisplayType(block, rootLevel);
      if (
        currentSection &&
        (currentSection.displayType === displayType ||
          // collapsed-footer blocks should always come last, so merge with collapsed-inline if that comes
          // immediately before, to avoid having multiple consecutive collapsed sections
          (currentSection.displayType === "collapsed-inline" && displayType === "collapsed-footer"))
      ) {
        currentSection.blocks.push(block);
      } else {
        if (currentSection) {
          sections.push(currentSection);
        }
        currentSection = {
          blocks: [block],
          displayType,
        };
      }
    }
    if (currentSection) {
      sections.push(currentSection);
    }
    if (sections.length === 1 && sections[0].displayType !== "expanded") {
      // if there is only a single collapsed section in the current group, render it as a collapsed-group
      sections[0].displayType = "collapsed-group";
    }
    if (rootLevel && sections.length > 1 && sections[sections.length - 1].displayType === "collapsed-inline") {
      // If the last section is collapsed-inline, render it as a collapsed-footer for a more consistent look.
      // Only do this at root level to avoid footers sections from showing in the middle of the form
      sections[sections.length - 1].displayType = "collapsed-footer";
    }

    return {
      sections,
      title,
    };
  };
}

const getDisplayType = (block: FormBlock, rootLevel: boolean) => {
  if (block.isRequired || block.always_show || block._type === "formCondition" || block._type === "formGroup") {
    return "expanded";
  }
  if (block.order !== undefined || !rootLevel) {
    return "collapsed-inline";
  }
  return "collapsed-footer";
};

const isEmpty = (obj: unknown) => {
  if (!Boolean(obj)) {
    return true;
  }

  if (Array.isArray(obj)) {
    return obj.length === 0;
  }

  return false;
};
