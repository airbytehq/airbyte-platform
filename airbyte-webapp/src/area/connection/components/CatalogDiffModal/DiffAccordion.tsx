import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import { useMemo } from "react";

import { StreamTransform } from "core/api/types/AirbyteClient";

import styles from "./DiffAccordion.module.scss";
import { DiffAccordionHeader } from "./DiffAccordionHeader";
import { DiffFieldTable } from "./DiffFieldTable";
import { DiffStreamAttribute } from "./DiffStreamAttribute";
import { getSortedDiff } from "./utils";

interface DiffAccordionProps {
  streamTransform: StreamTransform;
}

const hasBreakingChanges: (streamTransform: StreamTransform) => boolean = (streamTransform) => {
  return !!(
    streamTransform.updateStream?.fieldTransforms?.some((t) => t.breaking) ||
    streamTransform.updateStream?.streamAttributeTransforms?.some((t) => t.breaking)
  );
};

export const DiffAccordion: React.FC<DiffAccordionProps> = ({ streamTransform }) => {
  const { newItems, removedItems, changedItems } = useMemo(
    () => getSortedDiff(streamTransform.updateStream?.fieldTransforms),
    [streamTransform.updateStream]
  );

  return (
    <div className={styles.accordionContainer}>
      <Disclosure>
        {({ open }) => (
          <>
            <DisclosureButton
              className={styles.accordionButton}
              aria-label={`${open ? "collapse" : "expand"} list with changes in ${
                streamTransform.streamDescriptor.name
              } stream`}
              data-testid={`toggle-accordion-${streamTransform.streamDescriptor.name}-stream`}
            >
              <DiffAccordionHeader
                hasBreakingChanges={hasBreakingChanges(streamTransform)}
                streamDescriptor={streamTransform.streamDescriptor}
                removedCount={removedItems.length}
                newCount={newItems.length}
                changedCount={
                  changedItems.length + (streamTransform.updateStream?.streamAttributeTransforms?.length ?? 0)
                }
                open={open}
              />
            </DisclosureButton>
            <DisclosurePanel className={styles.accordionPanel}>
              {!!streamTransform.updateStream?.streamAttributeTransforms?.length && (
                <DiffStreamAttribute transforms={streamTransform.updateStream.streamAttributeTransforms} />
              )}
              {removedItems.length > 0 && <DiffFieldTable fieldTransforms={removedItems} diffVerb="removed" />}
              {newItems.length > 0 && <DiffFieldTable fieldTransforms={newItems} diffVerb="new" />}
              {changedItems.length > 0 && <DiffFieldTable fieldTransforms={changedItems} diffVerb="changed" />}
            </DisclosurePanel>
          </>
        )}
      </Disclosure>
    </div>
  );
};
