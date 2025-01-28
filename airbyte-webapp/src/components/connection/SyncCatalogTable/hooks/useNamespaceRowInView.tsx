import { useEffect } from "react";
import { useInView } from "react-intersection-observer";

export const useNamespaceRowInView = (
  rowIndex: number,
  stickyRowIndex: number,
  stickyIndexes: number[],
  setStickyRowIndex: (stickyRowIndex: number) => void,
  customScrollParent: HTMLElement | null
) => {
  const { ref, inView, entry } = useInView({
    root: customScrollParent,
    rootMargin: `-80px 0px -${customScrollParent && customScrollParent.clientHeight - 159}px 0px`, // area of the root container that will trigger the inView event
    threshold: 1,
  });

  useEffect(() => {
    if (!inView) {
      return;
    }

    let closestStickyIndex = 0;

    for (const num of stickyIndexes) {
      if (num <= rowIndex) {
        closestStickyIndex = num;
      } else {
        break; // No need to check further
      }
    }

    if (closestStickyIndex === stickyRowIndex) {
      return;
    }
    setStickyRowIndex(closestStickyIndex);
  }, [entry, inView, ref, rowIndex, setStickyRowIndex, stickyIndexes, stickyRowIndex]);

  return { ref };
};
