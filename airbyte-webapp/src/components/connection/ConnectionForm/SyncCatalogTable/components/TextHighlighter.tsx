import React from "react";
import Highlighter, { HighlighterProps } from "react-highlight-words";

import styles from "./TextHighlighter.module.scss";

export const TextHighlighter: React.FC<Pick<HighlighterProps, "searchWords" | "textToHighlight">> = ({
  searchWords,
  textToHighlight,
}) => (
  <Highlighter
    highlightClassName={styles.highlight}
    searchWords={searchWords}
    textToHighlight={textToHighlight}
    autoEscape
  />
);
