import { useState } from "react";
import { useFormState, useWatch } from "react-hook-form";

import styles from "./FormDevToolsInternal.module.scss";

const FormDevToolsInternal = () => {
  const [isOpened, setIsOpened] = useState(false);
  return (
    <>
      <button
        type="button"
        className={styles.button}
        title="Open dev tools"
        onClick={() => {
          setIsOpened(!isOpened);
        }}
      />
      {isOpened && <DebugView />}
    </>
  );
};

function replacer(_key: unknown, value: unknown) {
  // Required to avoid circular references in errors which contain a reference to the input
  if (value instanceof Element) {
    return undefined;
  }
  return value;
}

const DebugView = () => {
  const values = useWatch();
  // need to destructure to subscribe to changes
  const { dirtyFields, errors, touchedFields, isValid, isDirty } = useFormState();

  return (
    <>
      <pre>{JSON.stringify(values, null, 2)}</pre>
      <pre>{JSON.stringify({ dirtyFields, errors, touchedFields, isValid, isDirty }, replacer, 2)}</pre>
    </>
  );
};

export { FormDevToolsInternal as default };
