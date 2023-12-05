import classNames from "classnames";
import { useRef, useState } from "react";

import { Icon } from "components/ui/Icon";

import styles from "./FileUpload.module.scss";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";

interface FileUploadProps {
  onUpload: (content: string) => void;
}

export const FileUpload: React.FC<FileUploadProps> = ({ onUpload }) => {
  const [isDraggingOver, setIsDraggingOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const readFile = (file: File) => {
    const reader = new FileReader();

    reader.readAsText(file);

    reader.onload = () => {
      const content = convertToString(reader.result);

      if (content) {
        onUpload(content);
      }
    };

    // Set file input back to empty so that the same file can be reuploaded again
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  };

  const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDraggingOver(false);

    const file = event.dataTransfer.files[0];
    readFile(file);
  };

  const handleFileInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];

    if (file) {
      readFile(file);
    }
  };

  const handleDragLeave = (event: React.DragEvent<HTMLDivElement>) => {
    // Check if the related target is a child of the container.
    // This prevents the isDraggingOver state being set to false when
    // dragging a file over any of the child elements of the parent div.
    const { currentTarget, relatedTarget } = event;
    if (currentTarget.contains(relatedTarget as Node)) {
      return;
    }

    setIsDraggingOver(false);
  };

  return (
    <div
      className={styles.container}
      onDragOver={(event) => event.preventDefault()}
      onDrop={handleDrop}
      onDragEnter={(e) => {
        e.preventDefault();
        setIsDraggingOver(true);
      }}
      onDragLeave={handleDragLeave}
    >
      <button
        type="button"
        onClick={() => fileInputRef.current?.click()}
        className={classNames(styles.button, { [styles.draggingOver]: isDraggingOver })}
      >
        <FlexContainer alignItems="center" justifyContent="center" gap="sm" className={styles.buttonContents}>
          <Icon type="file" />
          <Text className={styles.buttonText} onDragOver={(e) => e.preventDefault()}>
            Upload file
          </Text>
        </FlexContainer>
      </button>
      <input type="file" ref={fileInputRef} onChange={handleFileInputChange} hidden />
    </div>
  );
};

const convertToString = (content: string | ArrayBuffer | null) => {
  if (typeof content === "string") {
    return content;
  } else if (content instanceof ArrayBuffer) {
    const decoder = new TextDecoder("utf-8");
    return decoder.decode(content);
  }

  return undefined;
};
