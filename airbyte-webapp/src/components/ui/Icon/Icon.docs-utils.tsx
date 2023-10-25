import camelcase from "camelcase";
import { load } from "cheerio";
import classnames from "classnames";
import React, { useState, useRef } from "react";
import { optimize } from "svgo";

import styles from "./Icon.docs-utils.module.scss";

// code & styles adapted from https://www.codemzy.com/blog/react-drag-drop-file-upload

export const IconConverter = () => {
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const [output, setOutput] = useState("");

  const handleFile = (file: File) => {
    const reader = new FileReader();
    const iconName = camelcase(file.name.replace(".svg", ""));
    reader.addEventListener("load", (e) => {
      const source = e.target?.result as string | undefined;
      if (source) {
        // use cheerio to add the role attribute and set data-icon
        const $ = load(source);
        const $svg = $("svg");

        $svg.removeAttr("xmlns");
        $svg.attr("data-icon", iconName);
        $svg.attr("role", "img");
        const cheerioOut = $.html("svg");

        const result = optimize(cheerioOut, {
          path: file.name,
          js2svg: {
            pretty: true,
          },
          plugins: [
            {
              name: "preset-default",
              params: {
                overrides: {
                  convertColors: {
                    currentColor: true,
                  },
                  removeUnknownsAndDefaults: {
                    keepRoleAttr: true,
                  },
                  removeViewBox: false,
                },
              },
            },
            "removeXMLNS",
          ],
        });
        setOutput(result.data);
      }
    });
    reader.readAsText(file);
  };

  // handle drag events
  const handleDrag: React.DragEventHandler<HTMLFormElement | HTMLDivElement> = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  // triggers when file is dropped
  const handleDrop = (e: React.DragEvent<HTMLDivElement | HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  // triggers when file is selected with click
  const handleChange = (e: React.FormEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.currentTarget.files && e.currentTarget.files[0]) {
      handleFile(e.currentTarget.files[0]);
    }
  };

  // triggers the input when the button is clicked
  const onButtonClick = () => {
    inputRef.current?.click();
  };

  return (
    <>
      <form className={styles.formFileUpload} onDragEnter={handleDrag} onSubmit={(e) => e.preventDefault()}>
        <input
          ref={inputRef}
          type="file"
          id="input-file-upload"
          className={styles.inputFileUpload}
          multiple
          onChange={handleChange}
        />
        <label
          className={classnames(styles.labelFileUpload, dragActive ? styles.dragActive : undefined)}
          htmlFor="input-file-upload"
        >
          <div>
            <p>Drag and drop your file here or</p>
            <button className={styles.uploadButton} onClick={onButtonClick}>
              Upload a file
            </button>
          </div>
        </label>
        {dragActive && (
          <div
            className={styles.dragFileElement}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
          />
        )}
      </form>
      {output && <code className={styles.code}>{output}</code>}
    </>
  );
};
