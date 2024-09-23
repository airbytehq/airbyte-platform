import camelcase from "camelcase";
import classnames from "classnames";
import React, { useState, useRef, useMemo } from "react";
import { optimize } from "svgo";

import { FILE_TYPE_DOWNLOAD, downloadFile } from "core/utils/file";

import { Icon, Icons } from "./Icon";
import styles from "./Icon.docs-utils.module.scss";
import { IconProps, IconType } from "./types";
import { withProviders } from "../../../../.storybook/withProvider";
import { Button } from "../Button";
import { CopyButton } from "../CopyButton";
import { FlexContainer, FlexItem } from "../Flex";

// code & styles adapted from https://www.codemzy.com/blog/react-drag-drop-file-upload

export const IconConverter = () => {
  return withProviders(() => <IconConverterInner />);
};

const IconConverterInner = () => {
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const [output, setOutput] = useState<{ name: string; svg: string } | null>(null);

  const handleFile = (file: File) => {
    const reader = new FileReader();
    const iconName = camelcase(file.name.replace(".svg", ""));
    reader.addEventListener("load", (e) => {
      const source = e.target?.result as string | undefined;
      if (source) {
        const result = optimize(source, {
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
            {
              name: "removeAttrs",
              params: {
                attrs: "(height|width)",
              },
            },
            {
              name: "addAttributesToSVGElement",
              params: {
                attributes: [{ "data-icon": iconName }, { role: "img" }],
              },
            },
          ],
        });
        setOutput({ name: iconName, svg: result.data });
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

  const iconPreview = useMemo(() => {
    if (output == null) {
      return null;
    }

    const { name, svg } = output;

    Icons[name as IconType] = (props) => {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore-next-line
      // eslint-disable-next-line react/no-danger
      return <span {...props} dangerouslySetInnerHTML={{ __html: svg }} />;
    };

    const sizes: Array<IconProps["size"]> = ["md", "lg", "xl"];
    const colors: Array<IconProps["color"]> = [
      "primary",
      "disabled",
      "action",
      "success",
      "error",
      "warning",
      "affordance",
      "foreground",
      "magic",
    ];

    return output ? (
      <>
        <table>
          <tbody>
            {sizes.map((size) => (
              <tr key={size}>
                {colors.map((color) => (
                  <td align="center" key={color}>
                    <Icon size={size} color={color} type={name as IconType} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        <table>
          <tbody>
            {sizes.map((size) => (
              <tr key={size}>
                {colors.map((color) => (
                  <td align="center" key={color}>
                    <Icon size={size} color={color} type={name as IconType} withBackground />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </>
    ) : undefined;
  }, [output]);

  return (
    <>
      <FlexContainer>
        <FlexItem>
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
        </FlexItem>
        <FlexItem>{iconPreview}</FlexItem>
      </FlexContainer>
      {output && (
        <>
          <FlexContainer>
            <FlexItem>
              <CopyButton content={output.svg} />
            </FlexItem>
            <FlexItem>
              <Button
                onClick={() => {
                  const file = new Blob([output.svg], {
                    type: FILE_TYPE_DOWNLOAD,
                  });
                  downloadFile(file, `${output.name}.svg`);
                }}
              >
                Download
              </Button>
            </FlexItem>
          </FlexContainer>
          <code className={styles.code}>{output.svg}</code>
        </>
      )}
    </>
  );
};
