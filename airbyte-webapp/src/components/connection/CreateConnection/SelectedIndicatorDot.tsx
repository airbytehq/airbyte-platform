// Once we refactor the RadioButton, we should replace this component with it https://github.com/airbytehq/airbyte/issues/25202
export const SelectedIndicatorDot = ({ selected = false }: { selected: boolean }) => {
  if (selected) {
    return (
      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" role="img">
        <path
          fill="#615EFF"
          fillRule="evenodd"
          d="M8 0a8 8 0 1 1 0 16A8 8 0 0 1 8 0Zm0 4a4 4 0 1 1 0 8 4 4 0 0 1 0-8Z"
        />
      </svg>
    );
  }
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" role="img">
      <g fill="#E8E8ED">
        <path d="M12 8a4 4 0 1 0-8 0 4 4 0 0 0 8 0Z" />
        <path
          fillRule="evenodd"
          d="M8 2.222a5.778 5.778 0 1 0 0 11.556A5.778 5.778 0 0 0 8 2.222ZM0 8a8 8 0 1 1 16 0A8 8 0 0 1 0 8Z"
        />
      </g>
    </svg>
  );
};
