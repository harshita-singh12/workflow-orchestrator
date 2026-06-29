export default function JsonView({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return <span className="json-empty">—</span>;
  }
  let text: string;
  try {
    text = JSON.stringify(value, null, 2);
  } catch {
    text = String(value);
  }
  if (text === "{}" || text === "null") {
    return <span className="json-empty">—</span>;
  }
  return <pre className="json-view">{text}</pre>;
}
