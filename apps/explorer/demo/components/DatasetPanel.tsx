import type { SchemaColumn } from "../arch/types.ts";
import { h3, panelInset, schemaList } from "../ui/classes.ts";

export type DatasetPanelInput = {
  schema: SchemaColumn[];
};

type Props = {
  input: DatasetPanelInput;
  onEvent: (event: never) => void;
};

export function DatasetPanel({ input }: Props) {
  return (
    <details class={`${panelInset} mb-4`}>
      <summary class={h3}>Schema</summary>
      <ul id="schema-list" class={`${schemaList} mt-2`}>
        {input.schema.map((col) => (
          <li key={col.id === -1 ? "more" : col.id}>
            {col.id >= 0 ? (
              <>
                <span class="text-slate-400">{col.id}</span> {col.name}{" "}
                <span class="text-slate-500">({col.physicalType})</span>
              </>
            ) : (
              col.name
            )}
          </li>
        ))}
      </ul>
    </details>
  );
}
