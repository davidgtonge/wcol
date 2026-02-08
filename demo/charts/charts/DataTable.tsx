import { chartCaption, chartSub, hint, table, tableWrap, td, th } from "../ui/classes.ts";

export type DataTableInput = {
  title: string;
  columns: string[];
  rows: Record<string, string | number | boolean | null>[];
};

type Props = {
  input: DataTableInput;
  onCrateSelect?: (name: string) => void;
};

export function DataTable({ input, onCrateSelect }: Props) {
  const crateCol = input.columns.find((c) => c === "crate_name") ?? null;
  if (!input.rows.length) return <p class={hint}>No rows</p>;

  return (
    <figure>
      <figcaption class={chartCaption}>
        <strong class="text-sm">{input.title}</strong>
        <span class={chartSub}>{input.rows.length} rows</span>
      </figcaption>
      <div class={tableWrap}>
        <table class={table}>
          <thead>
            <tr>
              {input.columns.map((col) => (
                <th key={col} class={th}>
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {input.rows.map((row, ri) => (
              <tr key={ri} class="hover:bg-blue-500/5">
                {input.columns.map((col) => {
                  const value = formatCell(row[col]);
                  const clickable = crateCol === col && onCrateSelect && value !== "—";
                  return (
                    <td key={col} class={td}>
                      {clickable ? (
                        <button
                          type="button"
                          class="text-left text-blue-600 hover:underline dark:text-blue-400"
                          onClick={() => onCrateSelect(value)}
                        >
                          {value}
                        </button>
                      ) : (
                        value
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </figure>
  );
}

function formatCell(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return value.toLocaleString();
  return String(value);
}
