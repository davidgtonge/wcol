fn main() {
    wcol_engine::export_types().expect("failed to export TypeScript types");
    eprintln!("exported → wcol/demo/generated/engine-types.ts");
}
