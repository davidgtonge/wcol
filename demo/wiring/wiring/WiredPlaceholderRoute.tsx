import { useSelector } from "../arch/use-selector.ts";
import { traceRender } from "../arch/debug-renders.ts";
import { selectExploreRoute } from "../arch/selectors.ts";
import { PlaceholderRoute } from "../components/PlaceholderRoute.tsx";

export function WiredPlaceholderRoute() {
  traceRender("WiredPlaceholderRoute");
  const route = useSelector(selectExploreRoute);
  if (route === "explore") return null;
  return <PlaceholderRoute route={route} />;
}
