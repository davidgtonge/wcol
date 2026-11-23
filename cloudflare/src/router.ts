type Handler = (request: Request, url: URL) => Response | Promise<Response>;

/** Exact `METHOD /path` lookup. No parameters, middleware, or pattern matching. */
export function createRouter() {
  const routes = new Map<string, Handler>();

  const register = (method: string, path: string, handler: Handler) => {
    routes.set(`${method} ${path}`, handler);
  };

  return {
    get: (path: string, handler: Handler) => register("GET", path, handler),

    post: (path: string, handler: Handler) => register("POST", path, handler),

    async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);
      const handler = routes.get(`${request.method} ${url.pathname}`);

      if (!handler) {
        return Response.json({ error: "not found" }, { status: 404 });
      }

      try {
        return await handler(request, url);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return Response.json({ error: message.slice(0, 500) }, { status: 500 });
      }
    },
  };
}
