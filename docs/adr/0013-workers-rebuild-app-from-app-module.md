# Workers rebuild the Streams App from an explicit App Module

Worker threads can't receive a Streams App definition, because it contains functions (DSL callbacks, Processors, Serdes). With `workers > 0`, the app must be exported from its own App Module, and config names that module: `workers: { count, module: import.meta.url }`. Each Worker imports the module and rebuilds the same app. We don't detect the entry file and re-run it inside Workers, because that silently re-runs whatever else the file starts (HTTP servers, timers, connections).

## Considered Options

- Re-run the entry file (`process.argv[1]`) in each Worker behind an `isWorker()` guard: rejected, because the re-execution is surprising and its side effects are easy to miss.

## Consequences

- With `workers > 0`, users must keep the app definition in a module with no side effects. `workers: 0` keeps working with an app defined anywhere.
- The App Module must export the same app on every import; it's rebuilt per Worker, so its definition must be deterministic.
- Bundlers must keep the App Module as a separate file that can be loaded at runtime.
