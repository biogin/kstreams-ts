// App Module of the crash test suite with worker threads (ADR-0013): its default export is the
// crash app itself (crash-app.ts) with `workers: { count: params.workers, module: import.meta.url }`,
// so each worker thread imports this module and rebuilds the same app. The params come from the
// KSTREAMS_APP_PARAMS environment variable the App Process harness sets, which worker threads see
// too.

import crashApp, { type CrashParams } from './crash-app.js'

const params = JSON.parse(process.env.KSTREAMS_APP_PARAMS ?? '{}') as CrashParams

export default crashApp(params, import.meta.url)
