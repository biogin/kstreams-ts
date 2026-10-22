import app from './quick-start.js'

process.on('SIGTERM', () => void app.stop())
process.on('SIGINT', () => void app.stop())

app.on('error', ({ error }) => console.error('Streams App failed', error))
await app.start()
