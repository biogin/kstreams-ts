import { stores } from '../src/index.js'
import { describeStateStoreContract } from './state-store-contract.js'

describeStateStoreContract('in-memory', () =>
  stores.inMemory()({ name: 'contract', applicationId: 'contract', task: 0, directory: undefined }),
)
