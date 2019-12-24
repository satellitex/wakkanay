import { KeyValueStore, Iterator, BatchOperation } from './KeyValueStore'
import { Bytes } from '../types/Codables'
const STORE_KEY_PATH = 'key'

class IndexedDbIterator implements Iterator {
  private req: IDBRequest | null = null
  private cursor: IDBCursorWithValue | null = null

  constructor(
    readonly lowerBound: Bytes,
    readonly dbPromise: Promise<IDBDatabase>,
    readonly storeKey: string
  ) {}

  private async init() {
    const db = await this.dbPromise
    const tx = db.transaction(this.storeKey, 'readonly')
    const store = tx.objectStore(this.storeKey)
    this.req = store.openCursor(
      IDBKeyRange.lowerBound(this.lowerBound.intoString())
    )
  }

  public async next(): Promise<{ key: Bytes; value: Bytes } | null> {
    if (!this.cursor) {
      await this.init()
      this.cursor = await new Promise(resolve => {
        if (this.req) {
          this.req.onsuccess = e => {
            resolve((e.target as any).result as IDBCursorWithValue)
          }
        }
      })
      return this.cursor ? createBytesKeyValue({ ...this.cursor.value }) : null
    } else {
      const result: IDBCursorWithValue = await new Promise(resolve => {
        if (this.cursor && this.req) {
          this.cursor.continue()
          this.req.onsuccess = e => {
            resolve((e.target as any).result)
          }
        }
      })
      return result.value ? createBytesKeyValue({ ...result.value }) : null
    }
  }
}

export class IndexedDbKeyValueStore implements KeyValueStore {
  private dbName: Bytes
  private db: IDBDatabase | null = null
  private openPromise: Promise<IDBDatabase>
  private storeKey: string

  constructor(
    name: Bytes = Bytes.fromString('db'),
    key = '_',
    version?: number
  ) {
    this.dbName = name
    this.storeKey = key
    this.openPromise = this.openDb(version)
  }

  private async openDb(version?: number): Promise<IDBDatabase> {
    return await new Promise(resolve => {
      const req = indexedDB.open(this.dbName.intoString(), version)

      req.onsuccess = () => {
        const db = req.result
        db.onversionchange = e => {
          this.db = null
          if (e.newVersion) {
            this.openPromise = this.openDb(e.newVersion)
          }
          db.close()
        }
        resolve(db)
      }

      req.onupgradeneeded = () => {
        const db = req.result
        if (!db.objectStoreNames.contains(this.storeKey)) {
          req.result.createObjectStore(this.storeKey, {
            keyPath: STORE_KEY_PATH
          })
        }
      }
    })
  }

  private async getDb(): Promise<IDBDatabase> {
    return this.db || (await this.openPromise)
  }

  private async getVersion(): Promise<number> {
    const db = await this.getDb()
    return db.version
  }

  private async getObjectStoreNames(): Promise<DOMStringList> {
    const db = await this.getDb()
    return db.objectStoreNames
  }

  public async get(key: Bytes): Promise<Bytes | null> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readonly')
    const store = tx.objectStore(this.storeKey)
    const req = store.get(key.intoString())

    return await new Promise(resolve => {
      req.onerror = () => {
        resolve(null)
      }
      req.onsuccess = () => {
        const result = req.result ? req.result.value : null
        resolve(result)
      }
    })
  }

  public async put(key: Bytes, value: Bytes): Promise<void> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readwrite')
    const store = tx.objectStore(this.storeKey)
    store.put(createKeyValue(key.intoString(), value))

    return new Promise((resolve, reject) => {
      tx.onerror = () => {
        reject(new Error('cannot complete put operation'))
      }
      tx.oncomplete = () => {
        resolve()
      }
    })
  }

  public async del(key: Bytes): Promise<void> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readwrite')
    const store = tx.objectStore(this.storeKey)
    store.delete(key.intoString())
    return new Promise((resolve, reject) => {
      tx.onerror = () => {
        reject(new Error('cannot complete put operation'))
      }
      tx.oncomplete = () => {
        resolve()
      }
    })
  }

  public async batch(operations: BatchOperation[]): Promise<void> {
    // console.log('batch')
  }

  public iter(lowerBound: Bytes): Iterator {
    return new IndexedDbIterator(lowerBound, this.getDb(), this.storeKey)
  }

  // use objectStore to manage bucket
  // each bucket has a bucket key as a private field for a key of global objectStore.
  // bucket(key) method returns a new IndexedDbKeyValueStore instance concatenating a given key
  // with parent key.
  // in order to add a new objectStore, bucket must pass new version number to constructor.
  public async bucket(key: Bytes): Promise<KeyValueStore> {
    let version
    const objectStoreNames = await this.getObjectStoreNames()
    const newObjectStoreKey = this.storeKey + key.intoString()
    if (!objectStoreNames.contains(newObjectStoreKey)) {
      version = (await this.getVersion()) + 1
    }

    return Promise.resolve(
      new IndexedDbKeyValueStore(this.dbName, newObjectStoreKey, version)
    )
  }

  public async open(): Promise<void> {
    // call getDb to open connection if it's been closed.
    // if it's already open, do nothing.
    await this.getDb()
  }

  public async close(): Promise<void> {
    return await new Promise(resolve => {
      if (this.db) {
        this.db.close()
        this.db.onclose = () => {
          resolve()
        }
      } else {
        resolve()
      }
    })
  }
}

// create key value pair to store in indexeddb
function createKeyValue<T>(key: string, value: T) {
  return {
    key,
    value
  }
}

function createBytesKeyValue<T>({ key, value }: { key: string; value: T }) {
  return {
    key: Bytes.fromString(key),
    value
  }
}