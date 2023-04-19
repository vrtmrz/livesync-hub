import { LiveSyncLocalDB, LiveSyncLocalDBEnv } from "./lib/src/LiveSyncLocalDB.ts";
import { LiveSyncDBReplicator, LiveSyncReplicatorEnv } from "./lib/src/LiveSyncReplicator.ts";
import { DocumentID, EntryHasPath, FilePath, FilePathWithPrefix, LOG_LEVEL, LoadedEntry, RemoteDBSettings } from "./lib/src/types.ts";
import { ObservableStore } from "./lib/src/store.ts";
import { EntryDoc, DatabaseConnectingStatus } from "./lib/src/types.ts";
import { addPrefix, getPath, isPlainText } from "./lib/src/path.ts";
import { enableEncryption, isCloudantURI } from "./lib/src/utils_couchdb.ts";
import { Logger } from "./lib/src/logger.ts";
import { PouchDB } from "./pouchdb.ts";
import { arrayBufferToBase64, base64ToArrayBuffer, writeString } from "./lib/src/strbin.ts";
import { delay, getDocData, isDocContentSame } from "./lib/src/utils.ts";
import { utimesSync } from "node:fs";
import { runWithLock } from "./lib/src/lock.ts";
import path from "node:path/posix";
import { dirname, expandGlob, fromFileUrl, join, posix, relative } from "./mod.ts";
import { statusDefault } from "./types.ts";
import { fetchStatOnStorage, scheduleTask, normalizePath, id2path, isIdOfInternalMetadata, stripInternalMetadataPrefix, path2id, loadStat, saveStat } from "./utils.ts";



export class RemoteDBProc implements LiveSyncLocalDBEnv, LiveSyncReplicatorEnv {

  settings: RemoteDBSettings;
  localDatabase: LiveSyncLocalDB;
  replicator!: LiveSyncDBReplicator;
  localBasePath?: string;
  remoteBasePath: string;
  dbName: string;
  fileProcessedMap = new Map<string, number>();
  group?: string;
  otherProcessors: RemoteDBProc[];
  fileProcessor: string | undefined;
  getProgressFilename() {
    return `./dat/${this.dbName}-stat.json`;
  }
  status = { ...statusDefault };
  async flushStat() {
    this.status.lastProcessed = Object.fromEntries(this.fileProcessedMap.entries());
    await saveStat(this.getProgressFilename(), this.status);
  }
  constructor(settings: RemoteDBSettings, dbName: string, localBasePath: string | undefined, remoteBasePath: string, group: string | undefined, fileProcessor: string | undefined, otherProcessors: RemoteDBProc[]) {
    this.settings = settings;
    this.dbName = dbName;
    //TODO:DB NAME
    this.localDatabase = new LiveSyncLocalDB("./dat/" + dbName, this);
    this.localBasePath = localBasePath;
    this.remoteBasePath = remoteBasePath;
    this.group = group;
    this.otherProcessors = otherProcessors;
    this.fileProcessor = fileProcessor;
    this.watch();
  }

  async storeLoadedEntryToDB(d: LoadedEntry, force: boolean, initialScan: boolean) {
    const datatype = d.datatype;
    const path = getPath(d);
    const mtime = d.mtime;
    const msg = `DB <- STORAGE (${datatype}) `;
    const isNotChanged = await runWithLock("file-" + path + this.dbName, false, async () => {
      const lTime = (this.fileProcessedMap.has(path) && this.fileProcessedMap.get(path)) || -1;
      if (lTime == mtime) {
        return true;
      }

      try {
        const old = await this.localDatabase.getDBEntry(path as FilePath, null, false, false);
        if (old !== false) {
          const oldData = { data: old.data, deleted: old._deleted || old.deleted };
          const newData = { data: d.data, deleted: d._deleted || d.deleted };
          if (oldData.deleted != newData.deleted)
            return false;
          if (!isDocContentSame(old.data, newData.data))
            return false;
          Logger(msg + "Skipped (not changed) " + path + ((d._deleted || d.deleted) ? " (deleted)" : ""), LOG_LEVEL.VERBOSE);
          return true;
          // d._rev = old._rev;
        }
      } catch (ex) {
        if (force) {
          Logger(msg + "Error, Could not check the diff for the old one." + (force ? "force writing." : "") + path + ((d._deleted || d.deleted) ? " (deleted)" : ""), LOG_LEVEL.VERBOSE);
        } else {
          Logger(msg + "Error, Could not check the diff for the old one." + path + ((d._deleted || d.deleted) ? " (deleted)" : ""), LOG_LEVEL.VERBOSE);
        }
        return !force;
      }
      return false;
    });
    if (isNotChanged)
      return true;
    const ret = await this.localDatabase.putDBEntry(d, initialScan);
    this.fileProcessedMap.set(path, mtime);
    scheduleTask("flush", 100, () => this.snapshotFileProcMap());

    Logger(msg + path);
    return ret != false;
  }

  async updateIntoDB(path: string, pathOnStorage: string, initialScan?: boolean, force?: boolean) {
    // if (!this.isTargetFile(file)) return true;
    let content: string | string[];
    let datatype: "plain" | "newnote" = "newnote";
    const stat = await fetchStatOnStorage(pathOnStorage);
    if (!stat) {
      Logger(`Missing: ${path}`, LOG_LEVEL.NOTICE);
      return false;
    }

    if (!isPlainText(path)) {
      Logger(`Reading   : ${path}`, LOG_LEVEL.VERBOSE);
      const contentBin = (await Deno.readFile(pathOnStorage)).buffer;
      Logger(`Processing: ${path}`, LOG_LEVEL.VERBOSE);
      try {
        content = await arrayBufferToBase64(contentBin);
      } catch (ex) {
        Logger(`The file ${path} could not be encoded`);
        Logger(ex, LOG_LEVEL.VERBOSE);
        return false;
      }
      datatype = "newnote";
    } else {
      content = await Deno.readTextFile(pathOnStorage);
      datatype = "plain";
    }

    const id = await this.path2id(path as FilePath);
    const mtime = ~~((stat.mtime?.getTime() || 0) / 1000) * 1000;
    const d: LoadedEntry = {
      _id: id,
      path: path as FilePathWithPrefix,
      data: content,
      ctime: mtime,
      mtime: mtime,
      size: stat.size,
      children: [],
      datatype: datatype,
      type: datatype,
    };
    //upsert should locked
    try {
      await this.storeLoadedEntryToDB(d, initialScan || false, force || false);
    } catch (ex) {
      Logger(ex);
    }
  }
  async snapshotFileProcMap() {
    this.flushStat();
  }
  onFileChanged(kind: string, paths: string[]) {
    const base = this.localBasePath || "";
    if (kind == "create" || kind == "modify" || kind == "remove") {
      for (const path of paths) {
        const localFilePath = relative(base, path);
        const key = `task-${localFilePath}`;
        scheduleTask(key, 100, ((path: string) => async () => {
          const pathOnStorage = fromFileUrl("file://" + posix.join(base, path));
          const stat = await fetchStatOnStorage(pathOnStorage);
          const pathOnRemote = normalizePath(posix.join(this.remoteBasePath, path)) as FilePath;
          const lPath = normalizePath(pathOnRemote);
          if (stat && stat.isDirectory)
            return;
          const lTime = (this.fileProcessedMap.has(lPath) && this.fileProcessedMap.get(lPath)) || -1;
          if (stat == false) {
            if (lTime == -1) {
              Logger(`Storage changes: ${this.dbName}:${path}  skipped (deleted)`);
            } else {
              Logger(`Storage changes: ${this.dbName}:${path} deleted`);
              await this.localDatabase.deleteDBEntry(pathOnRemote);
              this.fileProcessedMap.set(path, 0);
              scheduleTask("flush", 100, () => this.snapshotFileProcMap());
            }
          } else {
            const mtime = ~~((stat.mtime?.getTime() || 0) / 1000);
            if (lTime == mtime) {
              Logger(`Storage changes: ${this.dbName}:${path}  skipped (modified, but known)`);
            } else if (mtime > lTime) {
              await this.updateIntoDB(pathOnRemote, pathOnStorage);
              this.fileProcessedMap.set(path, mtime);
              scheduleTask("flush", 100, () => this.snapshotFileProcMap());
              Logger(`Storage changes: ${this.dbName}:${path}  modified`);

            } else {
              Logger(`Storage changes: ${this.dbName}:${path}  skipped (modified, but old)`);
            }
          }
          await Promise.resolve();
        })(localFilePath));
      }
    }
  }
  watch() {
    (async () => {
      if (!this.localBasePath) return;
      const base = this.localBasePath;
      const localPath = fromFileUrl("file://" + base);
      const watcher = Deno.watchFs(localPath, { recursive: true });
      for await (const event of watcher) {

        const kind = event.kind;
        this.onFileChanged(kind, event.paths);

      }
    })();
  }

  id2path(id: DocumentID, entry: EntryHasPath, stripPrefix?: boolean | undefined): FilePathWithPrefix {
    const tempId = id2path(id, entry);
    if (stripPrefix && isIdOfInternalMetadata(tempId)) {
      const out = stripInternalMetadataPrefix(tempId);
      return out;
    }
    return tempId;
  }

  async path2id(filename: FilePathWithPrefix | FilePath, prefix?: string): Promise<DocumentID> {
    const destPath = addPrefix(filename, prefix ?? "");
    return await path2id(destPath, this.settings.usePathObfuscation ? this.settings.passphrase : "");
  }
  createPouchDBInstance<T extends {}>(name?: string | undefined, options?: PouchDB.Configuration.DatabaseConfiguration | undefined): PouchDB.Database<T> {
    return new PouchDB<T>(`${name}`, {
      ...options
    });
  }
  beforeOnUnload(db: LiveSyncLocalDB): void {
    //NO OP
  }
  onClose(db: LiveSyncLocalDB): void {
    //NO OP.
  }
  nodeId = "";
  async onInitializeDatabase(db: LiveSyncLocalDB) {
    this.replicator = new LiveSyncDBReplicator(this, this.nodeId);
  }
  async onResetDatabase(db: LiveSyncLocalDB) {
    this.replicator = new LiveSyncDBReplicator(this, this.nodeId);
  }
  getReplicator() {
    return this.replicator;
  }
  getSettings(): RemoteDBSettings {
    return this.settings;
  }
  getDatabase(): PouchDB.Database<EntryDoc> {
    return this.localDatabase.localDatabase;
  }
  getIsMobile(): boolean {
    return false;
  }
  getLastPostFailedBySize(): boolean {
    return !this.last_successful_post;
  }
  last_successful_post = false;
  async runProcessor() {
    await runWithLock(`processor-${this.dbName}`, false, async () => {
      if (!this.fileProcessor) return;
      Logger(`${this.dbName}:Running processor ${this.fileProcessor}`);
      const p = Deno.run({
        cmd: [this.fileProcessor],
        stdout: "piped",
        stderr: "piped",
      });
      // Reading the outputs closes their pipes
      const [{ code }, rawOutput, rawError] = await Promise.all([
        p.status(),
        p.output(),
        p.stderrOutput(),
      ]);
      const dec = new TextDecoder();
      Logger(`${this.dbName}:Processor ${this.fileProcessor} finished with code ${code}`);
      Logger(`RESULT:${dec.decode(rawOutput)}\n---${dec.decode(rawError)}\n---`, LOG_LEVEL.VERBOSE);
    })
  }
  async fetchDoc(doc: EntryDoc) {
    try {

      if (doc.type != "newnote" && doc.type != "notes" && doc.type != "plain") {
        return;
      }
      const remotePath = getPath(doc);


      if (!remotePath.startsWith(this.remoteBasePath)) {
        Logger(`Skipped:${this.dbName}:${remotePath} out of scope`);
        return;
      }
      if (isIdOfInternalMetadata(remotePath)) {
        Logger(`Skipped:${this.dbName}:${remotePath} internal`);
        return;
      }
      if (!this.localDatabase.isTargetFile(remotePath)) {
        Logger(`Skipped:${this.dbName}:${remotePath} not target`);
        return;
      }
      Logger(`Processing ${this.dbName}:${remotePath}`);
      const relativePath = remotePath.substring(this.remoteBasePath.length);
      const docMtime = ~~(doc.mtime / 1000);
      if (this.fileProcessedMap.has(relativePath) && this.fileProcessedMap.get(relativePath) == docMtime) {
        return;
      }
      this.fileProcessedMap.set(relativePath, docMtime);
      scheduleTask("flush", 100, () => this.snapshotFileProcMap());
      const replicateToServer = this.otherProcessors.filter(e => e.group && this.group && e.group == this.group && e.dbName != this.dbName);
      for (const replicateTo of replicateToServer) {
        try {
          // Logger(`Replicate to ${replicateTo.dbName}`);
          const relativePath = remotePath.substring(this.remoteBasePath.length);
          const pathOnDestination = normalizePath(posix.join(replicateTo.remoteBasePath, relativePath));
          Logger(`Transfer from ${this.dbName} to ${replicateTo.dbName} (${remotePath} -> ${pathOnDestination})`);
          const xxDoc = await this.localDatabase.getDBEntry(remotePath, { rev: doc._rev }, false);
          if (xxDoc) {

            xxDoc._id = await this.path2id(pathOnDestination as FilePathWithPrefix);
            xxDoc.path = pathOnDestination as FilePathWithPrefix
            delete xxDoc._rev;
            const ret = await replicateTo.storeLoadedEntryToDB(xxDoc, false, false);
            if (ret) {
              Logger(`Transferred to ${replicateTo.dbName}`);
            } else {
              Logger(`Could not transfer to ${replicateTo.dbName}: Send failed`);
            }
          } else {
            Logger(`Could not transfer to ${replicateTo.dbName}: Read failed`);
          }

        } catch (ex) {
          Logger(`Could not transfer to ${replicateTo.dbName}`);
          Logger(ex, LOG_LEVEL.VERBOSE);
        }
      }
      if (this.localBasePath) {

        const localPath = fromFileUrl("file://" + path.join(this.localBasePath, relativePath));
        const dir = dirname(localPath);
        await Deno.mkdir(dir, { recursive: true });
        const isFileExist = await fetchStatOnStorage(localPath);
        Logger(`${this.dbName}:${remotePath} fetch localPath`);
        const xxDoc = await this.localDatabase.getDBEntryMeta(remotePath, null, false);
        const isDeleted = doc._deleted || (xxDoc && "deleted" in xxDoc && xxDoc.deleted);
        if (isDeleted && isFileExist) {
          if (xxDoc == false) {
            this.fileProcessedMap.set(localPath, 0);
            scheduleTask("flush", 100, () => this.snapshotFileProcMap());
            await Deno.remove(localPath);
            Logger(`${localPath} deleted`);
          } else {
            setTimeout(() => this.fetchDoc(xxDoc), 10);
            scheduleTask("run-processor", 250, async () => await this.runProcessor());
          }
        } else if (isDeleted) {
          // Logger(`Tried to remove, but already not exist`);
        } else {
          let stat: undefined | Deno.FileInfo = undefined;
          try {
            stat = await Deno.stat(localPath);
          } catch (_ex) {
            //NO OP.
          }

          const storageMtime = ~~((stat?.mtime?.getTime() || 0) / 1000);
          if (storageMtime > docMtime) {
            Logger(`Skipped:${this.dbName}:${remotePath} is old. (${storageMtime} > ${docMtime})`);
            // conflicted?
          } else {
            // local is old or new 
            const xDoc = await this.localDatabase.getDBEntry(remotePath, doc._rev, false, false);
            if (xDoc === false) {
              Logger(`Skipped:${this.dbName}:${remotePath} could not retrieved, skipped`);
              return;
            }
            const writeData = "datatype" in xDoc && xDoc.datatype == "newnote" ? new Uint8Array(base64ToArrayBuffer(xDoc.data)) : writeString(getDocData(xDoc.data));
            const fn = localPath;
            this.fileProcessedMap.set(relativePath, docMtime);
            scheduleTask("flush", 100, () => this.snapshotFileProcMap());
            const f = await Deno.open(fn, { create: true, write: true, truncate: true });
            await f.write(writeData);
            f.close();
            utimesSync(fn, docMtime, docMtime);
            scheduleTask("run-processor", 250, async () => await this.runProcessor());
            Logger(`Done! ${this.dbName}:${remotePath} written`);
          }
        }
      }
    } catch (ex) {
      Logger("Failed to process transferred document");
      Logger(ex);
    }
  }
  async processReplication(e: PouchDB.Core.ExistingDocument<EntryDoc>[], seq?: string) {

    for (const doc of e) {
      //Note, do not await
      this.fetchDoc(doc);
    }
    this.status.since = seq || "";
    await this.flushStat();
  }
  async connectRemoteCouchDB(uri: string, auth: { username: string; password: string; }, _disableRequestURI: boolean, passphrase: string | boolean, useDynamicIterationCount: boolean): Promise<string | { db: PouchDB.Database<EntryDoc>; info: PouchDB.Core.DatabaseInfo; }> {
    const conf: PouchDB.HttpAdapter.HttpAdapterConfiguration = {
      adapter: "http",
      auth,
      fetch: async (url: string | Request, opts?: RequestInit) => {
        if (opts?.signal) {
          delete opts.signal;
        }
        let size = "";
        const localURL = url.toString().substring(uri.length);
        const method = opts?.method ?? "GET";
        if (opts?.body) {
          const opts_length = opts.body.toString().length;
          if (opts_length > 1000 * 1000 * 10) {
            // over 10MB
            if (isCloudantURI(uri)) {
              this.last_successful_post = false;
              Logger("This request should fail on IBM Cloudant.", LOG_LEVEL.VERBOSE);
              throw new Error("This request should fail on IBM Cloudant.");
            }
          }
          size = ` (${opts_length})`;
        }

        try {
          const response: Response = await fetch(url, opts);
          if (method == "POST" || method == "PUT") {
            this.last_successful_post = response.ok;
          } else {
            this.last_successful_post = true;
          }
          Logger(`HTTP:${method}${size} to:${localURL} -> ${response.status}`, LOG_LEVEL.DEBUG);
          return response;
        } catch (ex) {
          Logger(`HTTP:${method}${size} to:${localURL} -> failed`, LOG_LEVEL.VERBOSE);
          // limit only in bulk_docs.
          if (url.toString().indexOf("_bulk_docs") !== -1) {
            this.last_successful_post = false;
          }
          Logger(ex);
          throw ex;
        }
        // return await fetch(url, opts);
      },
    };

    const db: PouchDB.Database<EntryDoc> = new PouchDB<EntryDoc>(uri, conf);
    if (passphrase !== "false" && typeof passphrase === "string") {
      enableEncryption(db, passphrase, useDynamicIterationCount);
    }
    try {
      const info = await db.info();
      return { db: db, info: info };
      // deno-lint-ignore no-explicit-any
    } catch (ex: any) {
      let msg = `${ex.name}:${ex.message}`;
      if (ex.name == "TypeError" && ex.message == "Failed to fetch") {
        msg += "\n**Note** This error caused by many reasons. The only sure thing is you didn't touch the server.\nTo check details, open inspector.";
      }
      Logger(ex, LOG_LEVEL.VERBOSE);
      return msg;
    }
  }
  replicationStat = new ObservableStore({
    sent: 0,
    arrived: 0,
    maxPullSeq: 0,
    maxPushSeq: 0,
    lastSyncPullSeq: 0,
    lastSyncPushSeq: 0,
    syncStatus: "CLOSED" as DatabaseConnectingStatus
  });


  async openDatabase() {
    if (this.localDatabase != null) {
      await this.localDatabase.close();
    }
    const vaultName = this.dbName;
    Logger("Waiting for ready...");
    this.localDatabase = new LiveSyncLocalDB(vaultName, this);
    // this.observeForLogs();
    return await this.localDatabase.initializeDatabase();
  }

  async start(initialScan: boolean) {
    this.status = await loadStat(this.getProgressFilename(), statusDefault);
    this.status.lastProcessed = Object.fromEntries(this.fileProcessedMap.entries());

    this.fileProcessedMap = new Map<string, number>(Object.entries(this.status.lastProcessed));
    const snapShot = { ...this.status.lastProcessed };
    await this.openDatabase();
    const syncLocalFile = async (pathOnStorage: string) => {
      const stat = await fetchStatOnStorage(pathOnStorage);
      const mtime = ~~(((stat && stat.mtime?.getTime()) || 0) / 1000);
      const base = this.localBasePath ?? "";
      const path = normalizePath(relative(base, pathOnStorage));

      if ((!(path in snapShot) || snapShot[path] == 0)) {
        // resurrected
        this.onFileChanged("create", [pathOnStorage]);
      } else {
        console.log(`${path}, ${snapShot[path]} > ${mtime}`)
        if (snapShot[path] > mtime) {
          this.onFileChanged("modify", [pathOnStorage]);
        }
      }
    }
    if (initialScan && this.localBasePath) {
      for await (const entry of expandGlob(`**/*`, { root: this.localBasePath, includeDirs: false })) {
        syncLocalFile(entry.path);
      }
    }
    let timeout = Date.now() + 120 * 1000;

    setInterval(() => {
      const d = Date.now();
      Logger(`${~~((timeout - d) / 1000)} seconds to reconnect..`);
      if (timeout < d) {
        this.getReplicator()?.closeReplication();
        timeout = Date.now() + 120 * 1000;
      }
    }, 30000);
    this.syncMain();
  }
  async syncMain() {
    if (this.getReplicator().controller == null) {
      try {
        if (!await this.getReplicator().openStreaming(this.settings, this.status.since, true)) {
          if (this.getReplicator().remoteLockedAndDeviceNotAccepted) {
            Logger("Remote locking detected! rewind sync seq!");
            this.status.since = "";
            await this.flushStat();
            await this.getReplicator().markRemoteResolved(this.settings);
            setTimeout(() => this.syncMain(), 100);
            return;
          }
        }
      } catch (ex) {
        Logger(`Something had happen!`)
        Logger(ex);
      }
      await delay(6000);
      //possibly timed out.
      try {
        await this.openDatabase();
      } catch (ex) {
        Logger(`Something had happen!`)
        Logger(ex);
      }
      Logger("Disconnected!!");
    }
    setTimeout(() => this.syncMain(), 1000);
  }
}
