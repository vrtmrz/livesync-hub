import { DEFAULT_SETTINGS, LOG_LEVEL, RemoteDBSettings } from "./lib/src/types.ts";
import { logStore } from "./lib/src/stores.ts";

import { configFile, statusDefault } from "./types.ts";
import { RemoteDBProc } from "./RemoteDBProc.ts";
import { loadStat } from "./utils.ts";

async function addLog(message: any, level: LOG_LEVEL = LOG_LEVEL.INFO, _key = "") {
  if (level == LOG_LEVEL.DEBUG) {
    return;
  }
  const now = new Date();
  const timestamp = now.toLocaleString();
  const messageContent = typeof message == "string" ? message : message instanceof Error ? `${message.name}:${message.message}` : JSON.stringify(message, null, 2);
  if (message instanceof Error) {
    // debugger;
    console.dir(message.stack);
  }
  const newMessage = timestamp + "->" + messageContent;
  console.log(newMessage);

}

async function main() {

  logStore.subscribe(e => addLog(e.message, e.level, e.key));

  const allConfigs = await loadStat<configFile>("./dat/config.json");
  const processors = [] as RemoteDBProc[];
  for (const [key, conf] of Object.entries(allConfigs)) {
    const progressFileName = `./dat/${key}-stat.json`;
    const progress = await loadStat(progressFileName, statusDefault);
    const dbConf: RemoteDBSettings = {
      ...DEFAULT_SETTINGS
    };
    dbConf.couchDB_DBNAME = conf.server.database;
    dbConf.couchDB_URI = conf.server.uri;
    dbConf.couchDB_USER = conf.server.auth.username;
    dbConf.couchDB_PASSWORD = conf.server.auth.password;
    dbConf.passphrase = conf.server.auth.passphrase ?? "";
    dbConf.usePathObfuscation = conf.server.usePathObfuscation ?? false;
    dbConf.encrypt = (conf.server.auth.passphrase ?? "") != "";
    dbConf.customChunkSize = conf.server.customChunkSize ?? 0;
    dbConf.readChunksOnline = true;
    // Open processor.
    
    const w = new RemoteDBProc(dbConf, key, conf.local?.path, conf.server.path, conf.group, conf.local?.processor, processors);
    processors.push(w);
    w.nodeId = progress.nodeId;
    w.start(conf.local?.initialScan??false);
  }
}


main();