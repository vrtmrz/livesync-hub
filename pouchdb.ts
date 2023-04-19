// import "./lib/indexeddb_global.ts";
// import { configureSQLiteDB, createIndexedDB } from "https://deno.land/x/indexeddb@1.3.5/lib/shim.ts";

// configureSQLiteDB({ memory: true });
// createIndexedDB(true, true, "./dat/db");

// const pouchdb_src = require("pouchdb-core")
//     .plugin(require("pouchdb-find"))
//     .plugin(require("pouchdb-adapter-leveldb"))
//     .plugin(require("pouchdb-adapter-http"))
//     .plugin(require("pouchdb-mapreduce"))
//     .plugin(require("pouchdb-replication"))
//     .plugin(require("transform-pouch"));
// const PouchDB: PouchDB.Static<{}> = pouchdb_src;
// /**
//  * @type {PouchDB.Static<>}
//  */
// export { PouchDB };

import PouchDB from 'pouchdb-core';
// import adapter from "pouchdb-adapter-localstorage";
// import IDBPouch from 'pouchdb-adapter-idb';
import adapter from 'pouchdb-adapter-memory';
import HttpPouch from 'pouchdb-adapter-http';
import mapreduce from 'pouchdb-mapreduce';
import replication from 'pouchdb-replication';

import find from "pouchdb-find";
import transform from "transform-pouch";

PouchDB
    .plugin(adapter)
    .plugin(HttpPouch)
    .plugin(mapreduce)
    .plugin(replication)
    .plugin(find)
    .plugin(transform)

export { PouchDB };