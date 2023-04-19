import { path2id_base, id2path_base, stripAllPrefixes } from "./lib/src/path.ts";
import { FilePath, FilePathWithPrefix, DocumentID, EntryHasPath, AnyEntry } from "./lib/src/types.ts";
import { fromFileUrl } from "./mod.ts";
import { ICHeaderLength, ICHeader, CHeader } from "./types.ts";

export async function fetchStatOnStorage(path: string) {
    try {
        return await Deno.stat(fromFileUrl("file://" + path));
    } catch (_ex) {
        return false;
    }
}


export function normalizePath(path: FilePath | string) {
    if (path.indexOf(":") !== -1) {
        return path;
    }
    return path.replace(/\\/g, "/").replace(/\/+/g, "/");
}

export async function path2id(filename: FilePathWithPrefix | FilePath, obfuscatePassphrase: string | false): Promise<DocumentID> {
    const temp = filename.split(":");
    const path = temp.pop();
    const normalizedPath = normalizePath(path as FilePath);
    temp.push(normalizedPath);
    const fixedPath = temp.join(":") as FilePathWithPrefix;

    const out = await path2id_base(fixedPath, obfuscatePassphrase);
    return out;
}

export function id2path(id: DocumentID, entry?: EntryHasPath): FilePathWithPrefix {
    const filename = id2path_base(id, entry);
    const temp = filename.split(":");
    const path = temp.pop();
    const normalizedPath = normalizePath(path as FilePath);
    temp.push(normalizedPath);
    const fixedPath = temp.join(":") as FilePathWithPrefix;
    return fixedPath;
}
export function getPath(entry: AnyEntry) {
    return id2path(entry._id, entry);

}
export function getPathWithoutPrefix(entry: AnyEntry) {
    const f = getPath(entry);
    return stripAllPrefixes(f);
}

export function stripInternalMetadataPrefix<T extends FilePath | FilePathWithPrefix | DocumentID>(id: T): T {
    return id.substring(ICHeaderLength) as T;
}
export function id2InternalMetadataId(id: DocumentID): DocumentID {
    return ICHeader + id as DocumentID;
}

// const CHeaderLength = CHeader.length;
export function isChunk(str: string): boolean {
    return str.startsWith(CHeader);
}

export function isIdOfInternalMetadata(id: FilePath | FilePathWithPrefix | DocumentID): boolean {
    return id.startsWith(ICHeader);
}

const tasks: { [key: string]: ReturnType<typeof setTimeout> } = {};
export function scheduleTask(key: string, timeout: number, proc: (() => Promise<any> | void)) {
    cancelTask(key);
    tasks[key] = setTimeout(async () => {
        delete tasks[key];
        await proc();
    }, timeout);
}
export function cancelTask(key: string) {
    if (key in tasks) {
        clearTimeout(tasks[key]);
        delete tasks[key];
    }
}
export function cancelAllTasks() {
    for (const v in tasks) {
        clearTimeout(tasks[v]);
        delete tasks[v];
    }
}
export async function saveStat<T>(path: string, obj: T) {
    await Deno.writeTextFile(path, JSON.stringify(obj));
}
export async function loadStat<T>(path: string, obj?: T) {
    let ret = obj ? { ...obj } : {} as T;
    try {
        const w = JSON.parse(await Deno.readTextFile(path));
        ret = { ...ret, ...w } as T;
    } catch (ex) {
        // NO OP
        console.dir(ex);
    }
    return ret;

}