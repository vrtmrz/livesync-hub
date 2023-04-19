# LiveSync-hub

The synchronization daemon between filesystem and CouchDB compatible with [Self-hosted LiveSync](https://github.com/vrtmrz/obsidian-livesync); The combined one of LiveSync-classroom and filesystem-LiveSync.


## How to run

```sh
git clone --recursive https://github.com/vrtmrz/livesync-hub
cp dat/config.sample.json dat/config.json
# Setting up configuration
vi dat/config.json
deno run -A main.ts
```




## Configuration

The configuration file consists of the following structure.

**Work in progress!**