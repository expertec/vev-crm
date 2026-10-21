// Preloaded only by explicit offline test commands; never by the application.
import net from 'node:net';
import http from 'node:http';
import https from 'node:https';
import dgram from 'node:dgram';
import dns from 'node:dns';
import { syncBuiltinESMExports } from 'node:module';
const denied = () => { throw new Error('NETWORK_DISABLED_DURING_PHASE0_TESTS'); };
globalThis.fetch = denied;
net.Socket.prototype.connect = denied;
net.connect = denied; net.createConnection = denied;
http.request = denied; http.get = denied; https.request = denied; https.get = denied;
dgram.createSocket = denied;
dns.lookup = denied; dns.resolve = denied;
dns.promises.lookup = denied; dns.promises.resolve = denied;
syncBuiltinESMExports();
