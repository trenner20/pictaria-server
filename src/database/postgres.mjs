import { MessageChannel, receiveMessageOnPort, Worker } from 'node:worker_threads';

const TIMEOUT_MS = 35_000;

// The repositories use synchronous transactions. A dedicated connection in a
// worker keeps their transaction boundaries intact while PostgreSQL network
// I/O is serviced on the worker's event loop.
export class PostgresDatabase {
  constructor({ url, schema }, { readOnly = false, onDisconnect } = {}) {
    if (!['pictaria_enrich', 'pictaria_frame', 'pictaria_insights'].includes(schema)) {
      throw new Error('Invalid Pictaria PostgreSQL schema.');
    }
    this.dialect = 'postgres';
    this.cursorId = 0;
    this.signal = new Int32Array(new SharedArrayBuffer(4));
    const { port1, port2 } = new MessageChannel();
    this.port = port1;
    this.worker = new Worker(new URL('./postgres-worker.mjs', import.meta.url), {
      workerData: { url, schema, readOnly, signal: this.signal, port: port2 },
      transferList: [port2],
      execArgv: [],
    });
    const disconnected = () => {
      if (this.closed) return;
      this.dispose();
      onDisconnect?.();
    };
    this.worker.on('error', disconnected);
    this.worker.on('exit', disconnected);
    this.worker.on('message', disconnected);
    this.worker.unref();
    this.port.unref();
    try {
      this.request('open');
    } catch (error) {
      this.dispose();
      throw error;
    }
  }

  request(operation, sql, values) {
    if (this.closed) throw new Error('PostgreSQL connection is closed.');
    Atomics.store(this.signal, 0, 0);
    this.port.postMessage({ operation, sql, values });
    if (Atomics.wait(this.signal, 0, 0, TIMEOUT_MS) === 'timed-out') {
      this.dispose();
      throw new Error('PostgreSQL connection did not respond before the deadline.');
    }
    const response = receiveMessageOnPort(this.port)?.message;
    if (!response) {
      this.dispose();
      throw new Error('PostgreSQL worker returned no response.');
    }
    const { result, error } = response;
    if (error) throw Object.assign(new Error(error.message), { code: error.code });
    return result;
  }

  exec(sql) {
    this.request('exec', sql);
  }

  prepare(sql) {
    return {
      all: (...values) => this.request('all', sql, values),
      iterate: (...values) => this.iterate(sql, values),
      get: (...values) => this.request('get', sql, values),
      run: (...values) => this.request('run', sql, values),
    };
  }

  *iterate(sql, values) {
    // Both repository callers already own a transaction (startup
    // reconciliation or a repeatable-read grouping snapshot).
    const name = `pictaria_cursor_${++this.cursorId}`;
    this.request('declare', sql, [name, ...values]);
    try {
      for (;;) {
        const rows = this.request('native', `FETCH 128 FROM ${name}`);
        yield* rows;
        if (rows.length < 128) break;
      }
    } finally {
      this.request('native', `CLOSE ${name}`);
    }
  }

  close() {
    if (this.closed) return;
    try {
      this.request('close');
    } finally {
      this.dispose();
    }
  }

  dispose() {
    if (this.closed) return;
    this.closed = true;
    this.port.close();
    void this.worker.terminate();
  }
}
