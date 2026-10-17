// Offline browser contract: no device, network, audio or token output.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname, '../static/phase5-aec.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

async function scenario(status, token = 'test-only', id = 'test-device') {
  const nodes = {device: {value: id}, download: {}, status: {textContent: ''}};
  const calls = [], downloads = [];
  const context = { URL: {createObjectURL: () => 'blob:test', revokeObjectURL() {}},
    document: {getElementById: id => nodes[id], createElement: () => ({click() {downloads.push(this.download);}})},
    localStorage: {getItem: () => token}, setTimeout: () => 1, clearTimeout() {},
    AbortController, fetch: async (url, options) => { calls.push({url, options});
      return {ok: status === 200, status, blob: async () => ({size: 100})}; } };
  vm.runInNewContext(script, context);
  assert.equal(calls.length, 0, 'loading must never export');
  await nodes.download.onclick();
  return {nodes, calls, downloads};
}
(async () => {
  let result = await scenario(200);
  assert.equal(result.calls.length, 1);
  assert.equal(result.calls[0].url, '../api/phase5/aec/test-device');
  assert.equal(result.calls[0].options.headers.Authorization, 'Bearer test-only');
  assert.equal(result.downloads[0], 'phase5-aec.local.zip');
  for (const status of [401, 403, 409, 502]) {
    result = await scenario(status);
    assert.equal(result.calls.length, 1, 'no automatic retry');
    assert.equal(result.downloads.length, 0, 'failure must not download');
    assert.equal(result.nodes.download.disabled, false);
  }
  for (const [token, id] of [['', 'test-device'], ['test-only', '../other']]) {
    result = await scenario(200, token, id);
    assert.equal(result.calls.length, 0);
  }
  console.log('PASS: explicit authenticated download, no automatic request/retry, failures and invalid input blocked');
})().catch(error => {console.error(error); process.exitCode = 1;});
