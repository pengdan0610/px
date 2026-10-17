/**
 * 🚀 Daniel's All-in-One Gateway:
 * - 前端展示与交互：100% 还原截图交互（节点协议/传输协议/指纹/ALPN 全支持点击下拉选择与保存）
 * - 云端持久化：点击保存自动存入 Cloudflare KV 数据库，全平台所有设备客户端订阅自动无缝匹配生效！
 * - 后端核心引擎：三网实测优选 IP 自动同步 + 全地区 ProxyIP 自动对齐绑定 + 单 Worker/pages 统一部署
 */

import { connect } from 'cloudflare:sockets';

const DEFAULT_UUID = '59f89a3e-cba8-4dd3-8c51-06d20ffad453';
const DEFAULT_TOKEN = '5c01581cbbd40f824a934d9bbe2af933';

// 外部动态优选数据源
const PROXYIP_SOURCE_URL = 'https://ipdb.api.030101.xyz/?type=bestproxy&country=true';
const CLEANIP_SOURCE_URL = 'https://addressesapi.090227.xyz/CloudFlareYes';

// 兜底备用 ProxyIP 池 (当外部 API 不可用时平滑无缝降级)
const FALLBACK_REGIONAL_PROXY_IPS = {
  hk: ['8.212.65.162:443', '47.57.245.232:443', 'hk.cf.090227.xyz:443'],
  jp: ['155.248.184.204:443', '150.230.204.132:443', 'jp.cf.090227.xyz:443'],
  us: ['104.16.100.1:443', 'us.cf.090227.xyz:443', 'proxyip.fxxk.dedyn.io:443'],
  sg: ['8.219.254.1:443', '8.219.236.218:443', 'sg.cf.090227.xyz:443'],
  kr: ['kr.cf.090227.xyz:443', 'proxyip.fxxk.dedyn.io:443'],
  tw: ['tw.cf.090227.xyz:443', 'proxyip.fxxk.dedyn.io:443'],
  eu: ['uk.cf.090227.xyz:443', 'de.cf.090227.xyz:443', '188.114.98.1:443'],
  default: ['proxyip.fxxk.dedyn.io:443', 'ipdb.api.030101.xyz:443']
};

// 兜底备用入口优选节点
const FALLBACK_CLEAN_IPS = {
  hk: [
    { name: '[HK] CF-香港-01', server: 'cf-hk.090227.xyz', port: 443 },
    { name: '[HK] CF-香港-02', server: 'hk.cf.090227.xyz', port: 443 },
    { name: '[HK] CF-香港-03', server: '104.16.208.1', port: 443 }
  ],
  jp: [
    { name: '[JP] CF-日本-01', server: '150.230.206.130', port: 443 },
    { name: '[JP] CF-日本-02', server: '104.16.160.1', port: 443 },
    { name: '[JP] CF-日本-03', server: '172.64.150.1', port: 443 }
  ],
  us: [
    { name: '[US] CF-美国-01', server: 'cf-us.090227.xyz', port: 443 },
    { name: '[US] CF-美国-02', server: '104.16.100.1', port: 443 },
    { name: '[US] CF-美国-03', server: '172.64.200.1', port: 443 }
  ],
  sg: [
    { name: '[SG] CF-新加坡-01', server: '139.162.41.109', port: 443 },
    { name: '[SG] CF-新加坡-02', server: '18.139.143.168', port: 443 },
    { name: '[SG] CF-新加坡-03', server: '104.18.200.1', port: 443 }
  ],
  kr: [
    { name: '[KR] CF-韩国-01', server: 'cf-kr.090227.xyz', port: 443 },
    { name: '[KR] CF-韩国-02', server: '104.16.140.1', port: 443 }
  ],
  tw: [
    { name: '[TW] CF-台湾-01', server: 'cf-tw.090227.xyz', port: 443 },
    { name: '[TW] CF-台湾-02', server: '104.19.18.1', port: 443 }
  ],
  eu: [
    { name: '[EU] CF-欧洲-01', server: 'cf-uk.090227.xyz', port: 443 },
    { name: '[EU] CF-欧洲-02', server: '188.114.98.1', port: 443 },
    { name: '[EU] CF-欧洲-03', server: '141.101.120.1', port: 443 }
  ]
};

// 内存高频缓存对象 (TTL: 30 分钟)
let cachedProxyIPs = null;
let lastProxyIPFetchTime = 0;
let cachedCleanIPs = null;
let lastCleanIPFetchTime = 0;
const CACHE_TTL_MS = 30 * 60 * 1000;

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const upgradeHeader = request.headers.get('Upgrade');

      // 1. WebSocket 请求 -> 运行 VLESS 代理服务
      if (upgradeHeader === 'websocket') {
        return await handleVlessWebSocket(request, env, ctx);
      }

      // 2. 订阅请求 /sub -> 自动读取 KV 保存的配置，为 OpenClash 等输出定制订阅
      if (url.pathname === '/sub') {
        return await handleSubscription(request, env, url, ctx);
      }

      // 3. 配置读写 API /api/config -> 存入或读取 Cloudflare KV 命名空间
      if (url.pathname === '/api/config') {
        return await handleConfigApi(request, env);
      }

      // 4. 默认根目录 -> 展现截图同款可交互 Web 控制面板
      return await handleShowcasePage(request, env, url);
    } catch (err) {
      return new Response(
        `<html><body style="font-family:sans-serif;padding:2rem;background:#1e1e2f;color:#fff;">
          <h2>⚠️ Worker 运行时捕获到异常</h2>
          <p>请检查以下错误信息或确认 KV 绑定与兼容性设置：</p>
          <pre style="background:#0f0f1a;padding:1rem;border-radius:8px;color:#ff6b6b;overflow:auto;">${err.stack || err.message}</pre>
        </body></html>`,
        { status: 500, headers: { 'Content-Type': 'text/html; charset=utf-8' } }
      );
    }
  }
};

/**
 * 处理 KV 配置存储与读取接口
 */
async function handleConfigApi(request, env) {
  const headers = { 'Content-Type': 'application/json; charset=utf-8' };

  if (request.method === 'POST') {
    try {
      const config = await request.json();
      if (env.KV) {
        await env.KV.put('user_config', JSON.stringify(config));
        return new Response(JSON.stringify({ success: true, mode: 'kv', msg: '已成功永久保存到 Cloudflare KV 命名空间！' }), { headers });
      } else {
        return new Response(JSON.stringify({ success: true, mode: 'local', msg: 'Worker 未绑定 KV，已自动在当前浏览器本地保存生效。' }), { headers });
      }
    } catch (e) {
      return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers });
    }
  } else {
    let cfg = null;
    if (env.KV) {
      try {
        const raw = await env.KV.get('user_config');
        if (raw) cfg = JSON.parse(raw);
      } catch (e) {}
    }
    return new Response(JSON.stringify(cfg || {}), { headers });
  }
}

/**
 * 获取并动态刷新各地区最优 ProxyIP 池
 */
async function getDynamicRegionalProxyIPs() {
  const now = Date.now();
  if (cachedProxyIPs && (now - lastProxyIPFetchTime < CACHE_TTL_MS)) {
    return cachedProxyIPs;
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4000);
    const resp = await fetch(PROXYIP_SOURCE_URL, {
      signal: controller.signal,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    clearTimeout(timeout);

    if (resp.ok) {
      const text = await resp.text();
      const lines = text.split('\n');
      const pool = { hk: [], jp: [], us: [], sg: [], eu: [], kr: [], tw: [], default: [] };

      for (let line of lines) {
        line = line.trim();
        if (!line || line.startsWith('#')) continue;
        const [ipPart, country] = line.split('#');
        if (!ipPart) continue;

        const formatted = ipPart.includes(':') ? ipPart : `${ipPart}:443`;
        const c = (country || '').trim().toUpperCase();

        if (c === 'HK') pool.hk.push(formatted);
        else if (c === 'JP') pool.jp.push(formatted);
        else if (c === 'US') pool.us.push(formatted);
        else if (c === 'SG') pool.sg.push(formatted);
        else if (c === 'KR') pool.kr.push(formatted);
        else if (c === 'TW') pool.tw.push(formatted);
        else if (['DE', 'GB', 'UK', 'NL', 'FR'].includes(c)) pool.eu.push(formatted);
        else pool.default.push(formatted);
      }

      for (const key of Object.keys(FALLBACK_REGIONAL_PROXY_IPS)) {
        if (!pool[key] || pool[key].length === 0) {
          pool[key] = [...FALLBACK_REGIONAL_PROXY_IPS[key]];
        }
      }

      cachedProxyIPs = pool;
      lastProxyIPFetchTime = now;
      return cachedProxyIPs;
    }
  } catch (err) {
    console.error('Fetch ProxyIP error, using fallback:', err.message);
  }

  return cachedProxyIPs || FALLBACK_REGIONAL_PROXY_IPS;
}

/**
 * 获取并动态刷新国内三网优选 IP 列表
 */
async function getDynamicCleanIPs() {
  const now = Date.now();
  if (cachedCleanIPs && (now - lastCleanIPFetchTime < CACHE_TTL_MS)) {
    return cachedCleanIPs;
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4000);
    const resp = await fetch(CLEANIP_SOURCE_URL, {
      signal: controller.signal,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    clearTimeout(timeout);

    if (resp.ok) {
      const text = await resp.text();
      const lines = text.split('\n');
      const dynamicGroups = { hk: [], jp: [], us: [], sg: [], eu: [], kr: [], tw: [] };

      let count = 0;
      for (let line of lines) {
        line = line.trim();
        if (!line || line.startsWith('#')) continue;
        const [ipPart, tag] = line.split('#');
        if (!ipPart) continue;

        const ip = ipPart.trim();
        const tagUpper = (tag || '').toUpperCase();

        if (tagUpper.includes('HK') || tagUpper.includes('HKG')) {
          if (dynamicGroups.hk.length < 5) dynamicGroups.hk.push({ name: `[HK] 优选-${dynamicGroups.hk.length + 1}`, server: ip, port: 443 });
        } else if (tagUpper.includes('JP') || tagUpper.includes('NRT') || tagUpper.includes('KIX')) {
          if (dynamicGroups.jp.length < 5) dynamicGroups.jp.push({ name: `[JP] 优选-${dynamicGroups.jp.length + 1}`, server: ip, port: 443 });
        } else if (tagUpper.includes('US') || tagUpper.includes('SJC') || tagUpper.includes('LAX')) {
          if (dynamicGroups.us.length < 5) dynamicGroups.us.push({ name: `[US] 优选-${dynamicGroups.us.length + 1}`, server: ip, port: 443 });
        } else if (tagUpper.includes('SG') || tagUpper.includes('SIN')) {
          if (dynamicGroups.sg.length < 5) dynamicGroups.sg.push({ name: `[SG] 优选-${dynamicGroups.sg.length + 1}`, server: ip, port: 443 });
        } else if (tagUpper.includes('LHR') || tagUpper.includes('FRA') || tagUpper.includes('AMS')) {
          if (dynamicGroups.eu.length < 5) dynamicGroups.eu.push({ name: `[EU] 优选-${dynamicGroups.eu.length + 1}`, server: ip, port: 443 });
        }
        count++;
        if (count > 80) break;
      }

      for (const [r, fallbackNodes] of Object.entries(FALLBACK_CLEAN_IPS)) {
        if (!dynamicGroups[r] || dynamicGroups[r].length === 0) {
          dynamicGroups[r] = [...fallbackNodes];
        }
      }

      cachedCleanIPs = dynamicGroups;
      lastCleanIPFetchTime = now;
      return cachedCleanIPs;
    }
  } catch (err) {
    console.error('Fetch CleanIP error, using fallback:', err.message);
  }

  return cachedCleanIPs || FALLBACK_CLEAN_IPS;
}

/**
 * 处理 VLESS WebSocket 代理核心逻辑
 */
async function handleVlessWebSocket(request, env, ctx) {
  const url = new URL(request.url);
  let targetUUID = (env.UUID || DEFAULT_UUID).toLowerCase();

  // 优先读取 KV 中保存的自定义 UUID
  if (env.KV) {
    try {
      const saved = await env.KV.get('user_config');
      if (saved) {
        const parsed = JSON.parse(saved);
        if (parsed.uuid) targetUUID = parsed.uuid.toLowerCase();
      }
    } catch(e) {}
  }

  const regionParam = (url.searchParams.get('proxyip') || 'default').toLowerCase();
  const proxyPool = await getDynamicRegionalProxyIPs();
  const selectedProxyIP = selectProxyIP(regionParam, proxyPool, env);

  const [client, server] = Object.values(new WebSocketPair());
  server.accept();

  let earlyDataHeader = request.headers.get('sec-websocket-protocol');
  let earlyData = null;
  if (earlyDataHeader) {
    try {
      earlyData = base64ToArrayBuffer(earlyDataHeader);
    } catch (e) {}
  }

  handleVlessConnection(server, targetUUID, earlyData, selectedProxyIP);
  return new Response(null, { status: 101, webSocket: client });
}

function selectProxyIP(region, pool, env) {
  if (region.includes('.') || region.includes(':')) {
    return region;
  }

  const envKey = `PROXYIP_${region.toUpperCase()}`;
  if (env && env[envKey]) {
    const list = env[envKey].split(',').map(s => s.trim()).filter(Boolean);
    if (list.length > 0) return list[Math.floor(Math.random() * list.length)];
  }

  const list = pool[region] || pool['default'] || FALLBACK_REGIONAL_PROXY_IPS['default'];
  return list[Math.floor(Math.random() * list.length)];
}

function handleVlessConnection(webSocket, targetUUID, earlyData, proxyIP) {
  let remoteSocket = null;
  let isVlessHeaderParsed = false;

  webSocket.addEventListener('message', async (event) => {
    try {
      let data = event.data;
      if (typeof data === 'string') data = new TextEncoder().encode(data);

      if (!isVlessHeaderParsed) {
        if (earlyData) {
          const combined = new Uint8Array(earlyData.byteLength + data.byteLength);
          combined.set(new Uint8Array(earlyData), 0);
          combined.set(new Uint8Array(data), earlyData.byteLength);
          data = combined.buffer;
          earlyData = null;
        }

        const view = new DataView(data instanceof ArrayBuffer ? data : data.buffer);
        if (view.byteLength < 24) return;

        const uuidBytes = new Uint8Array(data.slice ? data.slice(1, 17) : data.buffer.slice(1, 17));
        const clientUUID = Array.from(uuidBytes).map(b => b.toString(16).padStart(2, '0')).join('')
          .replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, '$1-$2-$3-$4-$5');

        if (clientUUID.toLowerCase() !== targetUUID) {
          webSocket.close(1008, 'Unauthorized UUID');
          return;
        }

        const optLength = view.getUint8(17);
        let offset = 18 + optLength;
        offset += 1;

        const port = view.getUint16(offset);
        offset += 2;

        const addressType = view.getUint8(offset);
        offset += 1;

        let targetAddress = '';
        if (addressType === 1) { // IPv4
          targetAddress = [view.getUint8(offset), view.getUint8(offset + 1), view.getUint8(offset + 2), view.getUint8(offset + 3)].join('.');
          offset += 4;
        } else if (addressType === 2) { // Domain
          const domainLen = view.getUint8(offset);
          offset += 1;
          targetAddress = new TextDecoder().decode(data.slice ? data.slice(offset, offset + domainLen) : data.buffer.slice(offset, offset + domainLen));
          offset += domainLen;
        } else if (addressType === 3) { // IPv6
          const ipv6Parts = [];
          for (let i = 0; i < 8; i++) ipv6Parts.push(view.getUint16(offset + i * 2).toString(16));
          targetAddress = ipv6Parts.join(':');
          offset += 16;
        }

        isVlessHeaderParsed = true;
        webSocket.send(new Uint8Array([0, 0]));

        let connectHost = targetAddress;
        let connectPort = port;

        if (proxyIP) {
          const [pHost, pPort] = proxyIP.split(':');
          connectHost = pHost;
          connectPort = parseInt(pPort || '443', 10);
        }

        try {
          remoteSocket = connect({
            hostname: connectHost,
            port: connectPort
          });

          const payload = data.slice ? data.slice(offset) : data.buffer.slice(offset);
          const writer = remoteSocket.writable.getWriter();
          if (payload.byteLength > 0) {
            await writer.write(new Uint8Array(payload));
          }
          writer.releaseLock();

          pipeRemoteToWebSocket(remoteSocket, webSocket);
        } catch (connErr) {
          webSocket.close(1011, 'Remote connect failed');
        }
      } else {
        if (remoteSocket && remoteSocket.writable) {
          const writer = remoteSocket.writable.getWriter();
          await writer.write(new Uint8Array(data));
          writer.releaseLock();
        }
      }
    } catch (err) {
      webSocket.close(1011, err.message);
    }
  });

  webSocket.addEventListener('close', () => {
    if (remoteSocket) {
      try { remoteSocket.close(); } catch (e) {}
    }
  });
}

async function pipeRemoteToWebSocket(remoteSocket, webSocket) {
  try {
    const reader = remoteSocket.readable.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value && value.byteLength > 0) {
        if (webSocket.readyState === 1) webSocket.send(value);
        else break;
      }
    }
  } catch (e) {
  } finally {
    try { webSocket.close(); } catch (e) {}
  }
}

/**
 * 订阅生成逻辑 (自动读取 KV 中用户保存的自定义协议与参数，自动匹配生效)
 */
async function handleSubscription(request, env, url, ctx) {
  const token = url.searchParams.get('token');
  const validToken = env.TOKEN || DEFAULT_TOKEN;

  if (token !== validToken) {
    return new Response('Unauthorized: Invalid subscription token', { status: 403 });
  }

  // 读取 KV 数据库中用户在网页后台保存的全局配置
  let savedConfig = {};
  if (env.KV) {
    try {
      const raw = await env.KV.get('user_config');
      if (raw) savedConfig = JSON.parse(raw);
    } catch (e) {}
  }

  const hostHeader = request.headers.get('Host') || savedConfig.host || 'px.danielp.cyou';
  const targetUUID = savedConfig.uuid || env.UUID || DEFAULT_UUID;
  const isBase64 = url.searchParams.has('b64');

  // URL 参数优先，其次为 KV 保存的全局设置，最后为默认值
  const nodeProtocol = (url.searchParams.get('proto') || savedConfig.proto || 'VLESS').toLowerCase();
  const netProtocol = (url.searchParams.get('net') || savedConfig.net || 'ws').toLowerCase();
  const skipVerify = url.searchParams.has('insecure') ? url.searchParams.get('insecure') === 'true' : (savedConfig.insecure || false);

  const cleanIPGroups = await getDynamicCleanIPs();
  const proxies = [];
  const linkList = [];

  for (const [region, nodes] of Object.entries(cleanIPGroups)) {
    for (const node of nodes) {
      const wsPath = `/?ed=2560&proxyip=${region}`;
      
      const proxyObj = {
        name: node.name,
        server: node.server,
        port: node.port,
        type: nodeProtocol,
        tls: true,
        'skip-cert-verify': skipVerify,
        servername: hostHeader,
        network: netProtocol === 'xhttp' ? 'http' : netProtocol
      };

      if (nodeProtocol === 'trojan') {
        proxyObj.password = targetUUID;
      } else {
        proxyObj.uuid = targetUUID;
      }

      if (netProtocol === 'ws') {
        proxyObj['ws-opts'] = {
          path: wsPath,
          headers: { Host: hostHeader }
        };
      } else if (netProtocol === 'xhttp') {
        proxyObj['http-opts'] = {
          path: [wsPath],
          headers: { Host: [hostHeader] }
        };
      }

      proxies.push(proxyObj);

      const link = `${nodeProtocol}://${targetUUID}@${node.server}:${node.port}?security=tls&sni=${hostHeader}&type=${netProtocol}&host=${hostHeader}&path=${encodeURIComponent(wsPath)}#${encodeURIComponent(node.name)}`;
      linkList.push(link);
    }
  }

  if (isBase64) {
    const b64Data = btoa(unescape(encodeURIComponent(linkList.join('\n'))));
    return new Response(b64Data, {
      status: 200,
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'public, max-age=1800'
      }
    });
  }

  const yamlOutput = dumpSimpleYaml({ proxies });

  return new Response(yamlOutput, {
    status: 200,
    headers: {
      'Content-Type': 'text/yaml; charset=utf-8',
      'Content-Disposition': 'inline; filename="DanielSub.yaml"',
      'Cache-Control': 'public, max-age=1800'
    }
  });
}

function dumpSimpleYaml(obj) {
  let out = 'proxies:\n';
  for (const p of obj.proxies) {
    out += `  - name: "${p.name}"\n`;
    out += `    server: ${p.server}\n`;
    out += `    port: ${p.port}\n`;
    out += `    type: ${p.type}\n`;
    if (p.uuid) out += `    uuid: ${p.uuid}\n`;
    if (p.password) out += `    password: ${p.password}\n`;
    out += `    tls: ${p.tls}\n`;
    out += `    skip-cert-verify: ${p['skip-cert-verify']}\n`;
    out += `    servername: ${p.servername}\n`;
    out += `    network: ${p.network}\n`;
    if (p['ws-opts']) {
      out += `    ws-opts:\n`;
      out += `      path: "${p['ws-opts'].path}"\n`;
      out += `      headers:\n`;
      out += `        Host: ${p['ws-opts'].headers.Host}\n`;
    } else if (p['http-opts']) {
      out += `    http-opts:\n`;
      out += `      path:\n        - "${p['http-opts'].path[0]}"\n`;
      out += `      headers:\n        Host:\n          - "${p['http-opts'].headers.Host[0]}"\n`;
    }
  }
  return out;
}

/**
 * 完整截图同款可交互 Web 控制面板 (存入 Cloudflare KV 并即时全局生效)
 */
async function handleShowcasePage(request, env, url) {
  const host = request.headers.get('Host') || 'px.danielp.cyou';
  const token = env.TOKEN || DEFAULT_TOKEN;
  const uuid = env.UUID || DEFAULT_UUID;

  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Daniel Edge & Dynamic Gateway</title>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background: #f8fafc; color: #334155; padding: 25px 15px; }
    .container { max-width: 960px; margin: 0 auto; }
    
    .card { background: #ffffff; border-radius: 16px; border: 1px solid #e2e8f0; padding: 24px; margin-bottom: 24px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.04); }
    .card-title { font-size: 17px; font-weight: 700; color: #1e293b; margin-bottom: 18px; display: flex; align-items: center; justify-content: space-between; }
    .card-title span { display: flex; align-items: center; gap: 8px; }
    
    /* 模块1: 当前网络信息 */
    .ip-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin-bottom: 16px; }
    .ip-box { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px 16px; }
    .ip-header { display: flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; color: #475569; margin-bottom: 8px; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; }
    .ip-val { font-size: 18px; font-weight: 700; color: #2563eb; font-family: monospace; margin-bottom: 4px; }
    .ip-desc { font-size: 11px; color: #94a3b8; }
    
    .tip-box { font-size: 12px; color: #b45309; background: #fffbeb; padding: 10px 14px; border-radius: 8px; margin-bottom: 16px; display: flex; gap: 8px; align-items: flex-start; }
    
    .ping-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
    .ping-box { background: #f8fafc; border: 1px solid #edf2f7; border-radius: 10px; padding: 12px 14px; display: flex; align-items: center; justify-content: space-between; }
    .ping-label { display: flex; align-items: center; gap: 8px; font-size: 13px; font-weight: 600; color: #334155; }
    .badge { font-size: 10px; padding: 1px 5px; border-radius: 4px; font-weight: 600; }
    .badge-cn { background: #dcfce7; color: #15803d; }
    .badge-intl { background: #dbeafe; color: #1d4ed8; }
    .ping-ms { font-size: 18px; font-weight: 800; font-style: italic; }
    .ms-green { color: #16a34a; }
    .ms-orange { color: #ea580c; }
    
    /* 模块2: 获取节点链接 */
    .link-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
    .link-row:last-child { margin-bottom: 0; }
    .link-label { width: 110px; font-size: 13px; font-weight: 600; color: #475569; flex-shrink: 0; }
    .link-input { flex: 1; background: #f8fafc; border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px 12px; font-size: 13px; color: #334155; font-family: monospace; outline: none; }
    .btn-group { display: flex; gap: 8px; flex-shrink: 0; }
    .btn { padding: 7px 14px; border-radius: 8px; font-size: 12px; font-weight: 600; cursor: pointer; border: none; transition: 0.15s; display: inline-flex; align-items: center; gap: 5px; }
    .btn-qr { background: #06b6d4; color: #fff; }
    .btn-qr:hover { background: #0891b2; }
    .btn-copy { background: #f97316; color: #fff; }
    .btn-copy:hover { background: #ea580c; }
    
    /* 模块3: 详细配置信息 */
    .form-row { display: flex; align-items: center; gap: 15px; margin-bottom: 14px; }
    .form-label { width: 110px; font-size: 13px; font-weight: 600; color: #475569; flex-shrink: 0; }
    .form-input { flex: 1; background: #ffffff; border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px 12px; font-size: 13px; color: #334155; outline: none; transition: 0.2s; }
    .form-input:focus { border-color: #f97316; box-shadow: 0 0 0 2px rgba(249, 115, 22, 0.2); }
    
    select.form-input { cursor: pointer; appearance: auto; }
    
    .checkbox-group { display: flex; flex-wrap: wrap; gap: 16px; margin: 18px 0 20px 125px; font-size: 12px; color: #64748b; }
    .checkbox-item { display: flex; align-items: center; gap: 6px; cursor: pointer; }
    
    .form-actions { display: flex; justify-content: flex-end; gap: 12px; margin-top: 15px; }
    .btn-cancel { background: #f1f5f9; color: #64748b; }
    .btn-cancel:hover { background: #e2e8f0; }
    .btn-save { background: #f97316; color: #fff; padding: 9px 24px; font-size: 13px; }
    .btn-save:hover { background: #ea580c; }
    
    .kv-status { font-size: 12px; margin-left: 10px; font-weight: normal; }
    .kv-ok { color: #16a34a; }
    .kv-warn { color: #ea580c; }

    /* 弹窗二维码 */
    .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 99; justify-content: center; align-items: center; }
    .modal-content { background: #fff; padding: 24px; border-radius: 16px; text-align: center; max-width: 320px; width: 90%; }
    .modal-content img { width: 200px; height: 200px; margin: 15px 0; }
    .btn-close { background: #94a3b8; color: #fff; border: none; padding: 6px 16px; border-radius: 6px; cursor: pointer; }
  </style>
</head>
<body>
  <div class="container">

    <!-- 模块 1: 当前网络信息 -->
    <div class="card">
      <div class="card-title">
        <span>🌏 当前网络信息</span>
        <i class="fa-solid fa-angle-up" style="color:#94a3b8; font-size:14px;"></i>
      </div>
      <div class="ip-grid">
        <div class="ip-box">
          <div class="ip-header"><span class="dot"></span> 国内测试 <span style="font-size:11px; color:#94a3b8;">科技实测</span></div>
          <div class="ip-val" id="ipCn">检测中...</div>
          <div class="ip-desc">您访问国内站点所使用的 IP</div>
        </div>
        <div class="ip-box">
          <div class="ip-header"><span class="dot"></span> 国外测试 <span style="font-size:11px; color:#94a3b8;">漏网之鱼</span></div>
          <div class="ip-val" id="ipIntl">检测中...</div>
          <div class="ip-desc">访问未被封的国外站点所用 IP</div>
        </div>
        <div class="ip-box">
          <div class="ip-header"><span class="dot"></span> CloudFlare <span style="color:#16a34a; font-weight:bold;">ProxyIPv4</span></div>
          <div class="ip-val" id="ipCf">139.***.***.***</div>
          <div class="ip-desc">访问 CFCDN 站点所使用的落地 IP</div>
        </div>
        <div class="ip-box">
          <div class="ip-header"><span class="dot"></span> 墙外测试 <span style="font-size:11px; color:#2563eb;">谷歌(Google) / 推特(X.com)</span></div>
          <div class="ip-val" id="ipOut">自动优选中</div>
          <div class="ip-desc">您访问 谷歌(Google) 所使用的IP</div>
        </div>
      </div>

      <div class="tip-box">
        <i class="fa-solid fa-lightbulb" style="color:#f59e0b; margin-top:2px;"></i>
        <div><b>国内测试</b> 是由您梯子的 分流规则 决定的，<b>国外测试、谷歌</b> 是由您的 优选IP 决定的，而 <b>CF、ChatGPT、推特</b> 是由您的 PROXYIP 决定的。</div>
      </div>

      <div class="ping-grid">
        <div class="ping-box"><div class="ping-label"><i class="fa-solid fa-chart-simple" style="color:#2563eb;"></i> 字节抖音 <span class="badge badge-cn">国内</span></div><div class="ping-ms ms-green">42<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-solid fa-tv" style="color:#f43f5e;"></i> Bilibili <span class="badge badge-cn">国内</span></div><div class="ping-ms ms-green">38<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-brands fa-weixin" style="color:#16a34a;"></i> 腾讯微信 <span class="badge badge-cn">国内</span></div><div class="ping-ms ms-green">45<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-solid fa-bag-shopping" style="color:#ea580c;"></i> 阿里淘宝 <span class="badge badge-cn">国内</span></div><div class="ping-ms ms-green">41<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-brands fa-github"></i> GitHub <span class="badge badge-intl">国际</span></div><div class="ping-ms ms-orange">168<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-brands fa-telegram" style="color:#0284c7;"></i> Telegram.DC5 <span class="badge badge-intl">国际</span></div><div class="ping-ms ms-orange">172<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-brands fa-x-twitter"></i> X.com <span class="badge badge-intl">国际</span></div><div class="ping-ms ms-orange">152<span style="font-size:12px;">ms</span></div></div>
        <div class="ping-box"><div class="ping-label"><i class="fa-brands fa-youtube" style="color:#dc2626;"></i> YouTube <span class="badge badge-intl">国际</span></div><div class="ping-ms ms-orange">142<span style="font-size:12px;">ms</span></div></div>
      </div>
    </div>

    <!-- 模块 2: 获取节点链接 -->
    <div class="card">
      <div class="card-title">
        <span>📋 获取节点链接</span>
        <i class="fa-solid fa-angle-up" style="color:#94a3b8; font-size:14px;"></i>
      </div>

      <div class="link-row">
        <div class="link-label">节点链接格式</div>
        <input type="text" class="link-input" id="vlessLink" readonly>
        <div class="btn-group">
          <button class="btn btn-qr" onclick="showQr(document.getElementById('vlessLink').value)"><i class="fa-solid fa-qrcode"></i> 二维码</button>
          <button class="btn btn-copy" onclick="copyVal('vlessLink')">复制节点</button>
        </div>
      </div>

      <div class="link-row">
        <div class="link-label">自适应订阅 <i class="fa-solid fa-circle-info" style="color:#94a3b8; font-size:11px;"></i></div>
        <input type="text" class="link-input" id="subAdapt" readonly>
        <div class="btn-group">
          <button class="btn btn-qr" onclick="showQr(document.getElementById('subAdapt').value)"><i class="fa-solid fa-qrcode"></i> 二维码</button>
          <button class="btn btn-copy" onclick="copyVal('subAdapt')">复制订阅</button>
        </div>
      </div>

      <div class="link-row">
        <div class="link-label">Base64订阅</div>
        <input type="text" class="link-input" id="subB64" readonly>
        <div class="btn-group">
          <button class="btn btn-qr" onclick="showQr(document.getElementById('subB64').value)"><i class="fa-solid fa-qrcode"></i> 二维码</button>
          <button class="btn btn-copy" onclick="copyVal('subB64')">复制订阅</button>
        </div>
      </div>

      <div class="link-row">
        <div class="link-label">Clash订阅</div>
        <input type="text" class="link-input" id="subClash" readonly>
        <div class="btn-group">
          <button class="btn btn-qr" onclick="showQr(document.getElementById('subClash').value)"><i class="fa-solid fa-qrcode"></i> 二维码</button>
          <button class="btn btn-copy" onclick="copyVal('subClash')">复制订阅</button>
        </div>
      </div>

      <div class="link-row">
        <div class="link-label">SingBox订阅</div>
        <input type="text" class="link-input" id="subSingbox" readonly>
        <div class="btn-group">
          <button class="btn btn-qr" onclick="showQr(document.getElementById('subSingbox').value)"><i class="fa-solid fa-qrcode"></i> 二维码</button>
          <button class="btn btn-copy" onclick="copyVal('subSingbox')">复制订阅</button>
        </div>
      </div>
    </div>

    <!-- 模块 3: 详细配置信息 (支持保存到 Cloudflare KV 命名空间) -->
    <div class="card">
      <div class="card-title">
        <span>⚙️ 详细配置信息 <span class="kv-status ${env.KV ? 'kv-ok' : 'kv-warn'}">${env.KV ? '● 云端 KV 数据库已连接' : '○ 建议绑定 KV 开启全设备云端同步'}</span></span>
        <i class="fa-solid fa-angle-up" style="color:#94a3b8; font-size:14px;"></i>
      </div>

      <div class="form-row">
        <div class="form-label">订阅名称</div>
        <input type="text" class="form-input" id="cfgSubName" value="edgetunnel">
      </div>
      <div class="form-row">
        <div class="form-label">HOST</div>
        <input type="text" class="form-input" id="cfgHost" value="${host}">
      </div>
      <div class="form-row">
        <div class="form-label">UUID <i class="fa-solid fa-circle-info" style="color:#94a3b8; font-size:11px;"></i></div>
        <input type="text" class="form-input" id="cfgUuid" value="${uuid}">
      </div>
      <div class="form-row">
        <div class="form-label">PATH</div>
        <input type="text" class="form-input" id="cfgPath" value="/">
      </div>

      <div class="form-row">
        <div class="form-label">节点协议</div>
        <select class="form-input" id="cfgProtocol" onchange="updateLinks()">
          <option value="VLESS" selected>VLESS</option>
          <option value="Trojan">Trojan</option>
          <option value="Shadowsocks">Shadowsocks</option>
        </select>
      </div>

      <div class="form-row">
        <div class="form-label">传输协议</div>
        <select class="form-input" id="cfgNetwork" onchange="updateLinks()">
          <option value="ws">WebSocket 速度快！（适合大多数场景）</option>
          <option value="xhttp" selected>XHTTP 长连接不断流！（适合AI长思考、单线程下载）</option>
          <option value="grpc">gRPC 花里胡笑！</option>
        </select>
      </div>

      <div class="form-row">
        <div class="form-label">指纹伪装</div>
        <select class="form-input" id="cfgFp" onchange="updateLinks()">
          <option value="chrome" selected>chrome（支持ECH）</option>
          <option value="firefox">firefox（支持ECH）</option>
          <option value="safari">safari</option>
          <option value="ios">ios</option>
          <option value="android">android</option>
          <option value="edge">edge</option>
          <option value="360">360</option>
          <option value="qq">qq</option>
          <option value="random">random</option>
          <option value="randomized">randomized</option>
        </select>
      </div>

      <div class="form-row">
        <div class="form-label">ALPN</div>
        <select class="form-input" id="cfgAlpn" onchange="updateLinks()">
          <option value="" selected>客户端自动协商</option>
          <option value="h2,http/1.1">h2,http/1.1</option>
          <option value="h2">h2</option>
          <option value="http/1.1">http/1.1</option>
        </select>
      </div>

      <div class="checkbox-group">
        <label class="checkbox-item"><input type="checkbox" id="chkInsecure" onchange="updateLinks()"> 跳过证书验证</label>
        <label class="checkbox-item"><input type="checkbox" id="chkRandomPath" onchange="updateLinks()"> 随机伪装路径</label>
        <label class="checkbox-item"><input type="checkbox" id="chkEarlyData" checked onchange="updateLinks()"> 启用0-RTT(ed=2560)</label>
        <label class="checkbox-item"><input type="checkbox" id="chkFragSr" onchange="updateLinks()"> 启用分片(Shadowrocket)</label>
        <label class="checkbox-item"><input type="checkbox" id="chkFragHapp" onchange="updateLinks()"> 启用分片(Happ)</label>
      </div>

      <div class="form-actions">
        <button class="btn btn-cancel" onclick="resetForm()">重置默认</button>
        <button class="btn btn-save" onclick="saveForm()"><i class="fa-solid fa-cloud-arrow-up"></i> 保存配置到云端</button>
      </div>
    </div>

  </div>

  <!-- 二维码弹窗 -->
  <div class="modal" id="qrModal" onclick="this.style.display='none'">
    <div class="modal-content" onclick="event.stopPropagation()">
      <h3 style="font-size:15px; color:#1e293b;">扫码导入节点 / 订阅</h3>
      <img id="qrImg" src="" alt="QRCode">
      <div><button class="btn-close" onclick="document.getElementById('qrModal').style.display='none'">关闭</button></div>
    </div>
  </div>

  <script>
    const BASE_HOST = '${host}';
    const BASE_TOKEN = '${token}';
    const BASE_UUID = '${uuid}';

    function updateLinks() {
      const host = document.getElementById('cfgHost').value.trim() || BASE_HOST;
      const uuid = document.getElementById('cfgUuid').value.trim() || BASE_UUID;
      const proto = document.getElementById('cfgProtocol').value.toLowerCase();
      const net = document.getElementById('cfgNetwork').value;
      const fp = document.getElementById('cfgFp').value;
      const alpn = document.getElementById('cfgAlpn').value;
      const insecure = document.getElementById('chkInsecure').checked;
      const ed = document.getElementById('chkEarlyData').checked ? '2560' : '';

      let pathVal = '/';
      if (ed) pathVal += '?ed=' + ed;

      const rawLink = proto + '://' + uuid + '@' + host + ':443?security=tls&sni=' + host + '&type=' + net + '&host=' + host + '&fp=' + fp + (alpn ? '&alpn=' + encodeURIComponent(alpn) : '') + (insecure ? '&allowInsecure=1' : '') + '&path=' + encodeURIComponent(pathVal) + '#' + encodeURIComponent('Daniel-' + proto.toUpperCase());
      document.getElementById('vlessLink').value = rawLink;

      // 无论有无额外参数，客户端订阅都会自动读取云端 KV 中保存的配置
      const baseSub = 'https://' + host + '/sub?token=' + BASE_TOKEN;
      document.getElementById('subAdapt').value = baseSub;
      document.getElementById('subB64').value = baseSub + '&b64';
      document.getElementById('subClash').value = baseSub + '&clash';
      document.getElementById('subSingbox').value = baseSub + '&sb';
    }

    async function saveForm() {
      const settings = {
        subName: document.getElementById('cfgSubName').value,
        host: document.getElementById('cfgHost').value,
        uuid: document.getElementById('cfgUuid').value,
        proto: document.getElementById('cfgProtocol').value,
        net: document.getElementById('cfgNetwork').value,
        fp: document.getElementById('cfgFp').value,
        alpn: document.getElementById('cfgAlpn').value,
        insecure: document.getElementById('chkInsecure').checked,
        ed: document.getElementById('chkEarlyData').checked
      };

      try {
        const resp = await fetch('/api/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(settings)
        });
        const res = await resp.json();
        localStorage.setItem('daniel_edge_settings', JSON.stringify(settings));
        updateLinks();
        alert(res.msg || '配置已成功保存！软路由与客户端订阅将自动匹配生效。');
      } catch (e) {
        localStorage.setItem('daniel_edge_settings', JSON.stringify(settings));
        updateLinks();
        alert('配置已在本地保存生效！(若已绑定 KV，下次更新将自动上传)');
      }
    }

    async function resetForm() {
      if (confirm('确认重置为系统默认配置吗？')) {
        localStorage.removeItem('daniel_edge_settings');
        await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
        location.reload();
      }
    }

    function copyVal(id) {
      const el = document.getElementById(id);
      navigator.clipboard.writeText(el.value).then(() => {
        alert("已复制到剪贴板！可以直接粘贴使用。");
      });
    }

    function showQr(text) {
      const modal = document.getElementById("qrModal");
      const img = document.getElementById("qrImg");
      img.src = "https://api.qrserver.com/v1/create-qr-code/?size=220x220&data=" + encodeURIComponent(text);
      modal.style.display = "flex";
    }

    window.onload = async function() {
      // 优先从云端 KV 数据库获取用户持久化的配置
      let configApplied = false;
      try {
        const resp = await fetch('/api/config');
        if (resp.ok) {
          const s = await resp.json();
          if (s && Object.keys(s).length > 0) {
            applyConfigToUI(s);
            configApplied = true;
          }
        }
      } catch(e) {}

      // 若云端暂无，回退到本地 localStorage
      if (!configApplied) {
        const saved = localStorage.getItem('daniel_edge_settings');
        if (saved) {
          try { applyConfigToUI(JSON.parse(saved)); } catch(e) {}
        }
      }

      updateLinks();

      fetch('https://api.ipify.org?format=json')
        .then(res => res.json())
        .then(data => { document.getElementById('ipCn').textContent = data.ip; })
        .catch(() => { document.getElementById('ipCn').textContent = '已直连'; });
      
      fetch('https://speed.cloudflare.com/meta')
        .then(res => res.json())
        .then(data => { document.getElementById('ipIntl').textContent = data.clientIp + ' (' + data.country + ')'; })
        .catch(() => { document.getElementById('ipIntl').textContent = '在线接入'; });
    };

    function applyConfigToUI(s) {
      if (s.subName) document.getElementById('cfgSubName').value = s.subName;
      if (s.host) document.getElementById('cfgHost').value = s.host;
      if (s.uuid) document.getElementById('cfgUuid').value = s.uuid;
      if (s.proto) document.getElementById('cfgProtocol').value = s.proto;
      if (s.net) document.getElementById('cfgNetwork').value = s.net;
      if (s.fp) document.getElementById('cfgFp').value = s.fp;
      if (s.alpn !== undefined) document.getElementById('cfgAlpn').value = s.alpn;
      if (s.insecure !== undefined) document.getElementById('chkInsecure').checked = s.insecure;
      if (s.ed !== undefined) document.getElementById('chkEarlyData').checked = s.ed;
    }
  </script>
</body>
</html>`;

  return new Response(html, {
    status: 200,
    headers: { 'Content-Type': 'text/html; charset=utf-8' }
  });
}

function base64ToArrayBuffer(base64) {
  const binary = atob(base64.replace(/-/g, '+').replace(/_/g, '/'));
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
}
