// =============================================
// 闲酌序·沾花局 — 剧本杀实时同步服务器
// Express + Socket.IO
// =============================================

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 10000,
  pingTimeout: 5000
});

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(__dirname, '.game_state.json');

// =============================================
// 游戏状态（服务端权威数据源）
// =============================================
let gameState = {
  maxAct: 1,
  taken: {},           // { roleId: socketId }
  players: {},         // { roleId: { act, silver, socketId } }
  hostActions: {
    silverGiven: {},   // { roleId: amount }
    cluesGiven: {},    // { roleId: [clueId, ...] }
    skillsGiven: {},   // { roleId: [skillName, ...] }
  },
  hostConnected: false,
  lastPush: null,
  gameStarted: false
};

// 从文件恢复状态
function loadStateFromFile() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
      gameState = { ...gameState, ...data };
      console.log('📂 从文件恢复游戏状态，当前幕次:', gameState.maxAct);
    }
  } catch (e) {
    console.error('状态文件读取失败:', e.message);
  }
}

// 保存状态到文件
function saveStateToFile() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(gameState, null, 2), 'utf-8');
  } catch (e) {
    console.error('状态文件写入失败:', e.message);
  }
}

// 获取可公开的状态（发给玩家的不包含敏感信息）
function getPublicState() {
  return {
    maxAct: gameState.maxAct,
    taken: gameState.taken,
    players: gameState.players,
    hostActions: gameState.hostActions,
    lastPush: gameState.lastPush,
    gameStarted: gameState.gameStarted,
    serverTime: Date.now()
  };
}

// =============================================
// 静态文件
// =============================================
app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// =============================================
// Socket.IO 事件处理
// =============================================
io.on('connection', (socket) => {
  console.log(`🔗 新连接: ${socket.id} (总连接数: ${io.engine.clientsCount})`);

  // 1. 客户端请求当前状态
  socket.on('get_state', () => {
    socket.emit('state_full', getPublicState());
  });

  // 2. 选择角色
  socket.on('select_role', (data) => {
    const { roleId, force } = data || {};
    if (!roleId) {
      socket.emit('role_result', { success: false, message: '未指定角色' });
      return;
    }

    // 检查是否已被占用
    if (gameState.taken[roleId] && gameState.taken[roleId] !== socket.id) {
      if (!force) {
        socket.emit('role_result', { success: false, message: '该角色已被其他玩家选择', roleId });
        return;
      }
    }

    // 释放旧角色（如果之前选过）
    Object.keys(gameState.taken).forEach(rid => {
      if (gameState.taken[rid] === socket.id && rid !== roleId) {
        delete gameState.taken[rid];
        if (gameState.players[rid]?.socketId === socket.id) {
          delete gameState.players[rid];
        }
      }
    });

    // 分配角色
    gameState.taken[roleId] = socket.id;
    if (!gameState.players[roleId]) {
      gameState.players[roleId] = { act: '1', silver: 0, socketId: socket.id };
    } else {
      gameState.players[roleId].socketId = socket.id;
    }
    gameState.gameStarted = true;

    saveStateToFile();

    // 通知该客户端
    socket.emit('role_result', {
      success: true,
      roleId,
      playerState: gameState.players[roleId],
      maxAct: gameState.maxAct
    });

    // 广播角色占用变更（不含自己，其他客户端需要知道）
    socket.broadcast.emit('state_update', {
      taken: gameState.taken,
      players: gameState.players
    });

    console.log(`🎭 ${roleId} 被 ${socket.id} 选择`);
  });

  // 3. 释放角色（断线时清理）
  socket.on('release_role', (data) => {
    const roleId = data?.roleId;
    if (roleId && gameState.taken[roleId] === socket.id) {
      delete gameState.taken[roleId];
      delete gameState.players[roleId];
      saveStateToFile();
      io.emit('state_update', {
        taken: gameState.taken,
        players: gameState.players
      });
      console.log(`🔓 ${roleId} 角色已释放`);
    }
  });

  // 4. 主持人登录
  socket.on('host_login', (data) => {
    const { password } = data || {};
    if (password === '8888') {
      gameState.hostConnected = socket.id;
      socket.emit('host_login_result', {
        success: true,
        fullState: gameState
      });
      console.log(`👑 主持人登录: ${socket.id}`);
    } else {
      socket.emit('host_login_result', { success: false, message: '密码错误' });
    }
  });

  // 5. 主持人推进幕次
  socket.on('host_push_act', (data) => {
    // 验证是否为主持人
    if (gameState.hostConnected !== socket.id) {
      socket.emit('error', { message: '仅主持人可推进幕次' });
      return;
    }

    const act = parseInt(data?.act) || 1;
    if (act <= gameState.maxAct) {
      socket.emit('error', { message: '该幕已推进' });
      return;
    }
    if (act > gameState.maxAct + 1) {
      socket.emit('error', { message: '请按幕次顺序依次推进' });
      return;
    }

    gameState.maxAct = act;
    gameState.lastPush = { act, time: Date.now() };
    saveStateToFile();

    // 广播给所有客户端（包括主持人自己）
    io.emit('act_advanced', {
      act,
      maxAct: gameState.maxAct,
      lastPush: gameState.lastPush
    });

    console.log(`📢 主持人推进至第${act}幕`);
  });

  // 6. 主持人发放物品
  socket.on('host_give', (data) => {
    if (gameState.hostConnected !== socket.id) {
      socket.emit('error', { message: '仅主持人可发放物品' });
      return;
    }

    const { roleId, type, value } = data || {};
    if (!roleId || !type) {
      socket.emit('error', { message: '参数不完整' });
      return;
    }

    if (!gameState.hostActions) {
      gameState.hostActions = { silverGiven: {}, cluesGiven: {}, skillsGiven: {} };
    }

    let msg = '';
    if (type === 'silver') {
      const amount = parseInt(value) || 0;
      if (amount <= 0) { socket.emit('error', { message: '无效数量' }); return; }
      gameState.hostActions.silverGiven[roleId] = (gameState.hostActions.silverGiven[roleId] || 0) + amount;
      msg = `向${roleId}发放${amount}两银票`;
    } else if (type === 'clue') {
      if (!gameState.hostActions.cluesGiven[roleId]) gameState.hostActions.cluesGiven[roleId] = [];
      gameState.hostActions.cluesGiven[roleId].push(value);
      msg = `向${roleId}发放线索卡${value}`;
    } else if (type === 'skill') {
      if (!gameState.hostActions.skillsGiven[roleId]) gameState.hostActions.skillsGiven[roleId] = [];
      gameState.hostActions.skillsGiven[roleId].push(value);
      msg = `向${roleId}发放技能卡「${value}」`;
    }

    saveStateToFile();

    // 广播物品发放
    io.emit('item_given', {
      roleId, type, value,
      hostActions: gameState.hostActions
    });

    console.log(`🎁 ${msg}`);
  });

  // 7. 主持人重置游戏
  socket.on('host_reset', () => {
    if (gameState.hostConnected !== socket.id) {
      socket.emit('error', { message: '仅主持人可重置' });
      return;
    }

    gameState = {
      maxAct: 1,
      taken: {},
      players: {},
      hostActions: { silverGiven: {}, cluesGiven: {}, skillsGiven: {} },
      hostConnected: socket.id, // 保留主持人连接
      lastPush: null,
      gameStarted: false
    };

    saveStateToFile();

    // 通知所有客户端
    io.emit('game_reset', { message: '主持人已重置游戏' });
    // 也给主持人发送完整状态
    socket.emit('host_login_result', {
      success: true,
      fullState: gameState
    });

    console.log('🔄 游戏已重置');
  });

  // 8. 更新玩家当前观看的幕次（不影响全局，只是记录）
  socket.on('player_act_change', (data) => {
    const { roleId, act } = data || {};
    if (roleId && gameState.taken[roleId] === socket.id) {
      if (gameState.players[roleId]) {
        gameState.players[roleId].act = act;
        saveStateToFile();
      }
    }
  });

  // 9. 断开连接
  socket.on('disconnect', () => {
    console.log(`🔌 断开连接: ${socket.id}`);

    // 如果主持人断开，清除标记
    if (gameState.hostConnected === socket.id) {
      gameState.hostConnected = false;
      console.log('👑 主持人已离线');
    }

    // 不自动释放角色（允许断线重连）
    // 但通知其他客户端有人断线
    socket.broadcast.emit('player_disconnected', { socketId: socket.id });

    console.log(`当前连接数: ${io.engine.clientsCount}`);
  });

  // 发送当前状态给新连接
  socket.emit('state_full', getPublicState());
});

// =============================================
// 启动服务器
// =============================================
loadStateFromFile();

server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('🏮 ====================================');
  console.log('   闲酌序·沾花局 服务器已启动');
  console.log(`   地址: http://0.0.0.0:${PORT}`);
  console.log(`   当前幕次: ${gameState.maxAct}`);
  console.log('   ====================================');
  console.log('');
});
