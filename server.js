require('dotenv').config(); 
const express = require('express'); 
const cors = require('cors'); 
const { createClient } = require('@supabase/supabase-js'); 
const crypto = require('crypto'); 
const http = require('http'); 
const WebSocket = require('ws'); 
const path = require('path'); 

const app = express(); 
const PORT = process.env.PORT || 3000; 
const server = http.createServer(app); 
const wss = new WebSocket.Server({ server }); 

// ─── SUPABASE ───────────────────────────────────────────────────────────────── 
const supabaseUrl = process.env.SUPABASE_URL; 
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY; 
const supabase = createClient(supabaseUrl, supabaseAnonKey, { 
  realtime: { transport: WebSocket } 
}); 

// ─── ESTADO ─────────────────────────────────────────────────────────────────── 
let connectedClients = []; 
let motoristasOnline = 0; 
let cuponsQueimados = 0; 

// ─── WEBSOCKET ──────────────────────────────────────────────────────────────── 
wss.on('connection', (ws) => { 
  console.log('[WS] Novo cliente conectado'); 
  connectedClients.push(ws); 

  ws.send(JSON.stringify({ 
    type: 'initial_stats', 
    motoristasOnline, 
    cuponsQueimados 
  })); 

  ws.on('message', (raw) => { 
    try { 
      const msg = JSON.parse(raw); 

      // Motorista entra online via app 
      if (msg.tipo === 'motorista_online' && msg.motorista_id) { 
        motoristasOnline = Math.max(motoristasOnline + 1, 1); 
        broadcast({ type: 'motorista_status', motoristasOnline }); 
      } 

      // Corrida capturada pelo copiloto via WS (alternativa ao POST) 
      if (msg.tipo === 'corrida_capturada') { 
        broadcast({ type: 'nova_corrida', corrida: msg }); 
      } 

      // Alerta de segurança via WS 
      if (msg.tipo === 'alerta_seguranca') { 
        broadcast({ type: 'novo_alerta', alerta: msg }); 
      } 

    } catch (e) { 
      console.error('[WS] Erro ao processar mensagem:', e.message); 
    } 
  }); 

  ws.on('close', () => { 
    console.log('[WS] Cliente desconectado'); 
    connectedClients = connectedClients.filter(c => c !== ws); 
    motoristasOnline = Math.max(motoristasOnline - 1, 0); 
    broadcast({ type: 'motorista_status', motoristasOnline }); 
  }); 
}); 

const broadcast = (message) => { 
  connectedClients.forEach((client) => { 
    if (client.readyState === WebSocket.OPEN) { 
      client.send(JSON.stringify(message)); 
    } 
  }); 
}; 

// ─── MIDDLEWARES ────────────────────────────────────────────────────────────── 
app.use(cors()); 
app.use(express.json()); 
app.use(express.static(path.join(__dirname))); 

// ─── UTILITÁRIOS ────────────────────────────────────────────────────────────── 
const getBrazilTimeInfo = () => { 
  const brazilDate = new Date().toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }); 
  const date = new Date(brazilDate); 
  const dayOfWeek = date.getDay(); 
  const hour = date.getHours(); 
  let timeRange; 
  if (hour >= 0 && hour < 6) timeRange = 'madrugada'; 
  else if (hour >= 6 && hour < 12) timeRange = 'manha'; 
  else if (hour >= 12 && hour < 18) timeRange = 'tarde'; 
  else timeRange = 'noite'; 
  return { dayOfWeek, timeRange }; 
}; 

const calculateSHA256 = (data) => crypto.createHash('sha256').update(data).digest('hex'); 

const generateRandomCode = (length) => { 
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'; 
  let result = ''; 
  for (let i = 0; i < length; i++) result += chars.charAt(Math.floor(Math.random() * chars.length)); 
  return result; 
}; 

// ─── PÁGINAS ────────────────────────────────────────────────────────────────── 
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'parceiro.html'))); 
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'admin_dashboard.html'))); 

// ─── ROTA: DASHBOARD STATS (expandida com dados reais) ─────────────────────── 
app.get('/api/v1/admin/dashboard-stats', async (req, res) => { 
  try { 
    const { periodo, data_inicio, data_fim } = req.query; 

    let startDate, endDate; 
    const now = new Date(); 
    if (periodo === '7') startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); 
    else if (periodo === '15') startDate = new Date(now.getTime() - 15 * 24 * 60 * 60 * 1000); 
    else if (periodo === '30') startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000); 
    else if (periodo === 'personalizado' && data_inicio && data_fim) { 
      startDate = new Date(data_inicio); 
      endDate = new Date(data_fim); 
    } else { 
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); 
    } 
    if (!endDate) endDate = now; 

    const [motoristasData, tokensData, corridasData, auditoriaData] = await Promise.all([ 
      supabase.from('smart_motoristas_cadastro').select('*').gte('created_at', startDate.toISOString()).lte('created_at', endDate.toISOString()), 
      supabase.from('smart_parceiros_tokens').select('*'), 
      supabase.from('smartmob_historico_precos').select('*').gte('created_at', startDate.toISOString()).lte('created_at', endDate.toISOString()), 
      supabase.from('smart_auditoria_aceites').select('motorista_id, data_aceite, hash_sha256') 
    ]); 

    const motoristas = motoristasData.data || []; 
    const corridas = corridasData.data || []; 
    const tokens = tokensData.data || []; 

    const totalCadastros = motoristas.length; 
    const totalCorridas = corridas.length; 
    const totalKM = corridas.reduce((sum, c) => sum + (parseFloat(c.km) || 0), 0); 
    const totalFaturamento = corridas.reduce((sum, c) => sum + (parseFloat(c.valor_corrida) || 0), 0); 
    const totalFaturamentoLiquido = corridas.reduce((sum, c) => sum + (parseFloat(c.ganho_liquido) || 0), 0); 

    const tokensAtivos = tokens.filter(t => t.status === 'ativo').length; 
    const tokensUsados = tokens.filter(t => t.status === 'utilizado').length; 

    // Frota por tipo de motor (dummy values since column doesn't exist)
    const eletricos = 0;
    const combustao = 0; 

    // Ranking por KM 
    const kmPorMotorista = {}; 
    const alertasPorMotorista = {}; 
    corridas.forEach(c => { 
      if (c.motorista_id) { 
        kmPorMotorista[c.motorista_id] = (kmPorMotorista[c.motorista_id] || 0) + (parseFloat(c.km) || 0); 
      } 
    }); 

    // Buscar nomes dos motoristas do ranking 
    const topIds = Object.entries(kmPorMotorista) 
      .sort((a, b) => b[1] - a[1]) 
      .slice(0, 10) 
      .map(([id]) => id); 

    const { data: topMotoristas } = topIds.length > 0 
      ? await supabase.from('smart_motoristas_cadastro').select('id, nome, celular, marca, modelo, ano_carro').in('id', topIds) 
      : { data: [] }; 

    const rankingMotoristas = (topMotoristas || []).map(m => ({ 
      nome: m.nome, 
      celular: m.celular, 
      veiculo: `${m.marca} ${m.modelo} ${m.ano_carro}`, 
      km: Math.round(kmPorMotorista[m.id] * 10) / 10, 
      alertas: alertasPorMotorista[m.id] || 0 
    })).sort((a, b) => b.km - a.km); 

    // Faturamento diário (últimos N dias) 
    const diasNum = parseInt(periodo) || 7; 
    const faturamentoDiario = []; 
    const labelsDiario = []; 
    for (let i = diasNum - 1; i >= 0; i--) { 
      const dia = new Date(now.getTime() - i * 24 * 60 * 60 * 1000); 
      const diaStr = dia.toISOString().split('T')[0]; 
      const corridasDia = corridas.filter(c => (c.created_at || '').startsWith(diaStr)); 
      faturamentoDiario.push(Math.round(corridasDia.reduce((s, c) => s + (parseFloat(c.valor_corrida) || 0), 0))); 
      labelsDiario.push(dia.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' })); 
    } 

    // Auditoria recente 
    const auditoria = (auditoriaData.data || []).slice(-10).reverse(); 

    res.json({ 
      totalCadastros, 
      totalCorridas, 
      totalKM: Math.round(totalKM), 
      totalFaturamento: Math.round(totalFaturamento * 100) / 100, 
      totalFaturamentoLiquido: Math.round(totalFaturamentoLiquido * 100) / 100, 
      motoristasOnline, 
      tokensAtivos, 
      tokensUsados, 
      veiculos: { combustao, eletricos }, 
      rankingMotoristas, 
      dadosLinha: { labels: labelsDiario, faturamento: faturamentoDiario }, 
      auditoria 
    }); 
  } catch (error) { 
    console.error('Erro ao buscar stats:', error); 
    res.status(500).json({ error: 'Erro interno do servidor' }); 
  } 
}); 

// ─── ROTA: SALVAR CORRIDA (copiloto) ───────────────────────────────────────── 
app.post('/api/v1/salvar-corrida', async (req, res) => { 
  try { 
    const { 
      app_name, valor_corrida, lat_origem, lng_origem, 
      lat_destino, lng_destino, motorista_id, 
      km, tempo_estimado, bairro_origem, ganho_liquido, aceita 
    } = req.body; 

    if (!app_name || valor_corrida === undefined || lat_origem === undefined || 
        lng_origem === undefined || lat_destino === undefined || lng_destino === undefined) { 
      return res.status(400).json({ error: 'Campos obrigatórios ausentes' }); 
    } 

    const { dayOfWeek, timeRange } = getBrazilTimeInfo(); 

    const { data, error } = await supabase 
      .from('smartmob_historico_precos') 
      .insert([{ 
        app_name, 
        valor_corrida, 
        origem: `POINT(${lng_origem} ${lat_origem})`, 
        destino: `POINT(${lng_destino} ${lat_destino})`, 
        dia_semana: dayOfWeek, 
        faixa_horario: timeRange, 
        motorista_id, 
        km: km || null, 
        tempo_estimado: tempo_estimado || null, 
        bairro_origem: bairro_origem || null, 
        ganho_liquido: ganho_liquido || null, 
        aceita: aceita !== undefined ? aceita : null 
      }]) 
      .select(); 

    if (error) throw error;

    // Atualizar ODO do motorista automaticamente
    if (motorista_id && km) {
      try {
        const { data: motoristaAtual } = await supabase
          .from('smart_motoristas_cadastro')
          .select('odometro_atual')
          .eq('id', motorista_id)
          .single();

        if (motoristaAtual) {
          const novoOdo = (motoristaAtual.odometro_atual || 0) + parseFloat(km);
          await supabase
            .from('smart_motoristas_cadastro')
            .update({ odometro_atual: Math.round(novoOdo) })
            .eq('id', motorista_id);

          console.log(`[ODO] Motorista ${motorista_id}: ${motoristaAtual.odometro_atual} → ${Math.round(novoOdo)} km`);
        }
      } catch(e) {
        console.error('[ODO-ERRO]', e.message);
      }
    }

    broadcast({ type: 'nova_corrida', corrida: data[0] }); 

    res.status(201).json({ message: 'Corrida salva com sucesso', data }); 
  } catch (error) { 
    console.error('Erro ao salvar corrida:', error); 
    res.status(500).json({ error: 'Erro interno do servidor' }); 
  } 
}); 

// ─── ROTA: CADASTRAR MOTORISTA ──────────────────────────────────────────────── 
app.post('/api/v1/motorista/cadastrar', async (req, res) => { 
  console.log('[CADASTRO] Body recebido:', JSON.stringify(req.body));
  
  try { 
    const { 
      nome, nome_completo, celular, email, cpf, 
      marca, marca_carro, modelo, modelo_carro, 
      ano_carro, placa_carro, cor_carro, 
      versao_termo, tipo_motor 
    } = req.body; 

    // Aceita tanto "nome" quanto "nome_completo" (compatibilidade com o app Android) 
    const nomeMotorista = nome || nome_completo; 
    const marcaVeiculo = marca || marca_carro; 
    const modeloVeiculo = modelo || modelo_carro; 

    if (!nomeMotorista || !celular || !email || !marcaVeiculo || 
        !modeloVeiculo || !ano_carro || !placa_carro || !cor_carro) { 
      return res.status(400).json({ error: 'Campos obrigatórios ausentes' }); 
    } 

    // Buscar termo atual (usar dummy se não existir para testes)
    let termosData = { texto: 'Termo de uso dummy', versao: '1.0' };
    const { data: termosFromDB, error: termosError } = await supabase 
      .from('smart_termos') 
      .select('*') 
      .order('criado_em', { ascending: false }) 
      .limit(1) 
      .single(); 

    if (!termosError && termosFromDB) {
      termosData = termosFromDB;
    }

    const timestamp = new Date().toISOString(); 
    const dataToHash = `${termosData.texto}${cpf || email}${nomeMotorista}${timestamp}`; 
    const hash = calculateSHA256(dataToHash); 

    const { data: motoristaData, error: motoristaError } = await supabase 
      .from('smart_motoristas_cadastro') 
      .insert([{ 
        nome: nomeMotorista, 
        celular, 
        email, 
        cpf: cpf || null, 
        marca: marcaVeiculo, 
        modelo: modeloVeiculo, 
        ano_carro, 
        placa_carro, 
        cor_carro, 
        tipo_motor: tipo_motor || 'combustao', 
        versao_termo_aceito: versao_termo || termosData.versao, 
        data_nascimento: req.body.data_nascimento || null, 
        odometro_atual: req.body.odometro_atual || null, 
        data_ultima_revisao: req.body.data_ultima_revisao || null 
      }]) 
      .select() 
      .single(); 

    if (motoristaError) { 
      console.error('Erro ao cadastrar motorista:', motoristaError); 
      return res.status(500).json({ error: 'Erro ao cadastrar motorista', details: motoristaError }); 
    } 

    // Tentar salvar auditoria, mas não falhar se não conseguir
    const { error: auditoriaError } = await supabase 
      .from('smart_auditoria_aceites') 
      .insert([{ 
        motorista_id: motoristaData.id, 
        data_aceite: new Date(), 
        termo_texto_completo: termosData.texto, 
        hash_sha256: hash 
      }]); 

    if (auditoriaError) { 
      console.error('Erro ao salvar auditoria:', auditoriaError); 
    } 

    broadcast({ type: 'novo_motorista', motorista: motoristaData }); 

    res.status(200).json({ 
      message: 'Motorista cadastrado com sucesso', 
      motorista: motoristaData, 
      hash_auditoria: hash 
    }); 
  } catch (error) { 
    console.error('[CADASTRO-ERRO]', error.message, error.details || ''); 
    res.status(500).json({ error: 'Erro interno do servidor', detalhe: error.message }); 
  } 
}); 

// ─── ROTA: GERAR TOKEN ──────────────────────────────────────────────────────── 
app.post('/api/v1/token/gerar', async (req, res) => { 
  try { 
    const { motorista_id } = req.body; 
    if (!motorista_id) return res.status(400).json({ error: 'motorista_id é obrigatório' }); 

    const twoDaysAgo = new Date(Date.now() - 48 * 60 * 60 * 1000); 
    const { count, error: countError } = await supabase 
      .from('smartmob_historico_precos') 
      .select('*', { count: 'exact', head: true }) 
      .gte('created_at', twoDaysAgo.toISOString()) 
      .eq('motorista_id', motorista_id); 

    if (countError) return res.status(500).json({ error: 'Erro ao verificar corridas' }); 
    if (count < 5) return res.status(403).json({ 
      error: 'Necessário pelo menos 5 corridas nas últimas 48 horas', 
      corridas_atuais: count, 
      corridas_necessarias: 5 
    }); 

    const tokenCode = generateRandomCode(6); 
    const validade = new Date(Date.now() + 15 * 60 * 1000); 

    const { data: tokenData, error: tokenError } = await supabase 
      .from('smart_parceiros_tokens') 
      .insert([{ token_codigo: tokenCode, motorista_id, validade, status: 'ativo' }]) 
      .select() 
      .single(); 

    if (tokenError) return res.status(500).json({ error: 'Erro ao gerar token' }); 

    broadcast({ type: 'novo_token', token_codigo: tokenCode }); 
    res.status(201).json({ message: 'Token gerado com sucesso', token: tokenData }); 
  } catch (error) { 
    console.error('Erro ao gerar token:', error); 
    res.status(500).json({ error: 'Erro interno do servidor' }); 
  } 
}); 

// ─── ROTA: VALIDAR TOKEN ────────────────────────────────────────────────────── 
app.post('/api/v1/token/validar', async (req, res) => { 
  try { 
    const { token_codigo } = req.body; 
    if (!token_codigo) return res.status(400).json({ error: 'token_codigo é obrigatório' }); 

    const { data: tokenData, error: tokenError } = await supabase 
      .from('smart_parceiros_tokens') 
      .select('*') 
      .eq('token_codigo', token_codigo) 
      .single(); 

    if (tokenError || !tokenData) return res.status(404).json({ error: 'Token não encontrado' }); 
    if (tokenData.status !== 'ativo') return res.status(403).json({ error: 'Token inválido ou já utilizado' }); 
    if (new Date(tokenData.validade) < new Date()) { 
      await supabase.from('smart_parceiros_tokens').update({ status: 'expirado' }).eq('id', tokenData.id); 
      return res.status(403).json({ error: 'Token expirado' }); 
    } 

    await supabase.from('smart_parceiros_tokens').update({ status: 'utilizado' }).eq('id', tokenData.id); 

    cuponsQueimados += 1; 
    broadcast({ type: 'cupom_validado', cuponsQueimados }); 

    res.status(200).json({ message: 'Autorizado: Motorista Smart Parceiro Ativo', valido: true }); 
  } catch (error) { 
    console.error('Erro ao validar token:', error); 
    res.status(500).json({ error: 'Erro interno do servidor' }); 
  } 
}); 

// ─── ROTA: ADMIN — TERMO ────────────────────────────────────────────────────── 
app.post('/api/v1/admin/termo', async (req, res) => { 
  try { 
    const { versao, texto } = req.body; 
    if (!versao || !texto) return res.status(400).json({ error: 'versao e texto são obrigatórios' }); 
    const { data, error } = await supabase.from('smart_termos').insert([{ versao, texto }]).select(); 
    if (error) throw error; 
    res.status(201).json({ message: 'Termo criado com sucesso', data }); 
  } catch (error) { 
    console.error('Erro ao criar termo:', error); 
    res.status(500).json({ error: 'Erro interno do servidor' }); 
  } 
}); 

// ─── ROTA: ADMIN — APP ──────────────────────────────────────────────────────── 
app.post('/api/v1/admin/app', async (req, res) => { 
  try { 
    const { nome, package_name, ativo = true } = req.body; 
    if (!nome || !package_name) return res.status(400).json({ error: 'nome e package_name são obrigatórios' }); 
    const { data, error } = await supabase.from('smart_apps').insert([{ nome, package_name, ativo }]).select(); 
    if (error) throw error; 
    res.status(201).json({ message: 'App criado com sucesso', data }); 
  } catch (error) { 
    console.error('Erro ao criar app:', error); 
    res.status(500).json({ error: 'Erro interno do servidor' }); 
  } 
}); 

// ─── ROTA: MOTORISTAS (listagem) ────────────────────────────────────────────── 
app.get('/api/v1/motoristas', async (req, res) => { 
  try { 
    const { busca } = req.query; 
    let query = supabase.from('smart_motoristas_cadastro').select('*').order('created_at', { ascending: false }); 
    const { data, error } = await query; 
    if (error) throw error; 
    let lista = data || []; 
    if (busca) { 
      const b = busca.toLowerCase(); 
      lista = lista.filter(m => 
        (m.nome || '').toLowerCase().includes(b) || 
        (m.email || '').toLowerCase().includes(b) || 
        (m.placa_carro || '').toLowerCase().includes(b) || 
        (m.celular || '').includes(b) 
      ); 
    } 
    res.json(lista); 
  } catch (error) { 
    res.status(500).json({ error: 'Erro ao listar motoristas' }); 
  } 
}); 

// ─── ROTA: AUDITORIA (listagem) ─────────────────────────────────────────────── 
app.get('/api/v1/auditoria', async (req, res) => { 
  try { 
    const { data, error } = await supabase 
      .from('smart_auditoria_aceites') 
      .select('motorista_id, data_aceite, hash_sha256') 
      .order('data_aceite', { ascending: false }); 
    if (error) throw error; 
    res.json(data || []); 
  } catch (error) { 
    res.status(500).json({ error: 'Erro ao listar auditoria' }); 
  } 
}); 

// ─── ROTA: APPS MONITORADOS ─────────────────────────────────────────────────── 
app.get('/api/v1/apps', async (req, res) => { 
  try { 
    const { data, error } = await supabase.from('smart_apps').select('*'); 
    if (error) throw error; 
    res.json(data || []); 
  } catch (error) { 
    res.status(500).json({ error: 'Erro ao listar apps' }); 
  } 
}); 

// ─── ROTA: DEBUG ──────────────────────────────────────────────────────────────
app.get('/api/v1/debug', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('smart_motoristas_cadastro')
      .select('*')
      .limit(5);
    console.log(JSON.stringify(data, null, 2));
    console.log('ERRO:', error);
    res.json({ data, error });
  } catch (err) {
    console.error('Erro na rota debug:', err);
    res.status(500).json({ error: err.message });
  }
});

// Rota para criar termo de teste
app.get('/api/v1/debug/create-termo', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('smart_termos')
      .insert([{
        versao: '1.0',
        texto: 'Termo de uso da plataforma SmartMob. By ELVIVA GROUP LTDA.'
      }])
      .select();
    res.json({ data, error });
  } catch (err) {
    console.error('Erro ao criar termo:', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── ROTAS: PÁGINAS WEB SELLER ────────────────────────────────────────────────
app.get('/cadastro', (req, res) => res.sendFile(path.join(__dirname, 'cadastro.html')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'login.html')));
app.get('/seller', (req, res) => res.sendFile(path.join(__dirname, 'seller.html')));

// ─── ROTAS: SELLER API ────────────────────────────────────────────────────────

// Buscar termo atual para sellers
app.get('/api/v1/seller/termos', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('smart_termos_web')
      .select('*')
      .order('criado_em', { ascending: false })
      .limit(1);

    console.log('[TERMOS-WEB] data:', JSON.stringify(data), 'error:', error);

    if (error) throw error;
    if (!data || data.length === 0) {
      return res.json({ versao: '1.0', texto: 'Termos não encontrados.' });
    }
    res.json(data[0]);
  } catch(e) {
    console.error('[TERMOS-WEB] Erro:', e);
    res.status(500).json({ error: 'Erro ao buscar termos', detalhe: e.message });
  }
});

// Cadastrar seller
app.post('/api/v1/seller/cadastrar', async (req, res) => {
  try {
    const { firebase_uid, nome, email, versao_termo, texto_termo } = req.body;
    if (!firebase_uid || !nome || !email) return res.status(400).json({ error: 'Campos obrigatórios ausentes' });

    const hash = calculateSHA256(`${texto_termo}${email}${new Date().toISOString()}`);

    const { data: existing } = await supabase
      .from('smart_sellers')
      .select('id')
      .eq('firebase_uid', firebase_uid)
      .single();

    if (existing) return res.json({ message: 'Seller já cadastrado', id: existing.id });

    const { data, error } = await supabase
      .from('smart_sellers')
      .insert([{ firebase_uid, nome, email, versao_termo_aceito: versao_termo, hash_termo: hash }])
      .select().single();

    if (error) throw error;
    broadcast({ type: 'novo_seller', seller: data });
    res.json({ message: 'Seller cadastrado com sucesso', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao cadastrar seller' }); }
});

// Verificar se termo precisa ser atualizado
app.get('/api/v1/seller/verificar-termo', async (req, res) => {
  try {
    const { uid } = req.query;
    const { data: seller } = await supabase.from('smart_sellers').select('versao_termo_aceito').eq('firebase_uid', uid).single();
    const { data: termo } = await supabase.from('smart_termos_web').select('*').order('criado_em', { ascending: false }).limit(1).single();
    if (!seller || !termo) return res.json({ precisa_atualizar: false });
    const precisa = seller.versao_termo_aceito !== termo.versao;
    res.json({ precisa_atualizar: precisa, versao: termo.versao, texto: termo.texto });
  } catch(e) { res.json({ precisa_atualizar: false }); }
});

// Atualizar termo aceito
app.post('/api/v1/seller/atualizar-termo', async (req, res) => {
  try {
    const { firebase_uid, versao_termo, texto_termo } = req.body;
    const hash = calculateSHA256(`${texto_termo}${firebase_uid}${new Date().toISOString()}`);
    await supabase.from('smart_sellers').update({ versao_termo_aceito: versao_termo, hash_termo: hash }).eq('firebase_uid', firebase_uid);
    res.json({ message: 'Termo atualizado' });
  } catch(e) { res.status(500).json({ error: 'Erro ao atualizar termo' }); }
});

// Buscar seller por uid
async function getSellerByUid(uid) {
  const { data } = await supabase.from('smart_sellers').select('*').eq('firebase_uid', uid).single();
  return data;
}

// Produtos
app.get('/api/v1/seller/produtos', async (req, res) => {
  try {
    const seller = await getSellerByUid(req.query.uid);
    if (!seller) return res.json([]);
    const { data } = await supabase.from('smart_produtos').select('*').eq('seller_id', seller.id).order('created_at', { ascending: false });
    res.json(data || []);
  } catch(e) { res.json([]); }
});

app.post('/api/v1/seller/produto', async (req, res) => {
  try {
    const { uid, nome, descricao, preco, estoque, categoria, compatibilidade } = req.body;
    const seller = await getSellerByUid(uid);
    if (!seller) return res.status(404).json({ error: 'Seller não encontrado' });
    const { data, error } = await supabase.from('smart_produtos').insert([{ seller_id: seller.id, nome, descricao, preco, estoque, categoria, compatibilidade }]).select().single();
    if (error) throw error;
    res.json({ message: 'Produto criado', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao criar produto' }); }
});

app.patch('/api/v1/seller/produto/:id', async (req, res) => {
  try {
    const { data, error } = await supabase.from('smart_produtos').update(req.body).eq('id', req.params.id).select().single();
    if (error) throw error;
    res.json({ message: 'Produto atualizado', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao atualizar produto' }); }
});

// Serviços
app.get('/api/v1/seller/servicos', async (req, res) => {
  try {
    const seller = await getSellerByUid(req.query.uid);
    if (!seller) return res.json([]);
    const { data } = await supabase.from('smart_servicos').select('*').eq('seller_id', seller.id).order('created_at', { ascending: false });
    res.json(data || []);
  } catch(e) { res.json([]); }
});

app.post('/api/v1/seller/servico', async (req, res) => {
  try {
    const { uid, nome, descricao, preco, tempo_estimado, categoria, compatibilidade } = req.body;
    const seller = await getSellerByUid(uid);
    if (!seller) return res.status(404).json({ error: 'Seller não encontrado' });
    const { data, error } = await supabase.from('smart_servicos').insert([{ seller_id: seller.id, nome, descricao, preco, tempo_estimado, categoria, compatibilidade }]).select().single();
    if (error) throw error;
    res.json({ message: 'Serviço criado', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao criar serviço' }); }
});

app.patch('/api/v1/seller/servico/:id', async (req, res) => {
  try {
    const { data, error } = await supabase.from('smart_servicos').update(req.body).eq('id', req.params.id).select().single();
    if (error) throw error;
    res.json({ message: 'Serviço atualizado', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao atualizar serviço' }); }
});

// Pedidos
app.get('/api/v1/seller/pedidos', async (req, res) => {
  try {
    const seller = await getSellerByUid(req.query.uid);
    if (!seller) return res.json([]);
    const { data } = await supabase.from('smart_pedidos').select('*').eq('seller_id', seller.id).order('created_at', { ascending: false });
    res.json(data || []);
  } catch(e) { res.json([]); }
});

app.patch('/api/v1/seller/pedido/:id', async (req, res) => {
  try {
    const { data, error } = await supabase.from('smart_pedidos').update(req.body).eq('id', req.params.id).select().single();
    if (error) throw error;
    res.json({ message: 'Pedido atualizado', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao atualizar pedido' }); }
});

// Avaliações
app.get('/api/v1/seller/avaliacoes', async (req, res) => {
  try {
    const seller = await getSellerByUid(req.query.uid);
    if (!seller) return res.json([]);
    const { data } = await supabase.from('smart_avaliacoes').select('*').eq('seller_id', seller.id).order('created_at', { ascending: false });
    res.json(data || []);
  } catch(e) { res.json([]); }
});

// Perfil do seller
app.patch('/api/v1/seller/perfil', async (req, res) => {
  try {
    const { uid, ...campos } = req.body;
    const { data, error } = await supabase.from('smart_sellers').update(campos).eq('firebase_uid', uid).select().single();
    if (error) throw error;
    res.json({ message: 'Perfil atualizado', data });
  } catch(e) { res.status(500).json({ error: 'Erro ao atualizar perfil' }); }
});

// ─── ALGORITMO DE INTELIGÊNCIA DO MOTORISTA ───────────────────────────────────

// Réguas de desgaste por tipo de motor (em km)
const REGUAS_DESGASTE = {
  combustao: {
    oleo: 5000,
    freios: 20000,
    pneus: 40000,
    revisao_geral: 10000
  },
  eletrico: {
    oleo: null,
    freios: 30000,
    pneus: 40000,
    revisao_geral: 15000
  },
  hibrido: {
    oleo: 7500,
    freios: 25000,
    pneus: 40000,
    revisao_geral: 12000
  }
};

async function calcularScoreDesgaste(kmDesdeRevisao, limite) {
  if (!limite) return 0;
  return Math.min(100, Math.round((kmDesdeRevisao / limite) * 100));
}

async function rodarAlgoritmoMotorista() {
  console.log('[ALGORITMO] Iniciando análise dos motoristas...');
  try {
    const { data: motoristas } = await supabase
      .from('smart_motoristas_cadastro')
      .select('*');

    if (!motoristas || motoristas.length === 0) {
      console.log('[ALGORITMO] Nenhum motorista encontrado.');
      return;
    }

    for (const motorista of motoristas) {
      try {
        const agora = new Date();
        const trintaDiasAtras = new Date(agora.getTime() - 30 * 24 * 60 * 60 * 1000);
        const seteDiasAtras = new Date(agora.getTime() - 7 * 24 * 60 * 60 * 1000);

        // Buscar corridas dos últimos 30 dias
        const { data: corridas } = await supabase
          .from('smartmob_historico_precos')
          .select('*')
          .eq('motorista_id', motorista.id)
          .gte('created_at', trintaDiasAtras.toISOString());

        const totalCorridas = corridas?.length || 0;
        const corridasAceitas = corridas?.filter(c => c.aceita === true).length || 0;

        // KM médio por dia
        const kmTotal = corridas?.reduce((acc, c) => acc + (parseFloat(c.km) || 0), 0) || 0;
        const kmMedioDia = Math.round((kmTotal / 30) * 10) / 10;

        // Valor médio das corridas
        const valorMedio = totalCorridas > 0
          ? Math.round((corridas.reduce((acc, c) => acc + (parseFloat(c.valor_corrida) || 0), 0) / totalCorridas) * 100) / 100
          : 0;

        // Horário de pico
        const horarios = corridas?.map(c => new Date(c.created_at).getHours()) || [];
        const contagemHorarios = horarios.reduce((acc, h) => {
          acc[h] = (acc[h] || 0) + 1;
          return acc;
        }, {});
        const horarioPico = Object.entries(contagemHorarios)
          .sort((a, b) => b[1] - a[1])[0]?.[0];
        const horarioPicoStr = horarioPico ? `${horarioPico}h-${parseInt(horarioPico) + 1}h` : null;

        // Bairro mais frequente
        const bairros = corridas?.map(c => c.bairro_origem).filter(Boolean) || [];
        const contagemBairros = bairros.reduce((acc, b) => {
          acc[b] = (acc[b] || 0) + 1;
          return acc;
        }, {});
        const bairroFrequente = Object.entries(contagemBairros)
          .sort((a, b) => b[1] - a[1])[0]?.[0] || null;

        // Dias ativos na última semana
        const { data: corridasSemana } = await supabase
          .from('smartmob_historico_precos')
          .select('created_at')
          .eq('motorista_id', motorista.id)
          .gte('created_at', seteDiasAtras.toISOString());

        const diasUnicos = new Set(
          corridasSemana?.map(c => new Date(c.created_at).toDateString()) || []
        );
        const diasAtivos = diasUnicos.size;

        // Calcular KM desde última revisão
        const odoAtual = motorista.odometro_atual || 0;
        const odoRevisao = motorista.odometro_atual || 0;
        const kmDesdeRevisao = kmTotal;

        // Score de desgaste
        const tipoMotor = motorista.tipo_motor || 'combustao';
        const reguas = REGUAS_DESGASTE[tipoMotor] || REGUAS_DESGASTE.combustao;

        const scoreOleo = await calcularScoreDesgaste(kmDesdeRevisao, reguas.oleo);
        const scoreFreios = await calcularScoreDesgaste(kmDesdeRevisao, reguas.freios);
        const scorePneus = await calcularScoreDesgaste(kmDesdeRevisao, reguas.pneus);
        const scoreRevisao = await calcularScoreDesgaste(kmDesdeRevisao, reguas.revisao_geral);
        const scoreGeral = Math.max(scoreOleo, scoreFreios, scorePneus, scoreRevisao);

        // Previsão próxima revisão baseada no km médio diário
        let previsaoRevisao = null;
        if (kmMedioDia > 0 && reguas.revisao_geral) {
          const kmRestante = reguas.revisao_geral - kmDesdeRevisao;
          if (kmRestante > 0) {
            const diasRestantes = Math.round(kmRestante / kmMedioDia);
            previsaoRevisao = new Date(agora.getTime() + diasRestantes * 24 * 60 * 60 * 1000)
              .toISOString().split('T')[0];
          }
        }

        // Previsão troca de óleo
        let previsaoOleo = null;
        if (kmMedioDia > 0 && reguas.oleo) {
          const kmRestanteOleo = reguas.oleo - kmDesdeRevisao;
          if (kmRestanteOleo > 0) {
            const diasRestantesOleo = Math.round(kmRestanteOleo / kmMedioDia);
            previsaoOleo = new Date(agora.getTime() + diasRestantesOleo * 24 * 60 * 60 * 1000)
              .toISOString().split('T')[0];
          }
        }

        // Salvar perfil de inteligência
        await supabase
          .from('smart_perfil_inteligencia')
          .upsert({
            motorista_id: motorista.id,
            km_total_acumulado: kmTotal,
            km_medio_dia: kmMedioDia,
            dias_ativos_semana: diasAtivos,
            horario_pico: horarioPicoStr,
            bairro_frequente: bairroFrequente,
            valor_medio_corrida: valorMedio,
            total_corridas: totalCorridas,
            corridas_aceitas: corridasAceitas,
            corridas_recusadas: totalCorridas - corridasAceitas,
            score_desgaste_oleo: scoreOleo,
            score_desgaste_freios: scoreFreios,
            score_desgaste_pneus: scorePneus,
            score_desgaste_geral: scoreGeral,
            km_desde_ultima_revisao: kmDesdeRevisao,
            previsao_proxima_revisao: previsaoRevisao,
            previsao_troca_oleo: previsaoOleo,
            ultima_analise: new Date().toISOString()
          }, { onConflict: 'motorista_id' });

        // Gerar notificações se necessário
        if (scoreGeral >= 80) {
          await gerarNotificacaoDesgaste(motorista, scoreOleo, scoreFreios, scorePneus, scoreRevisao, reguas);
        }

        console.log(`[ALGORITMO] Motorista ${motorista.id} — Score: ${scoreGeral}% — KM/dia: ${kmMedioDia}`);

      } catch(e) {
        console.error(`[ALGORITMO-ERRO] Motorista ${motorista.id}:`, e.message);
      }
    }

    console.log('[ALGORITMO] Análise concluída.');
  } catch(e) {
    console.error('[ALGORITMO-ERRO-GERAL]', e.message);
  }
}

async function gerarNotificacaoDesgaste(motorista, scoreOleo, scoreFreios, scorePneus, scoreRevisao, reguas) {
  try {
    let tipo = '';
    let titulo = '';
    let mensagem = '';

    if (reguas.oleo && scoreOleo >= 80) {
      tipo = 'troca_oleo';
      titulo = '⚠️ Troca de óleo próxima';
      mensagem = `Seu veículo está com ${scoreOleo}% do limite para troca de óleo. Agende agora!`;
    } else if (scoreFreios >= 80) {
      tipo = 'revisao_freios';
      titulo = '⚠️ Revisão de freios recomendada';
      mensagem = `Seus freios estão com ${scoreFreios}% do limite. Segurança em primeiro lugar!`;
    } else if (scorePneus >= 80) {
      tipo = 'troca_pneus';
      titulo = '⚠️ Verifique seus pneus';
      mensagem = `Seus pneus estão com ${scorePneus}% do limite recomendado.`;
    } else if (scoreRevisao >= 80) {
      tipo = 'revisao_geral';
      titulo = '🔧 Revisão geral recomendada';
      mensagem = `Seu veículo precisa de revisão geral. Cuide bem do seu carro!`;
    }

    if (!tipo) return;

    // Verificar se já existe notificação do mesmo tipo não visualizada
    const { data: existente } = await supabase
      .from('smart_notificacoes')
      .select('id')
      .eq('motorista_id', motorista.id)
      .eq('tipo', tipo)
      .eq('visualizada', false)
      .limit(1);

    if (existente && existente.length > 0) return;

    await supabase.from('smart_notificacoes').insert([{
      motorista_id: motorista.id,
      tipo,
      titulo,
      mensagem,
      enviada: false,
      visualizada: false,
      clicada: false,
      convertida: false
    }]);

    console.log(`[NOTIFICACAO] Gerada para motorista ${motorista.id}: ${tipo}`);
  } catch(e) {
    console.error('[NOTIFICACAO-ERRO]', e.message);
  }
}

// Rodar algoritmo a cada 24h (e imediatamente ao iniciar)
rodarAlgoritmoMotorista();
setInterval(rodarAlgoritmoMotorista, 24 * 60 * 60 * 1000);

// Rota para forçar análise manual (admin)
app.post('/api/v1/admin/rodar-algoritmo', async (req, res) => {
  rodarAlgoritmoMotorista();
  res.json({ message: 'Algoritmo iniciado!' });
});

// Rota para ver perfil de inteligência do motorista
app.get('/api/v1/motorista/perfil-inteligencia/:id', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('smart_perfil_inteligencia')
      .select('*')
      .eq('motorista_id', req.params.id)
      .single();
    if (error) throw error;
    res.json(data);
  } catch(e) {
    res.status(500).json({ error: 'Erro ao buscar perfil' });
  }
});

// Rota para ver notificações do motorista
app.get('/api/v1/motorista/notificacoes/:id', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('smart_notificacoes')
      .select('*')
      .eq('motorista_id', req.params.id)
      .order('created_at', { ascending: false });
    if (error) throw error;
    res.json(data || []);
  } catch(e) {
    res.status(500).json({ error: 'Erro ao buscar notificações' });
  }
});

app.post('/api/v1/enviar-alerta', async (req, res) => {
  try {
    const { tipo_alerta, motorista_id, lat, lng } = req.body;
    if (!tipo_alerta || !motorista_id) {
      return res.status(400).json({ error: 'Campos obrigatórios ausentes' });
    }
    const { data, error } = await supabase
      .from('smart_alertas_comunidade')
      .insert([{
        motorista_id,
        tipo_alerta,
        lat: lat || null,
        lng: lng || null,
        created_at: new Date().toISOString()
      }])
      .select()
      .single();
    if (error) throw error;
    // Check if broadcast exists before calling
    if (typeof broadcast === 'function') {
      broadcast({ type: 'novo_alerta_comunidade', alerta: data });
    }
    res.json({ message: 'Alerta enviado com sucesso', data });
  } catch(e) {
    console.error('[ALERTA-ERRO]', e.message);
    res.status(500).json({ error: 'Erro ao enviar alerta' });
  }
});

app.get('/api/v1/motorista/relatorio', async (req, res) => {
  try {
    const { motorista_id, dias, inicio, fim } = req.query;
    if (!motorista_id) return res.status(400).json({ error: 'motorista_id obrigatório' });

    let startDate, endDate;
    const now = new Date();

    if (dias) {
      startDate = new Date(now.getTime() - parseInt(dias) * 24 * 60 * 60 * 1000);
      endDate = now;
    } else if (inicio && fim) {
      startDate = new Date(inicio);
      endDate = new Date(fim);
      endDate.setHours(23, 59, 59);
    } else {
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      endDate = now;
    }

    const { data: corridas } = await supabase
      .from('smartmob_historico_precos')
      .select('*')
      .eq('motorista_id', motorista_id)
      .gte('created_at', startDate.toISOString())
      .lte('created_at', endDate.toISOString())
      .order('created_at', { ascending: true });

    const lista = corridas || [];
    const totalCorridas = lista.length;
    const fatBruto = lista.reduce((s, c) => s + (parseFloat(c.valor_corrida) || 0), 0);
    const kmRodados = lista.reduce((s, c) => s + (parseFloat(c.km) || 0), 0);
    const lucroLiquido = lista.reduce((s, c) => s + (parseFloat(c.ganho_liquido) || 0), 0);
    const custos = fatBruto - lucroLiquido;

    // Agrupar por dia
    const porDia = {};
    lista.forEach(c => {
      const dia = new Date(c.created_at);
      const label = dia.toLocaleDateString('pt-BR', { weekday: 'short', day: '2-digit', month: '2-digit' });
      if (!porDia[label]) porDia[label] = { label, lucro: 0, corridas: 0 };
      porDia[label].lucro += parseFloat(c.ganho_liquido) || 0;
      porDia[label].corridas += 1;
    });

    const porDiaArr = Object.values(porDia);
    const melhorDia = porDiaArr.sort((a, b) => b.lucro - a.lucro)[0];

    res.json({
      total_corridas: totalCorridas,
      faturamento_bruto: Math.round(fatBruto * 100) / 100,
      custos: Math.round(custos * 100) / 100,
      lucro_liquido: Math.round(lucroLiquido * 100) / 100,
      km_rodados: Math.round(kmRodados * 10) / 10,
      melhor_dia: melhorDia ? `${melhorDia.label} — R$ ${melhorDia.lucro.toFixed(2)}` : '-',
      por_dia: porDiaArr.sort((a, b) => a.label.localeCompare(b.label))
    });
  } catch(e) {
    console.error('[RELATORIO-ERRO]', e.message);
    res.status(500).json({ error: 'Erro ao gerar relatório' });
  }
});

// ─── START ──────────────────────────────────────────────────────────────────── 
server.listen(PORT, () => { 
  console.log(`\n🚀 SmartMob Server rodando em http://localhost:${PORT}`); 
  console.log(`📊 Dashboard Admin: http://localhost:${PORT}/admin`); 
  console.log(`🏪 Portal Parceiro: http://localhost:${PORT}/`);
  console.log(`🔍 Rota Debug: http://localhost:${PORT}/api/v1/debug\n`); 
});
