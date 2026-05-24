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
        cpf: cpf || `${Math.floor(Math.random() * 900 + 100)}.${Math.floor(Math.random() * 900 + 100)}.${Math.floor(Math.random() * 900 + 100)}-${Math.floor(Math.random() * 90 + 10)}`, // Dummy CPF único para testes
        marca: marcaVeiculo, 
        modelo: modeloVeiculo, 
        ano_carro, 
        placa_carro, 
        cor_carro, 
        versao_termo_aceito: versao_termo || termosData.versao 
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
    console.error('Erro no cadastro:', error); 
    res.status(500).json({ error: 'Erro interno do servidor', details: error.message }); 
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

// ─── START ──────────────────────────────────────────────────────────────────── 
server.listen(PORT, () => { 
  console.log(`\n🚀 SmartMob Server rodando em http://localhost:${PORT}`); 
  console.log(`📊 Dashboard Admin: http://localhost:${PORT}/admin`); 
  console.log(`🏪 Portal Parceiro: http://localhost:${PORT}/`);
  console.log(`🔍 Rota Debug: http://localhost:${PORT}/api/v1/debug\n`); 
});
