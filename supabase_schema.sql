
-- Script SQL para criação das tabelas da plataforma Smart - ELVIVA GROUP LTDA
-- CNPJ: 62.444.354/0001-82

-- Tabela: smart_apps (Armazena os apps suportados)
CREATE TABLE IF NOT EXISTS smart_apps (
    id BIGSERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    package_name VARCHAR(100) NOT NULL,
    ativo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabela: smart_termos (Armazena as versões do contrato)
CREATE TABLE IF NOT EXISTS smart_termos (
    id BIGSERIAL PRIMARY KEY,
    versao VARCHAR(20) NOT NULL,
    texto TEXT NOT NULL,
    criado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabela: smart_motoristas_cadastro (Cadastro de motoristas)
CREATE TABLE IF NOT EXISTS smart_motoristas_cadastro (
    id BIGSERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    celular VARCHAR(20) NOT NULL,
    email VARCHAR(100) NOT NULL,
    cpf VARCHAR(14) NOT NULL UNIQUE,
    marca VARCHAR(50) NOT NULL,
    modelo VARCHAR(50) NOT NULL,
    ano_carro INTEGER NOT NULL,
    placa_carro VARCHAR(10) NOT NULL,
    cor_carro VARCHAR(30) NOT NULL,
    versao_termo_aceito VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabela: smart_auditoria_aceites (Registra as assinaturas digitais)
CREATE TABLE IF NOT EXISTS smart_auditoria_aceites (
    id BIGSERIAL PRIMARY KEY,
    motorista_id BIGINT NOT NULL REFERENCES smart_motoristas_cadastro(id),
    data_aceite TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    termo_texto_completo TEXT NOT NULL,
    hash_sha256 VARCHAR(64) NOT NULL
);

-- Tabela: smart_parceiros_tokens (Registra os cupons/tokens)
CREATE TABLE IF NOT EXISTS smart_parceiros_tokens (
    id BIGSERIAL PRIMARY KEY,
    token_codigo VARCHAR(6) NOT NULL,
    motorista_id BIGINT NOT NULL REFERENCES smart_motoristas_cadastro(id),
    parceiro_id BIGINT,
    validade TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ativo',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT check_status CHECK (status IN ('ativo', 'utilizado', 'expirado'))
);
