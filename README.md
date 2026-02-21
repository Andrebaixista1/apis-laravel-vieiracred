# apis-laravel-vieiracred

Projeto Laravel para APIs da VieiraCred.

## APIs existentes

- `GET /api/db-health`
  - Faz checagem de conectividade (somente leitura) nos 3 SQL Server configurados:
    - `sqlsrv_kinghost_vps`
    - `sqlsrv_hostinger_vps`
    - `sqlsrv_servidor_planejamento`
  - Executa `SELECT 1` em cada conexão e retorna status (`up`/`down`) com latência e erro (quando houver).

- `GET /api/consulta-v8/run`
  - Executa 1 ciclo manual de consulta V8 (sem loop).
  - Busca logins em `[consultas_v8].[dbo].[limites_v8]` e aplica limite por login (`consultados < total`).
  - Se o limite zerar, reseta `consultados` após 1 hora de `updated_at`.
  - Busca clientes pendentes em `[consultas_v8].[dbo].[consulta_v8]`.
  - Processa consulta na V8, faz `MERGE` no banco e espera 5s entre clientes.

## Como rodar

1. Instale as dependências:
   - `composer install`
2. Suba a aplicação:
   - `php artisan serve`
3. Teste no navegador/Postman:
   - `http://127.0.0.1:8000/api/db-health`

## Conexões SQL Server

As conexões estão em:

- `config/database.php`
- `.env`

O projeto está preparado para múltiplas conexões SQL Server usando o driver `sqlsrv`.
