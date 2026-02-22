# apis-laravel-vieiracred

Projeto Laravel para APIs da VieiraCred.

## APIs existentes

- `GET /api/db-health`
  - Faz checagem de conectividade (somente leitura) nos 3 SQL Server configurados:
    - `sqlsrv_kinghost_vps`
    - `sqlsrv_hostinger_vps`
    - `sqlsrv_servidor_planejamento`
  - Executa `SELECT 1` em cada conex?o e retorna status (`up`/`down`) com lat?ncia e erro (quando houver).

- `GET /api/consulta-v8/run`
  - Executa 1 ciclo manual de consulta V8 (sem loop interno por endpoint).
  - Protegido por lock (`consulta-v8-manual-run`) para evitar concorr?ncia.
  - Busca logins em `[consultas_v8].[dbo].[limites_v8]` e aplica limite por login (`consultados < total`).
  - Se o limite zerar, reseta `consultados` ap?s 1 hora de `updated_at`.
  - Busca clientes pendentes em `[consultas_v8].[dbo].[consulta_v8]`.
  - Processa consulta na V8, faz `MERGE` no banco e espera 5s entre clientes.
  - Retorna resumo da execu??o (`total_logins`, `clientes_processados`, `clientes_erro`, etc.).

- `POST /api/consulta-v8`
  - Enfileira cliente na tabela `[consultas_v8].[dbo].[consulta_v8]`.
  - Obrigat?rios:
    - `cliente_cpf`
    - `cliente_nome`
    - `id_user`
    - `id_equipe`
    - `id_role`
  - Opcionais: `nascimento`, `telefone`, `cliente_sexo`, `email`.
  - Defaults/regras:
    - `nascimento`: `1996-05-15`
    - `cliente_sexo`: `male`
    - `status`: `pendente`
    - `created_at`: `SYSDATETIME()`
    - `email`: `naotem@gmail.com` quando n?o informado
    - `telefone`: se ausente/inv?lido, gera autom?tico v?lido no intervalo `11911111111..99999999999` com 3o d?gito igual a `9`
    - `cliente_nome`: normalizado para UPPER e sem acentua??o

- `GET /api/consulta-v8/limites`
  - Retorna `TOP (1000)` da tabela `[consultas_v8].[dbo].[limites_v8]` com todas as colunas:
    - `id`, `email`, `senha`, `total`, `consultados`, `limite`, `created_at`, `updated_at`

- `GET /api/consulta-v8/consultas`
  - Retorna `TOP (1000)` da tabela `[consultas_v8].[dbo].[consulta_v8]` com todas as colunas:
    - `id`, `cliente_cpf`, `cliente_sexo`, `nascimento`, `cliente_nome`, `email`, `telefone`, `created_at`, `status`, `status_consulta_v8`, `valor_liberado`, `descricao_v8`, `id_user`, `id_equipe`, `id_roles`

## PM2 (produ??o)

- Processo `api-consultaV8` configurado para chamar `GET /api/consulta-v8/run` a cada 10 segundos.
- Script: `/root/api-consultaV8/poller-consulta-v8.sh`
- Logs:
  - `pm2 logs api-consultaV8`
- O poller registra status HTTP e resumo retornado pelo endpoint quando dispon?vel.

## Como rodar

1. Instale as depend?ncias:
   - `composer install`
2. Suba a aplica??o:
   - `php artisan serve`
3. Teste no navegador/Postman:
   - `http://127.0.0.1:8000/api/db-health`

## Conex?es SQL Server

As conex?es est?o em:

- `config/database.php`
- `.env`

O projeto est? preparado para m?ltiplas conex?es SQL Server usando o driver `sqlsrv`.
