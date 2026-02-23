<?php

namespace App\Http\Controllers;

use Carbon\Carbon;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\Client\PendingRequest;

class ConsultaV8Controller extends Controller
{
    private const TOKEN_URL_DEFAULT = 'https://auth.v8sistema.com/oauth/token';
    private const AUDIENCE_DEFAULT = 'https://bff.v8sistema.com';
    private const SCOPE_DEFAULT = 'online_access';
    private const CLIENT_ID_DEFAULT = 'DHWogdaYmEI8n5bwwxPDzulMlSK7dwIn';
    private const GRANT_TYPE_DEFAULT = 'password';
    private const BFF_BASE_URL_DEFAULT = 'https://bff.v8sistema.com';
    private const PROVIDER_DEFAULT = 'QI';
    private const HTTP_TIMEOUT_SECONDS = 30;
    private const FINAL_GET_MAX_ATTEMPTS = 5;
    private const DEFAULT_CLIENT_BIRTH_DATE = '1996-05-15';
    private const DEFAULT_CLIENT_SEX = 'male';
    private const DEFAULT_CLIENT_STATUS = 'pendente';
    private const HOLD_CLIENT_STATUS = 'adicionando';
    private const RETRYABLE_V8_STATUSES = [
        'WAITING_CONSENT',
        'WAITING_CONSULT',
        'WAITING_CREDIT_ANALYSIS',
        'CONSENT_APPROVED',
    ];

    public function run(Request $request): JsonResponse
    {
        $lock = cache()->lock('consulta-v8-manual-run', 3600);

        if (! $lock->get()) {
            return response()->json([
                'ok' => false,
                'message' => 'Ja existe uma execucao em andamento.',
            ], 409);
        }

        $startedAt = microtime(true);
        $summary = [
            'ok' => true,
            'started_at' => now()->toIso8601String(),
            'finished_at' => null,
            'duration_ms' => 0,
            'total_logins' => 0,
            'logins_com_limite' => 0,
            'pendentes_encontrados' => 0,
            'clientes_distribuidos' => 0,
            'clientes_processados' => 0,
            'clientes_erro' => 0,
            'duplicados_criados' => 0,
            'logins' => [],
        ];

        try {
            $accounts = $this->loadAccountsWithAvailableLimit();
            $summary['total_logins'] = count($accounts);

            if (empty($accounts)) {
                $summary['finished_at'] = now()->toIso8601String();
                $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);
                $summary['message'] = 'Nenhum login com limite disponivel no momento.';

                return response()->json($summary);
            }

            $summary['logins_com_limite'] = count($accounts);
            $totalCapacity = array_sum(array_column($accounts, 'remaining'));

            $pendingClients = $this->loadPendingClients($totalCapacity);
            $summary['pendentes_encontrados'] = count($pendingClients);

            $distribution = $this->distributeClientsAcrossAccounts($pendingClients, $accounts);
            $summary['clientes_distribuidos'] = array_sum(array_map('count', $distribution));

            foreach ($accounts as $account) {
                $accountClients = $distribution[$account['id']] ?? [];
                $loginSummary = [
                    'id' => $account['id'],
                    'email' => $account['email'],
                    'limite_restante_inicio' => $account['remaining'],
                    'clientes_alocados' => count($accountClients),
                    'clientes_processados' => 0,
                    'clientes_erro' => 0,
                ];

                if (empty($accountClients)) {
                    $summary['logins'][] = $loginSummary;
                    continue;
                }

                try {
                    $accessToken = $this->requestV8AccessToken($account['email'], $account['senha']);
                } catch (\Throwable $e) {
                    $loginSummary['clientes_erro'] = count($accountClients);
                    $summary['clientes_erro'] += count($accountClients);
                    $loginSummary['erro_token'] = mb_substr($e->getMessage(), 0, 300);
                    $summary['logins'][] = $loginSummary;
                    continue;
                }

                foreach ($accountClients as $index => $client) {
                    try {
                        $result = $this->processClient($accessToken, $client, $account['id']);
                        $summary['clientes_processados']++;
                        $summary['duplicados_criados'] += $result['duplicates_created'];
                        $loginSummary['clientes_processados']++;
                        if (!empty($result['should_increment_limit'])) {
                            $this->incrementConsultedCounter($account['id']);
                        }
                    } catch (\Throwable $e) {
                        $summary['clientes_erro']++;
                        $loginSummary['clientes_erro']++;
                        $this->markClientAsError((int) $client->id, $e->getMessage());
                    } finally {
                        $hasNextClient = $index < (count($accountClients) - 1);
                        if ($hasNextClient) {
                            sleep(5);
                        }
                    }
                }

                $summary['logins'][] = $loginSummary;
            }

            $summary['finished_at'] = now()->toIso8601String();
            $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

            return response()->json($summary);
        } catch (\Throwable $e) {
            $summary['ok'] = false;
            $summary['message'] = $e->getMessage();
            $summary['finished_at'] = now()->toIso8601String();
            $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

            return response()->json($summary, 500);
        } finally {
            optional($lock)->release();
        }
    }


    public function store(Request $request): JsonResponse
    {
        $batchRows = $this->extractBatchRowsFromRequest($request);

        if ($batchRows !== null) {
            if (empty($batchRows)) {
                return response()->json([
                    'ok' => false,
                    'message' => 'Informe pelo menos uma linha no lote.',
                ], 422);
            }

            if (count($batchRows) > 5000) {
                return response()->json([
                    'ok' => false,
                    'message' => 'Lote acima do limite permitido de 5000 linhas por requisicao.',
                ], 422);
            }

            $defaults = [
                'id_user' => $request->input('id_user'),
                'id_equipe' => $request->input('id_equipe'),
                'id_role' => $request->input('id_role', $request->input('id_roles')),
                'cliente_sexo' => $request->input('cliente_sexo'),
                'email' => $request->input('email'),
                'tipoConsulta' => $request->input('tipoConsulta'),
                'hold_pending' => $request->input('hold_pending'),
            ];

            $payloads = [];
            foreach ($batchRows as $index => $row) {
                if (! is_array($row)) {
                    return response()->json([
                        'ok' => false,
                        'message' => 'Linha '.($index + 1).' invalida: formato de objeto esperado.',
                    ], 422);
                }

                try {
                    $payloads[] = $this->buildStorePayloadFromInput($row, $defaults);
                } catch (\InvalidArgumentException $e) {
                    return response()->json([
                        'ok' => false,
                        'message' => 'Linha '.($index + 1).': '.$e->getMessage(),
                    ], 422);
                }
            }

            $insertedIds = $this->insertConsultaRows($payloads);
            $holdPendingCount = count(array_filter(
                $payloads,
                static fn (array $payload): bool => $payload['status'] === self::HOLD_CLIENT_STATUS
            ));

            return response()->json([
                'ok' => true,
                'message' => 'Lote enfileirado para consulta V8.',
                'data' => [
                    'mode' => 'batch',
                    'inserted_count' => count($insertedIds),
                    'hold_pending_count' => $holdPendingCount,
                    'pendente_count' => count($insertedIds) - $holdPendingCount,
                    'ids' => $insertedIds,
                    'created_at' => now()->toIso8601String(),
                ],
            ], 201);
        }

        try {
            $payload = $this->buildStorePayloadFromInput($request->all());
        } catch (\InvalidArgumentException $e) {
            return response()->json([
                'ok' => false,
                'message' => $e->getMessage(),
            ], 422);
        }

        $insertedIds = $this->insertConsultaRows([$payload]);
        $insertedId = (int) ($insertedIds[0] ?? 0);
        $holdPending = $payload['status'] === self::HOLD_CLIENT_STATUS;

        return response()->json([
            'ok' => true,
            'message' => 'Cliente enfileirado para consulta V8.',
            'data' => [
                'id' => $insertedId,
                'cliente_cpf' => $payload['cliente_cpf'],
                'cliente_nome' => $payload['cliente_nome'],
                'status' => $payload['status'],
                'tipoConsulta' => $payload['tipoConsulta'],
                'hold_pending' => $holdPending,
                'created_at' => now()->toIso8601String(),
            ],
        ], 201);
    }

    public function releasePendingByScope(Request $request): JsonResponse
    {
        $validated = $request->validate([
            'id_user' => ['required', 'integer'],
            'id_equipe' => ['required', 'integer'],
            'tipoConsulta' => ['nullable', 'string', 'max:255'],
            'ids' => ['nullable', 'array'],
            'ids.*' => ['integer', 'min:1'],
        ]);

        $idUser = (int) $validated['id_user'];
        $idEquipe = (int) $validated['id_equipe'];
        $tipoConsulta = trim((string) ($validated['tipoConsulta'] ?? ''));
        $ids = array_values(array_unique(array_map(
            static fn ($id) => (int) $id,
            array_filter($validated['ids'] ?? [], static fn ($id) => (int) $id > 0)
        )));

        if (empty($ids) && $tipoConsulta === '') {
            return response()->json([
                'ok' => false,
                'message' => 'Informe ids ou tipoConsulta para liberar pendentes.',
            ], 422);
        }

        $where = [
            '[id_user] = ?',
            '[id_equipe] = ?',
        ];
        $bindings = [$idUser, $idEquipe];

        if (! empty($ids)) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = "[id] IN ($placeholders)";
            $bindings = array_merge($bindings, $ids);
        } else {
            $where[] = '[tipoConsulta] = ?';
            $bindings[] = $tipoConsulta;
        }

        $whereSql = implode(' AND ', $where);
        $released = 0;
        $connection = DB::connection('sqlsrv_kinghost_vps');

        if (! empty($ids)) {
            foreach (array_chunk($ids, 1000) as $idsChunk) {
                $chunkWhere = [
                    '[id_user] = ?',
                    '[id_equipe] = ?',
                ];
                $chunkBindings = [$idUser, $idEquipe];

                $chunkPlaceholders = implode(',', array_fill(0, count($idsChunk), '?'));
                $chunkWhere[] = "[id] IN ($chunkPlaceholders)";
                $chunkBindings = array_merge($chunkBindings, $idsChunk);

                $chunkWhereSql = implode(' AND ', $chunkWhere);
                $released += (int) $connection->update("
                    UPDATE [consultas_v8].[dbo].[consulta_v8]
                    SET [status] = ?
                    WHERE $chunkWhereSql
                      AND UPPER(LTRIM(RTRIM(COALESCE([status], '')))) IN ('ADICIONANDO', 'EM INSERCAO', 'EM_INSERCAO', 'AGUARDANDO INSERCAO', 'AGUARDANDO_INSERCAO')
                ", array_merge([self::DEFAULT_CLIENT_STATUS], $chunkBindings));
            }
        } else {
            $released = (int) $connection->update("
                UPDATE [consultas_v8].[dbo].[consulta_v8]
                SET [status] = ?
                WHERE $whereSql
                  AND UPPER(LTRIM(RTRIM(COALESCE([status], '')))) IN ('ADICIONANDO', 'EM INSERCAO', 'EM_INSERCAO', 'AGUARDANDO INSERCAO', 'AGUARDANDO_INSERCAO')
            ", array_merge([self::DEFAULT_CLIENT_STATUS], $bindings));
        }

        return response()->json([
            'ok' => true,
            'message' => $released > 0
                ? 'Registros liberados para pendente.'
                : 'Nenhum registro em status de insercao encontrado para liberar.',
            'released_count' => (int) $released,
            'filters' => [
                'id_user' => $idUser,
                'id_equipe' => $idEquipe,
                'tipoConsulta' => $tipoConsulta !== '' ? $tipoConsulta : null,
                'ids' => $ids,
            ],
        ]);
    }


    public function listLimites(Request $request): JsonResponse
    {
        $userId = $this->toNullableInt($request->query('id_user'));
        $equipeId = $this->toNullableInt($request->query('id_equipe'));

        $rows = DB::connection('sqlsrv_kinghost_vps')->select("
            SELECT TOP (1000)
                [id],
                [email],
                [senha],
                [total],
                [consultados],
                [limite],
                [created_at],
                [updated_at]
            FROM [consultas_v8].[dbo].[limites_v8]
            ORDER BY [id] DESC
        ");

        $rows = array_map(static fn($row) => (array) $row, $rows);

        $filtered = $this->filterAccountsForUser($rows, $userId, $equipeId);

        return response()->json([
            'ok' => true,
            'total' => count($filtered),
            'data' => array_values($filtered),
        ]);
    }

    public function listConsultas(Request $request): JsonResponse
    {
        $userId = $this->toNullableInt($request->query('id_user'));
        $cpf = preg_replace('/\D+/', '', (string) $request->query('cpf', ''));
        $nome = $this->normalizeClientName((string) $request->query('nome', ''));

        $hasTokenUsadoColumn = false;
        try {
            $columnCheck = DB::connection('sqlsrv_kinghost_vps')->selectOne("
                SELECT CASE
                    WHEN COL_LENGTH('consultas_v8.dbo.consulta_v8', 'token_usado') IS NULL THEN 0
                    ELSE 1
                END AS [has_token_usado]
            ");
            $hasTokenUsadoColumn = (int) ($columnCheck->has_token_usado ?? 0) === 1;
        } catch (\Throwable $e) {
            $hasTokenUsadoColumn = false;
        }

        $where = [];
        $bindings = [];

        if ($userId !== null && $userId !== 1) {
            $where[] = "[id_user] = ?";
            $bindings[] = $userId;
        }

        if ($cpf !== '') {
            $where[] = "REPLACE(REPLACE(REPLACE(COALESCE([cliente_cpf], ''), '.', ''), '-', ''), ' ', '') = ?";
            $bindings[] = $cpf;
        }

        if ($nome !== '') {
            $where[] = "UPPER(LTRIM(RTRIM(COALESCE([cliente_nome], '')))) = ?";
            $bindings[] = $nome;
        }

        $whereSql = empty($where) ? '' : 'WHERE '.implode(' AND ', $where);
        $tokenUsadoSelect = $hasTokenUsadoColumn
            ? '[token_usado]'
            : 'CAST(NULL AS NVARCHAR(255)) AS [token_usado]';

        $sql = sprintf("
            SELECT TOP (1000)
                [id],
                [cliente_cpf],
                [cliente_sexo],
                [nascimento],
                [cliente_nome],
                [email],
                [telefone],
                [created_at],
                [status],
                [status_consulta_v8],
                [valor_liberado],
                [descricao_v8],
                [tipoConsulta],
                %s,
                [id_user],
                [id_equipe],
                [id_roles]
            FROM [consultas_v8].[dbo].[consulta_v8]
            %s
            ORDER BY [id] DESC
        ", $tokenUsadoSelect, $whereSql);

        $rows = DB::connection('sqlsrv_kinghost_vps')->select($sql, $bindings);

        return response()->json([
            'ok' => true,
            'total' => count($rows),
            'data' => $rows,
        ]);
    }

    public function deleteConsultasByLote(Request $request): JsonResponse
    {
        $requesterUserId = $this->toNullableInt($request->input('id_user'));
        $tipoConsulta = trim((string) $request->input('tipoConsulta', ''));

        if ($requesterUserId === null || $requesterUserId <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'id_user invalido.',
            ], 422);
        }

        if ($tipoConsulta === '') {
            return response()->json([
                'ok' => false,
                'message' => 'tipoConsulta invalido.',
            ], 422);
        }

        $targetUserId = $this->toNullableInt($request->input('target_id_user'));
        $targetEquipeId = $this->toNullableInt($request->input('target_id_equipe'));

        if ($requesterUserId === 1) {
            $where = ['[tipoConsulta] = ?'];
            $bindings = [$tipoConsulta];

            if ($targetUserId !== null && $targetUserId > 0) {
                $where[] = '[id_user] = ?';
                $bindings[] = $targetUserId;
            }
            if ($targetEquipeId !== null && $targetEquipeId > 0) {
                $where[] = '[id_equipe] = ?';
                $bindings[] = $targetEquipeId;
            }

            $deleted = DB::connection('sqlsrv_kinghost_vps')->delete("
                DELETE FROM [consultas_v8].[dbo].[consulta_v8]
                WHERE ".implode(' AND ', $where)."
            ", $bindings);

            return response()->json([
                'ok' => true,
                'message' => $deleted > 0
                    ? 'Lote removido com sucesso.'
                    : 'Nenhum registro encontrado para os filtros informados.',
                'deleted_count' => (int) $deleted,
                'filters' => [
                    'requester_id_user' => $requesterUserId,
                    'target_id_user' => $targetUserId,
                    'target_id_equipe' => $targetEquipeId,
                    'tipoConsulta' => $tipoConsulta,
                ],
            ]);
        }

        $idEquipe = $this->toNullableInt($request->input('id_equipe'));
        if ($idEquipe === null || $idEquipe <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'id_equipe invalido.',
            ], 422);
        }

        $deleted = DB::connection('sqlsrv_kinghost_vps')->delete("
            DELETE FROM [consultas_v8].[dbo].[consulta_v8]
            WHERE [id_user] = ?
              AND [id_equipe] = ?
              AND [tipoConsulta] = ?
        ", [$requesterUserId, $idEquipe, $tipoConsulta]);

        return response()->json([
            'ok' => true,
            'message' => $deleted > 0
                ? 'Lote removido com sucesso.'
                : 'Nenhum registro encontrado para os filtros informados.',
            'deleted_count' => (int) $deleted,
            'filters' => [
                'id_user' => $requesterUserId,
                'id_equipe' => $idEquipe,
                'tipoConsulta' => $tipoConsulta,
            ],
        ]);
    }

    private function extractBatchRowsFromRequest(Request $request): ?array
    {
        $rows = $request->input('rows');
        if (is_array($rows)) {
            return $rows;
        }

        $items = $request->input('items');
        if (is_array($items)) {
            return $items;
        }

        $data = $request->input('data');
        if (is_array($data) && array_is_list($data)) {
            return $data;
        }

        $all = $request->all();
        if (is_array($all) && array_is_list($all)) {
            return $all;
        }

        return null;
    }

    private function buildStorePayloadFromInput(array $input, array $defaults = []): array
    {
        $cpfRaw = $input['cliente_cpf'] ?? $input['cpf'] ?? $defaults['cliente_cpf'] ?? $defaults['cpf'] ?? '';
        $nomeRaw = $input['cliente_nome'] ?? $input['nome'] ?? $defaults['cliente_nome'] ?? $defaults['nome'] ?? '';
        $idUserRaw = $input['id_user'] ?? $defaults['id_user'] ?? null;
        $idEquipeRaw = $input['id_equipe'] ?? $input['equipe_id'] ?? $defaults['id_equipe'] ?? $defaults['equipe_id'] ?? null;
        $idRoleRaw = $input['id_role'] ?? $input['id_roles'] ?? $defaults['id_role'] ?? $defaults['id_roles'] ?? null;

        $cpf = preg_replace('/\D+/', '', (string) $cpfRaw);
        $nome = $this->normalizeClientName((string) $nomeRaw);
        $idUser = $this->toNullableInt($idUserRaw);
        $idEquipe = $this->toNullableInt($idEquipeRaw);
        $idRole = $this->toNullableInt($idRoleRaw);

        if ($cpf === '') {
            throw new \InvalidArgumentException('cliente_cpf e obrigatorio.');
        }

        if ($nome === '') {
            throw new \InvalidArgumentException('cliente_nome e obrigatorio.');
        }

        if ($idUser === null || $idUser <= 0) {
            throw new \InvalidArgumentException('id_user e obrigatorio.');
        }

        if ($idEquipe === null || $idEquipe <= 0) {
            throw new \InvalidArgumentException('id_equipe e obrigatorio.');
        }

        if ($idRole === null || $idRole <= 0) {
            throw new \InvalidArgumentException('id_role e obrigatorio.');
        }

        $nascimento = $this->toBirthDate($input['nascimento'] ?? $defaults['nascimento'] ?? null);
        if ($nascimento === '') {
            $nascimento = self::DEFAULT_CLIENT_BIRTH_DATE;
        }

        $telefoneDigits = preg_replace('/\D+/', '', (string) ($input['telefone'] ?? $defaults['telefone'] ?? ''));
        if (! $this->isValidBrazilCellPhone($telefoneDigits)) {
            $telefoneDigits = $this->generateRandomPhoneNumber();
        }

        $clienteSexo = trim((string) ($input['cliente_sexo'] ?? $defaults['cliente_sexo'] ?? self::DEFAULT_CLIENT_SEX));
        if ($clienteSexo === '') {
            $clienteSexo = self::DEFAULT_CLIENT_SEX;
        }

        $email = $this->toNullableString($input['email'] ?? $defaults['email'] ?? null);
        if ($email === null) {
            $email = 'naotem@gmail.com';
        }

        $tipoConsulta = trim((string) ($input['tipoConsulta'] ?? $defaults['tipoConsulta'] ?? 'Individual'));
        if ($tipoConsulta === '') {
            $tipoConsulta = 'Individual';
        }

        $holdPending = $this->toBoolean($input['hold_pending'] ?? $defaults['hold_pending'] ?? null, false);

        return [
            'cliente_cpf' => mb_substr($cpf, 0, 20),
            'cliente_sexo' => mb_substr($clienteSexo, 0, 20),
            'nascimento' => $nascimento,
            'cliente_nome' => mb_substr($nome, 0, 255),
            'email' => mb_substr($email, 0, 255),
            'telefone' => mb_substr($telefoneDigits, 0, 20),
            'status' => $holdPending ? self::HOLD_CLIENT_STATUS : self::DEFAULT_CLIENT_STATUS,
            'tipoConsulta' => mb_substr($tipoConsulta, 0, 255),
            'id_user' => $idUser,
            'id_equipe' => $idEquipe,
            'id_roles' => $idRole,
        ];
    }

    private function insertConsultaRows(array $payloads): array
    {
        if (empty($payloads)) {
            return [];
        }

        $insertedIds = [];
        $connection = DB::connection('sqlsrv_kinghost_vps');
        $chunkSize = 150;

        $connection->transaction(function () use ($connection, $payloads, $chunkSize, &$insertedIds): void {
            foreach (array_chunk($payloads, $chunkSize) as $chunk) {
                $valuesSql = [];
                $bindings = [];

                foreach ($chunk as $payload) {
                    $valuesSql[] = '(?, ?, ?, ?, ?, ?, SYSDATETIME(), ?, NULL, NULL, NULL, ?, ?, ?, ?)';
                    $bindings[] = $payload['cliente_cpf'];
                    $bindings[] = $payload['cliente_sexo'];
                    $bindings[] = $payload['nascimento'];
                    $bindings[] = $payload['cliente_nome'];
                    $bindings[] = $payload['email'];
                    $bindings[] = $payload['telefone'];
                    $bindings[] = $payload['status'];
                    $bindings[] = $payload['tipoConsulta'];
                    $bindings[] = $payload['id_user'];
                    $bindings[] = $payload['id_equipe'];
                    $bindings[] = $payload['id_roles'];
                }

                $sql = "
                    INSERT INTO [consultas_v8].[dbo].[consulta_v8] (
                        [cliente_cpf],
                        [cliente_sexo],
                        [nascimento],
                        [cliente_nome],
                        [email],
                        [telefone],
                        [created_at],
                        [status],
                        [status_consulta_v8],
                        [valor_liberado],
                        [descricao_v8],
                        [tipoConsulta],
                        [id_user],
                        [id_equipe],
                        [id_roles]
                    )
                    OUTPUT INSERTED.[id] AS [id]
                    VALUES ".implode(",\n", $valuesSql).";
                ";

                $rows = $connection->select($sql, $bindings);
                foreach ($rows as $row) {
                    $insertedIds[] = (int) ($row->id ?? 0);
                }
            }
        });

        return $insertedIds;
    }

    private function isValidBrazilCellPhone(string $digits): bool
    {
        return strlen($digits) === 11
            && substr($digits, 2, 1) === '9'
            && $digits >= '11911111111'
            && $digits <= '99999999999';
    }

    private function toBoolean($value, bool $default = false): bool
    {
        if ($value === null) {
            return $default;
        }

        if (is_bool($value)) {
            return $value;
        }

        $parsed = filter_var($value, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
        return $parsed ?? $default;
    }

    private function loadAccountsWithAvailableLimit(): array
    {
        $rows = DB::connection('sqlsrv_kinghost_vps')->select("
            SELECT
                [id],
                [email],
                [senha],
                [total],
                [consultados],
                [limite],
                [created_at],
                [updated_at]
            FROM [consultas_v8].[dbo].[limites_v8]
            ORDER BY [id] ASC
        ");

        $accounts = [];
        $now = Carbon::now();

        foreach ($rows as $row) {
            $email = trim((string) ($row->email ?? ''));
            $senha = trim((string) ($row->senha ?? ''));
            $total = max(0, (int) ($row->total ?? 0));
            $consultados = max(0, (int) ($row->consultados ?? 0));
            $remaining = max(0, $total - $consultados);

            if ($remaining <= 0 && $total > 0) {
                $updatedAt = $row->updated_at ? Carbon::parse($row->updated_at) : null;
                $canReset = $updatedAt === null || $updatedAt->lte($now->copy()->subHour());

                if ($canReset) {
                    DB::connection('sqlsrv_kinghost_vps')->update("
                        UPDATE [consultas_v8].[dbo].[limites_v8]
                        SET [consultados] = 0, [updated_at] = SYSDATETIME()
                        WHERE [id] = ?
                    ", [$row->id]);
                    $consultados = 0;
                    $remaining = $total;
                }
            }

            if ($remaining <= 0 || $email === '' || $senha === '') {
                continue;
            }

            $accounts[] = [
                'id' => (int) $row->id,
                'email' => $email,
                'senha' => $senha,
                'total' => $total,
                'consultados' => $consultados,
                'remaining' => $remaining,
            ];
        }

        return $accounts;
    }

    private function loadPendingClients(int $limit): array
    {
        $safeLimit = max(0, $limit);

        if ($safeLimit === 0) {
            return [];
        }

        return DB::connection('sqlsrv_kinghost_vps')->select("
            SELECT TOP ($safeLimit)
                [id],
                [cliente_cpf],
                [cliente_sexo],
                [nascimento],
                [cliente_nome],
                [email],
                [telefone],
                [status],
                [status_consulta_v8],
                [descricao_v8],
                [valor_liberado],
                [created_at],
                [tipoConsulta],
                [id_user],
                [id_equipe],
                [id_roles]
            FROM [consultas_v8].[dbo].[consulta_v8]
            WHERE UPPER(LTRIM(RTRIM(COALESCE([status], '')))) = 'PENDENTE'
            ORDER BY [id] ASC
        ");
    }

    private function distributeClientsAcrossAccounts(array $clients, array &$accounts): array
    {
        $distribution = [];
        foreach ($accounts as $account) {
            $distribution[$account['id']] = [];
        }

        if (empty($accounts) || empty($clients)) {
            return $distribution;
        }

        $userPointers = [];

        foreach ($clients as $client) {
            $clientUserId = (int) ($client->id_user ?? 0);
            $clientEquipeId = (int) ($client->id_equipe ?? 0);

            $allowedIndexes = $this->getAllowedAccountIndexes(
                $accounts,
                $clientUserId > 0 ? $clientUserId : null,
                $clientEquipeId > 0 ? $clientEquipeId : null
            );
            if (empty($allowedIndexes)) {
                continue;
            }

            $pointerKey = $clientUserId.'|'.$clientEquipeId;
            if (!isset($userPointers[$pointerKey])) {
                $userPointers[$pointerKey] = 0;
            }

            $totalAllowed = count($allowedIndexes);
            $tries = 0;
            $selectedIndex = null;

            while ($tries < $totalAllowed) {
                $candidatePosition = $userPointers[$pointerKey] % $totalAllowed;
                $accountIndex = $allowedIndexes[$candidatePosition];
                $userPointers[$pointerKey] = ($userPointers[$pointerKey] + 1) % $totalAllowed;
                $tries++;

                if ($accounts[$accountIndex]['remaining'] <= 0) {
                    continue;
                }

                $selectedIndex = $accountIndex;
                break;
            }

            if ($selectedIndex === null) {
                continue;
            }

            $accountId = $accounts[$selectedIndex]['id'];
            $distribution[$accountId][] = $client;
            $accounts[$selectedIndex]['remaining']--;
        }

        return $distribution;
    }

    private function requestV8AccessToken(string $username, string $password): string
    {
        $tokenUrl = (string) env('V8_AUTH_TOKEN_URL', self::TOKEN_URL_DEFAULT);
        $audience = (string) env('V8_AUTH_AUDIENCE', self::AUDIENCE_DEFAULT);
        $scope = (string) env('V8_AUTH_SCOPE', self::SCOPE_DEFAULT);
        $clientId = (string) env('V8_AUTH_CLIENT_ID', self::CLIENT_ID_DEFAULT);
        $grantType = (string) env('V8_AUTH_GRANT_TYPE', self::GRANT_TYPE_DEFAULT);
        $cookie = (string) env('V8_AUTH_COOKIE', '');

        $request = $this->baseHttpRequest()
            ->asForm();

        if ($cookie !== '') {
            $request = $request->withHeaders(['Cookie' => $cookie]);
        }

        $response = $request->post($tokenUrl, [
            'grant_type' => $grantType,
            'username' => $username,
            'password' => $password,
            'audience' => $audience,
            'scope' => $scope,
            'client_id' => $clientId,
        ]);

        $accessToken = trim((string) data_get($response->json(), 'access_token', ''));
        if (! $response->ok() || $accessToken === '') {
            $status = $response->status();
            $errorMessage = (string) (
                data_get($response->json(), 'error_description')
                ?? data_get($response->json(), 'error')
                ?? data_get($response->json(), 'message')
                ?? 'Falha ao gerar token no auth'
            );
            throw new \RuntimeException('Falha auth V8 (HTTP_'.$status.'): '.mb_substr($errorMessage, 0, 200));
        }

        return $accessToken;
    }

    private function processClient(string $accessToken, object $client, int $accountId): array
    {
        $consultId = null;
        $createStatus = null;
        $shouldIncrementLimit = false;

        $createResponse = $this->v8Http($accessToken)->post('/private-consignment/consult', [
            'borrowerDocumentNumber' => (string) ($client->cliente_cpf ?? ''),
            'gender' => (string) ($client->cliente_sexo ?? ''),
            'birthDate' => $this->toBirthDate($client->nascimento ?? null),
            'signerName' => (string) ($client->cliente_nome ?? ''),
            'signerEmail' => (string) ($client->email ?? ''),
            'signerPhone' => $this->parseSignerPhone((string) ($client->telefone ?? '')),
            'provider' => (string) env('V8_PROVIDER', self::PROVIDER_DEFAULT),
        ]);

        $createStatus = $createResponse->status();
        if ($createStatus >= 200 && $createStatus < 300) {
            $consultId = data_get($createResponse->json(), 'id');
        } elseif ($createStatus !== 400) {
            $this->markClientAsError((int) $client->id, 'API1 HTTP_'.$createStatus);
            return ['duplicates_created' => 0, 'should_increment_limit' => false];
        }

        if ($consultId) {
            $authorizeResponse = $this->v8Http($accessToken)
                ->post('/private-consignment/consult/'.$consultId.'/authorize', []);

            if ($authorizeResponse->status() === 200) {
                $shouldIncrementLimit = true;
            } elseif (! $authorizeResponse->ok()) {
                $this->markClientAsError((int) $client->id, 'API2 HTTP_'.$authorizeResponse->status());
            }
        }

        $entries = [];
        $lastError = null;
        $retrySleepSeconds = max(1, (int) env('V8_FINAL_GET_RETRY_SECONDS', 3));

        for ($attempt = 1; $attempt <= self::FINAL_GET_MAX_ATTEMPTS; $attempt++) {
            $resultResponse = $this->v8Http($accessToken)->get('/private-consignment/consult', [
                'startDate' => Carbon::now('UTC')->startOfDay()->format('Y-m-d\TH:i:s\Z'),
                'endDate' => Carbon::now('UTC')->endOfDay()->format('Y-m-d\TH:i:s\Z'),
                'limit' => 50,
                'page' => 1,
                'search' => (string) ($client->cliente_cpf ?? ''),
                'provider' => (string) env('V8_PROVIDER', self::PROVIDER_DEFAULT),
            ]);

            if (! $resultResponse->ok()) {
                $lastError = 'API3 HTTP_'.$resultResponse->status();
                if ($attempt < self::FINAL_GET_MAX_ATTEMPTS) {
                    sleep($retrySleepSeconds);
                    continue;
                }
                $this->markClientAsError((int) $client->id, $lastError);
                return ['duplicates_created' => 0, 'should_increment_limit' => $shouldIncrementLimit];
            }

            $data = data_get($resultResponse->json(), 'data', []);
            if (! is_array($data) || empty($data)) {
                $lastError = 'API3 sem dados';
                if ($attempt < self::FINAL_GET_MAX_ATTEMPTS) {
                    sleep($retrySleepSeconds);
                    continue;
                }
                $this->markClientAsError((int) $client->id, $lastError);
                return ['duplicates_created' => 0, 'should_increment_limit' => $shouldIncrementLimit];
            }

            $entries = $this->distinctResultEntries($data);
            if (empty($entries)) {
                $lastError = 'Sem entrada valida no retorno';
                if ($attempt < self::FINAL_GET_MAX_ATTEMPTS) {
                    sleep($retrySleepSeconds);
                    continue;
                }
                $this->markClientAsError((int) $client->id, $lastError);
                return ['duplicates_created' => 0, 'should_increment_limit' => $shouldIncrementLimit];
            }

            $allRetryable = ! empty($entries);
            foreach ($entries as $entry) {
                if (! $this->isRetryableV8Status($entry['status'])) {
                    $allRetryable = false;
                    break;
                }
            }

        if (! $allRetryable || $attempt === self::FINAL_GET_MAX_ATTEMPTS) {
            break;
        }

        sleep($retrySleepSeconds);
    }

        $duplicatesCreated = 0;

        $first = true;
        foreach ($entries as $entry) {
        $isRetryable = $this->isRetryableV8Status($entry['status']);
        $payload = [
            'status_consulta_v8' => $entry['status'],
                'descricao_v8' => $entry['description'],
                'valor_liberado' => $entry['available_margin'],
                'status' => $isRetryable ? 'Pendente' : 'Consultado',
            ];

            if ($first) {
                $this->mergeClientRow((int) $client->id, $payload);
                $first = false;
                continue;
            }

            $this->mergeClientRow((int) $client->id, $payload);
            $duplicatesCreated++;
        }

        return [
            'duplicates_created' => $duplicatesCreated,
            'should_increment_limit' => $shouldIncrementLimit,
        ];
    }

    private function distinctResultEntries(array $rawEntries): array
    {
        $distinct = [];
        $seen = [];

        foreach ($rawEntries as $item) {
            if (! is_array($item)) {
                continue;
            }

            $entry = [
                'status' => $this->toNullableString($item['status'] ?? null),
                'description' => $this->appendLinkToDescription(
                    $this->toNullableString($item['description'] ?? null),
                    $this->extractEntryLink($item)
                ),
                'available_margin' => $this->parseMarginValue($item['availableMarginValue'] ?? null),
            ];

            $hash = md5(json_encode($entry));
            if (isset($seen[$hash])) {
                continue;
            }

            $seen[$hash] = true;
            $distinct[] = $entry;
        }

        return $distinct;
    }

    private function isRetryableV8Status(?string $status): bool
    {
        $normalized = strtoupper(trim((string) $status));
        return in_array($normalized, self::RETRYABLE_V8_STATUSES, true);
    }

    private function appendLinkToDescription(?string $description, ?string $url): ?string
    {
        $desc = trim((string) ($description ?? ''));
        if ($url === null || $url === '') {
            return $desc === '' ? null : $desc;
        }

        if ($desc === '') {
            return 'Link: '.$url;
        }

        if (str_contains($desc, $url)) {
            return $desc;
        }

        return $desc.' | Link: '.$url;
    }

    private function extractEntryLink(array $entry): ?string
    {
        $knownKeys = ['consentLink', 'consentUrl', 'link', 'url', 'redirectUrl', 'redirectURL'];
        foreach ($knownKeys as $key) {
            $value = data_get($entry, $key);
            if (is_string($value) && preg_match('/^https?:\\/\\//i', trim($value))) {
                return trim($value);
            }
        }

        $stack = [$entry];
        while (! empty($stack)) {
            $current = array_pop($stack);
            if (! is_array($current)) {
                continue;
            }

            foreach ($current as $value) {
                if (is_string($value) && preg_match('/^https?:\\/\\//i', trim($value))) {
                    return trim($value);
                }
                if (is_array($value)) {
                    $stack[] = $value;
                }
            }
        }

        return null;
    }


    private function mergeClientRow(int $id, array $payload): void
    {
        DB::connection('sqlsrv_kinghost_vps')->statement("
            MERGE [consultas_v8].[dbo].[consulta_v8] AS target
            USING (
                SELECT
                    ? AS [id],
                    ? AS [status_consulta_v8],
                    ? AS [descricao_v8],
                    ? AS [valor_liberado],
                    ? AS [status]
            ) AS source
            ON target.[id] = source.[id]
            WHEN MATCHED THEN
                UPDATE SET
                    target.[status_consulta_v8] = COALESCE(source.[status_consulta_v8], target.[status_consulta_v8]),
                    target.[descricao_v8] = source.[descricao_v8],
                    target.[valor_liberado] = source.[valor_liberado],
                    target.[status] = COALESCE(source.[status], target.[status]),
                    target.[created_at] = SYSDATETIME();
        ", [
            $id,
            $payload['status_consulta_v8'],
            $payload['descricao_v8'],
            $payload['valor_liberado'],
            $payload['status'],
        ]);
    }

    private function duplicateClientRow(int $sourceId): int
    {
        $inserted = DB::connection('sqlsrv_kinghost_vps')->selectOne("
            INSERT INTO [consultas_v8].[dbo].[consulta_v8] (
                [cliente_cpf],
                [cliente_sexo],
                [nascimento],
                [cliente_nome],
                [email],
                [telefone],
                [status],
                [status_consulta_v8],
                [descricao_v8],
                [valor_liberado],
                [created_at],
                [tipoConsulta],
                [id_user],
                [id_equipe],
                [id_roles]
            )
            OUTPUT INSERTED.[id] AS [id]
            SELECT
                [cliente_cpf],
                [cliente_sexo],
                [nascimento],
                [cliente_nome],
                [email],
                [telefone],
                [status],
                [status_consulta_v8],
                [descricao_v8],
                [valor_liberado],
                SYSDATETIME(),
                [tipoConsulta],
                [id_user],
                [id_equipe],
                [id_roles]
            FROM [consultas_v8].[dbo].[consulta_v8]
            WHERE [id] = ?;
        ", [$sourceId]);
        if (! $inserted || ! isset($inserted->id)) {
            throw new \RuntimeException('Falha ao duplicar cliente consultado.');
        }

        return (int) $inserted->id;
    }

    private function markClientAsError(int $id, string $message): void
    {
        $status = 'Erro';
        $descricao = mb_substr(trim($message), 0, 3900);

        $this->mergeClientRow($id, [
            'status_consulta_v8' => $status,
            'descricao_v8' => $descricao,
            'valor_liberado' => null,
            'status' => $status,
        ]);
    }

    private function incrementConsultedCounter(int $accountId): void
    {
        DB::connection('sqlsrv_kinghost_vps')->update("
            UPDATE [consultas_v8].[dbo].[limites_v8]
            SET
                [consultados] = ISNULL([consultados], 0) + 1,
                [updated_at] = SYSDATETIME()
            WHERE [id] = ?
        ", [$accountId]);
    }

    private function v8Http(string $accessToken)
    {
        return $this->baseHttpRequest()
            ->baseUrl((string) env('V8_BASE_URL', self::BFF_BASE_URL_DEFAULT))
            ->withToken($accessToken)
            ->acceptJson();
    }

    private function baseHttpRequest(): PendingRequest
    {
        $request = Http::timeout(self::HTTP_TIMEOUT_SECONDS);

        $verifySsl = filter_var((string) env('V8_HTTP_VERIFY_SSL', 'true'), FILTER_VALIDATE_BOOLEAN);
        if (! $verifySsl) {
            $request = $request->withoutVerifying();
        }

        return $request;
    }

    private function toBirthDate($value): string
    {
        if (! $value) {
            return '';
        }

        try {
            return Carbon::parse($value)->format('Y-m-d');
        } catch (\Throwable $e) {
            return '';
        }
    }

    private function parseSignerPhone(string $rawPhone): array
    {
        $digits = preg_replace('/\D+/', '', $rawPhone);

        $countryCode = (string) env('SIGNER_PHONE_COUNTRY_CODE', '55');
        $areaCode = (string) env('SIGNER_PHONE_AREA_CODE', '11');
        $phoneNumber = (string) env('SIGNER_PHONE_NUMBER', '980733602');

        if (! $digits) {
            return [
                'countryCode' => $countryCode,
                'areaCode' => $areaCode,
                'phoneNumber' => $phoneNumber,
            ];
        }

        if (str_starts_with($digits, '55') && strlen($digits) >= 12) {
            $digits = substr($digits, 2);
            $countryCode = '55';
        }

        if (strlen($digits) > 11) {
            $digits = substr($digits, -11);
        }

        if (strlen($digits) === 10 || strlen($digits) === 11) {
            return [
                'countryCode' => $countryCode,
                'areaCode' => substr($digits, 0, 2),
                'phoneNumber' => substr($digits, 2),
            ];
        }

        if (strlen($digits) === 8 || strlen($digits) === 9) {
            return [
                'countryCode' => $countryCode,
                'areaCode' => $areaCode,
                'phoneNumber' => $digits,
            ];
        }

        return [
            'countryCode' => $countryCode,
            'areaCode' => $areaCode,
            'phoneNumber' => $phoneNumber,
        ];
    }



    private function generateRandomPhoneNumber(): string
    {
        $ddd = (string) random_int(11, 99);
        $subscriber = str_pad((string) random_int(11111111, 99999999), 8, '0', STR_PAD_LEFT);

        return $ddd.'9'.$subscriber;
    }


    private function normalizeClientName(string $value): string
    {
        $name = trim($value);
        if ($name === '') {
            return '';
        }

        $name = iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $name) ?: $name;
        $name = preg_replace('/[^A-Za-z0-9\s]/', '', $name) ?? $name;
        $name = preg_replace('/\s+/', ' ', $name) ?? $name;

        return mb_strtoupper(trim($name), 'UTF-8');
    }
    private function parseMarginValue($raw): ?float
    {
        if ($raw === null || $raw === '') {
            return null;
        }

        if (is_numeric($raw)) {
            return round((float) $raw, 2);
        }

        $value = preg_replace('/\s+/', '', (string) $raw);
        if ($value === '') {
            return null;
        }

        $hasDot = str_contains($value, '.');
        $hasComma = str_contains($value, ',');

        if ($hasDot && $hasComma) {
            if (strrpos($value, ',') > strrpos($value, '.')) {
                $value = str_replace('.', '', $value);
                $value = str_replace(',', '.', $value);
            } else {
                $value = str_replace(',', '', $value);
            }
        } elseif ($hasComma) {
            $value = str_replace(',', '.', $value);
        }

        if (! is_numeric($value)) {
            return null;
        }

        return round((float) $value, 2);
    }

    private function toNullableString($value): ?string
    {
        if ($value === null) {
            return null;
        }

        $trimmed = trim((string) $value);

        return $trimmed === '' ? null : $trimmed;
    }

    private function toNullableInt($value): ?int
    {
        if ($value === null) {
            return null;
        }
        if (!is_numeric($value)) {
            return null;
        }
        return (int) $value;
    }

    private function filterAccountsForUser(array $accounts, ?int $userId, ?int $equipeId = null): array
    {
        if ($userId === 1) {
            return $accounts;
        }

        $filtered = [];
        foreach ($accounts as $account) {
            if ($this->isAccountAllowedForUser($account['id'], $userId, $equipeId)) {
                $filtered[] = $account;
            }
        }

        return $filtered;
    }

    private function getAllowedAccountIndexes(array $accounts, ?int $userId, ?int $equipeId = null): array
    {
        $indexes = [];
        foreach ($accounts as $index => $account) {
            if ($this->isAccountAllowedForUser($account['id'], $userId, $equipeId)) {
                $indexes[] = $index;
            }
        }

        return $indexes;
    }

    private function isAccountAllowedForUser(int $accountId, ?int $userId, ?int $equipeId = null): bool
    {
        if ($userId === 1) {
            return true;
        }

        $special = [
            4354 => [13],
            3347 => [16],
            3349 => [12],
        ];

        if (isset($special[$userId])) {
            return in_array($accountId, $special[$userId], true);
        }

        $specialEquipes = [1, 2, 3, 1011, 1012, 1013, 1045, 1046, 2045];
        $allowedByEquipe = [14, 15, 17, 18, 19, 20, 21, 22];

        if ($equipeId !== null && in_array($equipeId, $specialEquipes, true)) {
            return in_array($accountId, $allowedByEquipe, true);
        }

        return !in_array($accountId, [12, 13, 16], true);
    }
}
