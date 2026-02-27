<?php

namespace App\Http\Controllers;

use Carbon\Carbon;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;

class ConsultaHandmaisController extends Controller
{
    private const DB_CONNECTION = 'sqlsrv_kinghost_vps';
    private const HANDMAIS_SIMULACAO_URL = 'https://app.handmais.com/uy3/simulacao_clt';
    private const HTTP_TIMEOUT_SECONDS = 60;
    private const RUN_DELAY_SECONDS = 2;
    private const RETRY_AFTER_APPROVAL_SECONDS = 2;
    private const DAILY_DEFAULT_LIMIT = 500;

    private const APPROVAL_SERVICE_URL = 'http://127.0.0.1:3211/accept-handmais';
    private const APPROVAL_SERVICE_TIMEOUT_SECONDS = 120;
    private const APPROVAL_TIMEOUT_SECONDS = 90;

    public function run(Request $request): JsonResponse
    {
        $lock = cache()->lock('consulta-handmais-manual-run', 3600);

        if (! $lock->get()) {
            return response()->json([
                'ok' => false,
                'message' => 'Ja existe uma execucao de consulta HandMais em andamento.',
            ], 409);
        }

        $startedAt = microtime(true);
        $summary = [
            'ok' => true,
            'started_at' => now()->toIso8601String(),
            'finished_at' => null,
            'duration_ms' => 0,
            'total_logins' => 0,
            'logins_com_saldo' => 0,
            'pendentes_encontrados' => 0,
            'pendentes_alocados' => 0,
            'processados' => 0,
            'erros' => 0,
            'duplicados_criados' => 0,
            'logins' => [],
        ];

        try {
            $accounts = $this->loadAccountsWithAvailableLimit();
            $summary['total_logins'] = count($accounts);
            $summary['logins_com_saldo'] = count(array_filter(
                $accounts,
                static fn (array $a): bool => (int) ($a['remaining'] ?? 0) > 0
            ));

            if (empty($accounts)) {
                $summary['message'] = 'Nenhum token com saldo disponivel.';
                $summary['finished_at'] = now()->toIso8601String();
                $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

                return response()->json($summary);
            }

            $totalCapacity = array_sum(array_map(static fn (array $a): int => (int) ($a['remaining'] ?? 0), $accounts));
            $pendingRows = $this->loadPendingRows($totalCapacity);
            $summary['pendentes_encontrados'] = count($pendingRows);

            if (empty($pendingRows)) {
                $summary['message'] = 'Nenhuma consulta pendente encontrada.';
                $summary['finished_at'] = now()->toIso8601String();
                $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

                return response()->json($summary);
            }

            $distribution = $this->distributePendingRowsAcrossAccounts($pendingRows, $accounts);
            $summary['pendentes_alocados'] = array_sum(array_map('count', $distribution));

            foreach ($accounts as $account) {
                $accountId = (int) ($account['id'] ?? 0);
                $rowsForAccount = $distribution[$accountId] ?? [];

                $loginSummary = [
                    'id' => $accountId,
                    'empresa' => $account['empresa'] ?? '',
                    'saldo_inicio' => (int) ($account['remaining'] ?? 0),
                    'alocados' => count($rowsForAccount),
                    'processados' => 0,
                    'erros' => 0,
                    'duplicados_criados' => 0,
                ];

                foreach ($rowsForAccount as $pendingRow) {
                    $pendingId = (int) ($pendingRow->id ?? 0);
                    if ($pendingId <= 0) {
                        continue;
                    }

                    try {
                        $this->markPendingAsProcessing($pendingId);

                        $result = $this->processPendingRow($pendingRow, $account);
                        $this->incrementConsultedCounter($accountId);

                        $summary['processados']++;
                        $summary['duplicados_criados'] += (int) ($result['duplicates_created'] ?? 0);
                        $loginSummary['processados']++;
                        $loginSummary['duplicados_criados'] += (int) ($result['duplicates_created'] ?? 0);
                    } catch (\Throwable $e) {
                        $summary['erros']++;
                        $loginSummary['erros']++;
                        $this->markPendingAsError($pendingId, $e->getMessage());
                    } finally {
                        $this->sleepSeconds(self::RUN_DELAY_SECONDS);
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

            $payloads = [];
            foreach ($batchRows as $index => $row) {
                if (! is_array($row)) {
                    return response()->json([
                        'ok' => false,
                        'message' => 'Linha '.($index + 1).' invalida: formato de objeto esperado.',
                    ], 422);
                }

                try {
                    $payloads[] = $this->buildStorePayloadFromInput($row);
                } catch (\InvalidArgumentException $e) {
                    return response()->json([
                        'ok' => false,
                        'message' => 'Linha '.($index + 1).': '.$e->getMessage(),
                    ], 422);
                }
            }

            $insertedIds = $this->insertConsultaRows($payloads);

            return response()->json([
                'ok' => true,
                'message' => 'Lote enfileirado para consulta HandMais.',
                'data' => [
                    'mode' => 'batch',
                    'inserted_count' => count($insertedIds),
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

        return response()->json([
            'ok' => true,
            'message' => 'Cliente enfileirado para consulta HandMais.',
            'data' => [
                'id' => $insertedId,
                'nome' => $payload['nome'],
                'cpf' => $payload['cpf'],
                'telefone' => $payload['telefone'],
                'dataNascimento' => $payload['dataNascimento'],
                'status' => $payload['status'],
                'id_user' => $payload['id_user'],
                'equipe_id' => $payload['equipe_id'],
                'id_consulta_hand' => $payload['id_consulta_hand'],
                'created_at' => now()->toIso8601String(),
            ],
        ], 201);
    }

    public function storeIndividual(Request $request): JsonResponse
    {
        return $this->store($request);
    }

    public function listLimites(Request $request): JsonResponse
    {
        $rows = DB::connection(self::DB_CONNECTION)->select("
            SELECT TOP (1000)
                [id],
                [empresa],
                [token_api],
                [total],
                [consultados],
                [limite],
                [id_user],
                [equipe_id],
                [created_at],
                [updated_at]
            FROM [consultas_handmais].[dbo].[limites_handmais]
            ORDER BY [id] DESC
        ");

        $data = [];
        foreach ($rows as $row) {
            $total = max(0, (int) ($row->total ?? 0));
            $limite = max(0, (int) ($row->limite ?? 0));
            $daily = $total > 0 ? $total : ($limite > 0 ? $limite : self::DAILY_DEFAULT_LIMIT);
            $consultados = max(0, (int) ($row->consultados ?? 0));
            $remaining = max(0, $daily - $consultados);

            $data[] = [
                'id' => (int) ($row->id ?? 0),
                'empresa' => trim((string) ($row->empresa ?? '')),
                'token_api' => trim((string) ($row->token_api ?? '')),
                'total' => $daily,
                'consultados' => $consultados,
                'restantes' => $remaining,
                'limite' => $limite,
                'id_user' => $this->toNullableInt($row->id_user ?? null),
                'equipe_id' => $this->toNullableInt($row->equipe_id ?? null),
                'created_at' => $row->created_at ?? null,
                'updated_at' => $row->updated_at ?? null,
            ];
        }

        return response()->json([
            'ok' => true,
            'total' => count($data),
            'data' => $data,
        ]);
    }

    public function listConsultas(Request $request): JsonResponse
    {
        $cpf = preg_replace('/\D+/', '', (string) $request->query('cpf', ''));
        $nome = trim((string) $request->query('nome', ''));
        $status = trim((string) $request->query('status', ''));

        $where = [];
        $bindings = [];

        if ($cpf !== '') {
            $where[] = "RIGHT(REPLICATE('0', 11) + REPLACE(REPLACE(REPLACE(COALESCE([cpf], ''), '.', ''), '-', ''), ' ', ''), 11) = ?";
            $bindings[] = str_pad(substr($cpf, -11), 11, '0', STR_PAD_LEFT);
        }

        if ($nome !== '') {
            $where[] = "UPPER(LTRIM(RTRIM(COALESCE([nome], '')))) LIKE ?";
            $bindings[] = '%'.mb_strtoupper($nome, 'UTF-8').'%';
        }

        if ($status !== '') {
            $where[] = "UPPER(LTRIM(RTRIM(COALESCE([status], '')))) = ?";
            $bindings[] = mb_strtoupper($status, 'UTF-8');
        }

        $whereSql = empty($where) ? '' : 'WHERE '.implode(' AND ', $where);

        $rows = DB::connection(self::DB_CONNECTION)->select("
            SELECT TOP (1000)
                [id],
                [nome],
                [cpf],
                [telefone],
                [dataNascimento],
                [status],
                [descricao],
                [nome_tabela],
                [valor_margem],
                [id_tabela],
                [token_tabela],
                [id_user],
                [equipe_id],
                [id_consulta_hand]
            FROM [consultas_handmais].[dbo].[consulta_handmais]
            $whereSql
            ORDER BY [id] DESC
        ", $bindings);

        $data = [];
        foreach ($rows as $row) {
            $data[] = [
                'id' => (int) ($row->id ?? 0),
                'nome' => trim((string) ($row->nome ?? '')),
                'cpf' => $this->normalizeCpf($row->cpf ?? ''),
                'telefone' => trim((string) ($row->telefone ?? '')),
                'dataNascimento' => $this->toBirthDate($row->dataNascimento ?? null),
                'status' => trim((string) ($row->status ?? '')),
                'descricao' => trim((string) ($row->descricao ?? '')),
                'nome_tabela' => trim((string) ($row->nome_tabela ?? '')),
                'valor_margem' => trim((string) ($row->valor_margem ?? '')),
                'id_tabela' => trim((string) ($row->id_tabela ?? '')),
                'token_tabela' => trim((string) ($row->token_tabela ?? '')),
                'id_user' => $this->toNullableInt($row->id_user ?? null),
                'equipe_id' => $this->toNullableInt($row->equipe_id ?? null),
                'id_consulta_hand' => $this->toNullableInt($row->id_consulta_hand ?? null),
            ];
        }

        return response()->json([
            'ok' => true,
            'total' => count($data),
            'data' => $data,
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

    private function buildStorePayloadFromInput(array $input): array
    {
        $nome = $this->normalizePersonName((string) ($input['nome'] ?? $input['cliente_nome'] ?? ''));
        $cpf = $this->normalizeCpf($input['cpf'] ?? $input['cliente_cpf'] ?? '');
        $telefone = preg_replace('/\D+/', '', (string) ($input['telefone'] ?? ''));
        $dataNascimento = $this->toBirthDate($input['dataNascimento'] ?? $input['nascimento'] ?? null);
        $idUser = $this->toNullableInt($input['id_user'] ?? null);
        $equipeId = $this->toNullableInt($input['equipe_id'] ?? $input['id_equipe'] ?? null);
        $idConsultaHand = $this->toNullableInt(
            $input['id_consulta_hand']
            ?? $input['idConsultaHand']
            ?? $input['id_consulta']
            ?? null
        );

        if ($nome === '') {
            throw new \InvalidArgumentException('nome e obrigatorio.');
        }

        if ($cpf === '') {
            throw new \InvalidArgumentException('cpf e obrigatorio.');
        }

        if ($dataNascimento === '') {
            throw new \InvalidArgumentException('dataNascimento e obrigatorio.');
        }

        if (! $this->isValidBrazilCellPhoneInRange($telefone)) {
            $telefone = $this->generateRandomPhoneNumberInRange();
        }

        return [
            'nome' => mb_substr($nome, 0, 255),
            'cpf' => $cpf,
            'telefone' => mb_substr($telefone, 0, 20),
            'dataNascimento' => $dataNascimento,
            'status' => 'Pendente',
            'descricao' => null,
            'nome_tabela' => null,
            'valor_margem' => null,
            'id_tabela' => null,
            'token_tabela' => null,
            'id_user' => $idUser,
            'equipe_id' => $equipeId,
            'id_consulta_hand' => $idConsultaHand,
        ];
    }

    private function insertConsultaRows(array $payloads): array
    {
        if (empty($payloads)) {
            return [];
        }

        $insertedIds = [];
        $connection = DB::connection(self::DB_CONNECTION);

        $connection->transaction(function () use ($connection, $payloads, &$insertedIds): void {
            foreach (array_chunk($payloads, 200) as $chunk) {
                $valuesSql = [];
                $bindings = [];

                foreach ($chunk as $payload) {
                    $valuesSql[] = '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
                    $bindings[] = $payload['nome'];
                    $bindings[] = $payload['cpf'];
                    $bindings[] = $payload['telefone'];
                    $bindings[] = $payload['dataNascimento'];
                    $bindings[] = $payload['status'];
                    $bindings[] = $payload['descricao'];
                    $bindings[] = $payload['nome_tabela'];
                    $bindings[] = $payload['valor_margem'];
                    $bindings[] = $payload['id_tabela'];
                    $bindings[] = $payload['token_tabela'];
                    $bindings[] = $payload['id_user'];
                    $bindings[] = $payload['equipe_id'];
                    $bindings[] = $payload['id_consulta_hand'];
                }

                $sql = "
                    INSERT INTO [consultas_handmais].[dbo].[consulta_handmais] (
                        [nome],
                        [cpf],
                        [telefone],
                        [dataNascimento],
                        [status],
                        [descricao],
                        [nome_tabela],
                        [valor_margem],
                        [id_tabela],
                        [token_tabela],
                        [id_user],
                        [equipe_id],
                        [id_consulta_hand]
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

    private function loadAccountsWithAvailableLimit(): array
    {
        $rows = DB::connection(self::DB_CONNECTION)->select("
            SELECT
                [id],
                [empresa],
                [token_api],
                [total],
                [consultados],
                [limite],
                [id_user],
                [equipe_id],
                [created_at],
                [updated_at]
            FROM [consultas_handmais].[dbo].[limites_handmais]
            ORDER BY [id] ASC
        ");

        $accounts = [];
        $now = Carbon::now();

        foreach ($rows as $row) {
            $id = (int) ($row->id ?? 0);
            $tokenApi = trim((string) ($row->token_api ?? ''));
            $empresa = trim((string) ($row->empresa ?? ''));
            $total = max(0, (int) ($row->total ?? 0));
            $limite = max(0, (int) ($row->limite ?? 0));
            $dailyLimit = $total > 0 ? $total : ($limite > 0 ? $limite : self::DAILY_DEFAULT_LIMIT);
            $consultados = max(0, (int) ($row->consultados ?? 0));

            if ($dailyLimit > 0 && $consultados >= $dailyLimit) {
                $updatedAt = $this->parseNullableCarbon($row->updated_at ?? null);
                $canReset = $updatedAt === null || $updatedAt->lte($now->copy()->subHours(24));
                if ($canReset) {
                    DB::connection(self::DB_CONNECTION)->update("
                        UPDATE [consultas_handmais].[dbo].[limites_handmais]
                        SET [consultados] = 0, [updated_at] = SYSDATETIME()
                        WHERE [id] = ?
                    ", [$id]);
                    $consultados = 0;
                }
            }

            $remaining = max(0, $dailyLimit - $consultados);

            if ($id <= 0 || $tokenApi === '' || $remaining <= 0) {
                continue;
            }

            $accounts[] = [
                'id' => $id,
                'empresa' => $empresa,
                'token_api' => $tokenApi,
                'daily_limit' => $dailyLimit,
                'consultados' => $consultados,
                'remaining' => $remaining,
                'id_user' => $this->toNullableInt($row->id_user ?? null),
                'equipe_id' => $this->toNullableInt($row->equipe_id ?? null),
            ];
        }

        return $accounts;
    }

    private function loadPendingRows(int $limit): array
    {
        $safeLimit = max(0, $limit);
        if ($safeLimit === 0) {
            return [];
        }

        $sql = "
            SELECT TOP ($safeLimit)
                [id],
                [nome],
                [cpf],
                [telefone],
                [dataNascimento],
                [status],
                [descricao],
                [nome_tabela],
                [valor_margem],
                [id_tabela],
                [token_tabela],
                [id_user],
                [equipe_id],
                [id_consulta_hand]
            FROM [consultas_handmais].[dbo].[consulta_handmais]
            WHERE UPPER(LTRIM(RTRIM(COALESCE([status], '')))) = 'PENDENTE'
            ORDER BY [id] ASC
        ";

        return DB::connection(self::DB_CONNECTION)->select($sql);
    }

    private function distributePendingRowsAcrossAccounts(array $rows, array &$accounts): array
    {
        $distribution = [];
        foreach ($accounts as $account) {
            $distribution[(int) $account['id']] = [];
        }

        if (empty($rows) || empty($accounts)) {
            return $distribution;
        }

        $accountsById = [];
        foreach ($accounts as $idx => $account) {
            $accountsById[(int) ($account['id'] ?? 0)] = $idx;
        }

        $accountCount = count($accounts);
        $pointer = 0;

        foreach ($rows as $row) {
            $forcedId = $this->toNullableInt($row->id_consulta_hand ?? null);
            if ($forcedId !== null && isset($accountsById[$forcedId])) {
                $forcedIdx = $accountsById[$forcedId];
                if ((int) ($accounts[$forcedIdx]['remaining'] ?? 0) > 0) {
                    $distribution[$forcedId][] = $row;
                    $accounts[$forcedIdx]['remaining'] = max(0, (int) $accounts[$forcedIdx]['remaining'] - 1);
                    continue;
                }
            }

            $selectedIndex = null;
            for ($attempt = 0; $attempt < $accountCount; $attempt++) {
                $idx = ($pointer + $attempt) % $accountCount;
                if ((int) ($accounts[$idx]['remaining'] ?? 0) <= 0) {
                    continue;
                }
                $selectedIndex = $idx;
                break;
            }

            if ($selectedIndex === null) {
                break;
            }

            $accountId = (int) ($accounts[$selectedIndex]['id'] ?? 0);
            if ($accountId <= 0) {
                continue;
            }

            $distribution[$accountId][] = $row;
            $accounts[$selectedIndex]['remaining'] = max(0, (int) $accounts[$selectedIndex]['remaining'] - 1);
            $pointer = ($selectedIndex + 1) % $accountCount;
        }

        return $distribution;
    }

    private function processPendingRow(object $pendingRow, array $account): array
    {
        $pendingId = (int) ($pendingRow->id ?? 0);
        if ($pendingId <= 0) {
            throw new \RuntimeException('Registro pendente sem id valido.');
        }

        $cpf = $this->normalizeCpf($pendingRow->cpf ?? '');
        if ($cpf === '') {
            throw new \RuntimeException('CPF invalido para processar a consulta.');
        }

        $nome = $this->normalizePersonName((string) ($pendingRow->nome ?? ''));
        if ($nome === '') {
            throw new \RuntimeException('Nome obrigatorio para fluxo de autorizacao.');
        }

        $dataNascimento = $this->toBirthDate($pendingRow->dataNascimento ?? null);
        if ($dataNascimento === '') {
            throw new \RuntimeException('Data de nascimento obrigatoria para fluxo de autorizacao.');
        }

        $telefone = preg_replace('/\D+/', '', (string) ($pendingRow->telefone ?? ''));
        if (! $this->isValidBrazilCellPhoneInRange($telefone)) {
            $telefone = $this->generateRandomPhoneNumberInRange();
        }

        $tokenApi = trim((string) ($account['token_api'] ?? ''));
        if ($tokenApi === '') {
            throw new \RuntimeException('Token API do limite HandMais nao informado.');
        }

        $simulacao = $this->callHandmaisSimulacao($tokenApi, $cpf);
        $payload = $simulacao['payload'];

        if ($this->isApprovalRequired($simulacao['status'], $payload)) {
            $approvalUrl = $this->extractApprovalUrl($payload);
            if ($approvalUrl === '') {
                throw new \RuntimeException('API informou aprovacao pendente, mas sem URL valida.');
            }

            $this->approveHandmaisLink($approvalUrl, [
                'nome' => $nome,
                'cpf' => $cpf,
                'telefone' => $telefone,
                'dataNascimento' => $dataNascimento,
            ]);

            $this->sleepSeconds(self::RETRY_AFTER_APPROVAL_SECONDS);
            $simulacao = $this->callHandmaisSimulacao($tokenApi, $cpf);
            $payload = $simulacao['payload'];
        }

        $entries = $this->extractSuccessEntries($payload);
        if (empty($entries)) {
            throw new \RuntimeException($this->extractFailureMessage($simulacao['status'], $payload, $simulacao['raw']));
        }

        $idUser = $this->toNullableInt($pendingRow->id_user ?? null);
        $equipeId = $this->toNullableInt($pendingRow->equipe_id ?? null);
        $idConsultaHand = $this->toNullableInt($pendingRow->id_consulta_hand ?? null);

        $first = $entries[0];
        $this->updateConsultaById($pendingId, [
            'nome' => $nome,
            'cpf' => $cpf,
            'telefone' => $telefone,
            'dataNascimento' => $dataNascimento,
            'status' => 'Consultado',
            'descricao' => null,
            'nome_tabela' => $first['nome_tabela'],
            'valor_margem' => $first['valor_margem'],
            'id_tabela' => $first['id_tabela'],
            'token_tabela' => $first['token_tabela'],
            'id_user' => $idUser,
            'equipe_id' => $equipeId,
            'id_consulta_hand' => $idConsultaHand,
        ]);

        $duplicatesCreated = 0;
        for ($i = 1; $i < count($entries); $i++) {
            $this->insertConsultaResultDuplicate([
                'nome' => $nome,
                'cpf' => $cpf,
                'telefone' => $telefone,
                'dataNascimento' => $dataNascimento,
                'status' => 'Consultado',
                'descricao' => null,
                'nome_tabela' => $entries[$i]['nome_tabela'],
                'valor_margem' => $entries[$i]['valor_margem'],
                'id_tabela' => $entries[$i]['id_tabela'],
                'token_tabela' => $entries[$i]['token_tabela'],
                'id_user' => $idUser,
                'equipe_id' => $equipeId,
                'id_consulta_hand' => $idConsultaHand,
            ]);
            $duplicatesCreated++;
        }

        return [
            'entries_count' => count($entries),
            'duplicates_created' => $duplicatesCreated,
        ];
    }

    private function callHandmaisSimulacao(string $tokenApi, string $cpf): array
    {
        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->acceptJson()
            ->withHeaders([
                'Authorization' => $tokenApi,
                'Content-Type' => 'application/json',
            ])
            ->asJson()
            ->post(self::HANDMAIS_SIMULACAO_URL, [
                'cpf' => $cpf,
            ]);

        $raw = (string) $response->body();
        $payload = $this->decodeJson($raw);

        return [
            'status' => (int) $response->status(),
            'payload' => $payload,
            'raw' => $raw,
        ];
    }

    private function isApprovalRequired(int $httpStatus, $payload): bool
    {
        if ($httpStatus === 202) {
            return true;
        }

        if (! is_array($payload)) {
            return false;
        }

        $code = (int) ($payload['http_code'] ?? 0);
        if ($code === 202) {
            return true;
        }

        $message = trim($this->toSafeString($payload['mensagem'] ?? ''));
        return $message !== '' && preg_match('/^https?:\\/\\//i', $message) === 1;
    }

    private function extractApprovalUrl($payload): string
    {
        if (! is_array($payload)) {
            return '';
        }

        $candidates = [
            $payload['mensagem'] ?? null,
            $payload['url'] ?? null,
            $payload['link'] ?? null,
        ];

        foreach ($candidates as $candidate) {
            $url = trim($this->toSafeString($candidate));
            if ($url !== '' && preg_match('/^https?:\\/\\//i', $url) === 1) {
                return $url;
            }
        }

        return '';
    }

    private function approveHandmaisLink(string $url, array $person): void
    {
        $headlessErrors = [];

        foreach ($this->approvalServiceUrls() as $serviceUrl) {
            try {
                $response = Http::timeout(self::APPROVAL_SERVICE_TIMEOUT_SECONDS)
                    ->acceptJson()
                    ->asJson()
                    ->post($serviceUrl, [
                        'url' => $url,
                        'shortUrl' => $url,
                        'nome' => $person['nome'] ?? '',
                        'cpf' => $person['cpf'] ?? '',
                        'telefone' => $person['telefone'] ?? '',
                        'dataNascimento' => $person['dataNascimento'] ?? '',
                        'acceptTerms' => true,
                        'allowGeolocation' => true,
                        'submit' => true,
                        'timeoutSeconds' => self::APPROVAL_TIMEOUT_SECONDS,
                    ]);

                if ($response->successful()) {
                    $payload = $this->decodeJson((string) $response->body());
                    if (is_array($payload) && (bool) ($payload['ok'] ?? $payload['success'] ?? false)) {
                        return;
                    }
                    $headlessErrors[] = '['.$serviceUrl.'] '.$this->truncate((string) $response->body(), 700);
                    continue;
                }

                $headlessErrors[] = '['.$serviceUrl.'] Headless HTTP_'.$response->status().' '.$this->truncate((string) $response->body(), 700);
            } catch (\Throwable $e) {
                $headlessErrors[] = '['.$serviceUrl.'] Headless exception: '.$this->truncate($e->getMessage(), 350);
            }
        }

        if (! $this->approveHandmaisLinkViaForm($url, $person)) {
            $suffix = ! empty($headlessErrors) ? ' Detalhe: '.$this->truncate(implode(' | ', $headlessErrors), 1600) : '';
            throw new \RuntimeException('Falha na aprovacao automatica HandMais.'.$suffix);
        }
    }

    private function approvalServiceUrls(): array
    {
        $envUrl = trim((string) env('HANDMAIS_APPROVAL_URL', ''));
        $urls = [];
        if ($envUrl !== '') {
            $urls[] = $envUrl;
        }

        $urls[] = self::APPROVAL_SERVICE_URL;
        $urls[] = str_replace('127.0.0.1', '172.17.0.1', self::APPROVAL_SERVICE_URL);
        $urls[] = str_replace('127.0.0.1', 'host.docker.internal', self::APPROVAL_SERVICE_URL);
        $urls[] = str_replace('localhost', '172.17.0.1', self::APPROVAL_SERVICE_URL);

        $unique = [];
        foreach ($urls as $url) {
            $clean = trim((string) $url);
            if ($clean === '' || isset($unique[$clean])) {
                continue;
            }
            $unique[$clean] = true;
        }

        return array_keys($unique);
    }

    private function approveHandmaisLinkViaForm(string $url, array $person): bool
    {
        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->withHeaders(['Accept' => 'text/html,application/xhtml+xml'])
            ->get($url);

        if (! $response->successful()) {
            return false;
        }

        $html = (string) $response->body();
        $dom = new \DOMDocument();
        $loaded = @$dom->loadHTML($html, LIBXML_NOWARNING | LIBXML_NOERROR);
        if (! $loaded) {
            return false;
        }

        $forms = $dom->getElementsByTagName('form');
        if ($forms->length === 0) {
            return false;
        }

        $form = $forms->item(0);
        if (! $form) {
            return false;
        }

        $actionRaw = trim((string) $form->getAttribute('action'));
        $methodRaw = strtoupper(trim((string) $form->getAttribute('method')));
        $action = $this->resolveUrl($url, $actionRaw !== '' ? $actionRaw : $url);
        $method = $methodRaw !== '' ? $methodRaw : 'POST';

        $fields = [];
        foreach ($form->getElementsByTagName('input') as $input) {
            $name = trim((string) $input->getAttribute('name'));
            if ($name === '') {
                continue;
            }

            $type = mb_strtolower(trim((string) $input->getAttribute('type')));
            $value = (string) $input->getAttribute('value');
            $key = mb_strtolower($name);

            if ($type === 'checkbox') {
                $fields[$name] = $value !== '' ? $value : 'on';
                continue;
            }

            if (str_contains($key, 'nome')) {
                $fields[$name] = (string) ($person['nome'] ?? '');
                continue;
            }

            if (str_contains($key, 'cpf')) {
                $fields[$name] = (string) ($person['cpf'] ?? '');
                continue;
            }

            if (str_contains($key, 'fone') || str_contains($key, 'cel') || str_contains($key, 'tel')) {
                $fields[$name] = (string) ($person['telefone'] ?? '');
                continue;
            }

            if (str_contains($key, 'nasc') || str_contains($key, 'birth') || str_contains($key, 'data')) {
                $fields[$name] = (string) ($person['dataNascimento'] ?? '');
                continue;
            }

            $fields[$name] = $value;
        }

        foreach ($form->getElementsByTagName('textarea') as $textarea) {
            $name = trim((string) $textarea->getAttribute('name'));
            if ($name !== '' && ! isset($fields[$name])) {
                $fields[$name] = trim((string) $textarea->textContent);
            }
        }

        $submitResponse = $method === 'GET'
            ? Http::timeout(self::HTTP_TIMEOUT_SECONDS)->get($action, $fields)
            : Http::timeout(self::HTTP_TIMEOUT_SECONDS)->asForm()->post($action, $fields);

        if (! $submitResponse->successful()) {
            return false;
        }

        $submitHtml = mb_strtolower(trim((string) $submitResponse->body()));
        return str_contains($submitHtml, 'sucesso')
            || str_contains($submitHtml, 'cadastro enviado')
            || str_contains($submitHtml, 'obrigado')
            || str_contains($submitHtml, 'autorizado');
    }

    private function resolveUrl(string $baseUrl, string $targetUrl): string
    {
        if ($targetUrl === '') {
            return $baseUrl;
        }

        if (preg_match('/^https?:\/\//i', $targetUrl) === 1) {
            return $targetUrl;
        }

        $base = parse_url($baseUrl);
        if (! is_array($base)) {
            return $targetUrl;
        }

        $scheme = $base['scheme'] ?? 'https';
        $host = $base['host'] ?? '';
        $port = isset($base['port']) ? ':'.$base['port'] : '';
        if ($host === '') {
            return $targetUrl;
        }

        if (str_starts_with($targetUrl, '/')) {
            return $scheme.'://'.$host.$port.$targetUrl;
        }

        $path = $base['path'] ?? '/';
        $dir = rtrim(substr($path, 0, (int) strrpos($path, '/')), '/');
        return $scheme.'://'.$host.$port.$dir.'/'.$targetUrl;
    }

    private function extractSuccessEntries($payload): array
    {
        $rows = [];

        if (is_array($payload)) {
            if (array_is_list($payload)) {
                $rows = $payload;
            } elseif (isset($payload['data']) && is_array($payload['data'])) {
                $rows = $payload['data'];
            } elseif (isset($payload['rows']) && is_array($payload['rows'])) {
                $rows = $payload['rows'];
            } elseif (isset($payload['result']) && is_array($payload['result'])) {
                $rows = $payload['result'];
            } elseif (isset($payload['simulacoes']) && is_array($payload['simulacoes'])) {
                $rows = $payload['simulacoes'];
            } elseif (isset($payload['nome_tabela']) || isset($payload['token_tabela']) || isset($payload['id'])) {
                $rows = [$payload];
            }
        }

        $entries = [];
        foreach ($rows as $row) {
            if (! is_array($row)) {
                continue;
            }

            $nomeTabela = trim($this->toSafeString($row['nome_tabela'] ?? $row['nomeTabela'] ?? ''));
            $valorMargem = trim($this->toSafeString($row['valor_margem'] ?? $row['valorMargem'] ?? ''));
            $idTabela = trim($this->toSafeString($row['id_tabela'] ?? $row['id'] ?? ''));
            $tokenTabela = trim($this->toSafeString($row['token_tabela'] ?? $row['tokenTabela'] ?? ''));

            if ($nomeTabela === '' && $valorMargem === '' && $idTabela === '' && $tokenTabela === '') {
                continue;
            }

            $entries[] = [
                'nome_tabela' => mb_substr($nomeTabela, 0, 255),
                'valor_margem' => mb_substr($valorMargem, 0, 255),
                'id_tabela' => mb_substr($idTabela, 0, 255),
                'token_tabela' => mb_substr($tokenTabela, 0, 255),
            ];
        }

        return $entries;
    }

    private function extractFailureMessage(int $httpStatus, $payload, string $raw): string
    {
        if (is_array($payload)) {
            $message = trim($this->toSafeString($payload['descricao'] ?? $payload['mensagem'] ?? $payload['message'] ?? $payload['error'] ?? ''));
            if ($message !== '') {
                return $this->normalizeHandmaisFailureMessage($message);
            }
        }

        if ($raw !== '') {
            return $this->normalizeHandmaisFailureMessage('Falha HandMais (HTTP_'.$httpStatus.'): '.$this->truncate($raw, 500));
        }

        return 'Falha HandMais sem retorno valido (HTTP_'.$httpStatus.').';
    }

    private function normalizeHandmaisFailureMessage(string $message): string
    {
        $text = trim(preg_replace('/\s+/', ' ', (string) $message) ?? (string) $message);
        if ($text === '') {
            return '';
        }

        $preferred = 'A empresa possui regime fiscal (ISENTA DO IRPJ) não atendido por este produto de crédito.';

        $lower = mb_strtolower($text, 'UTF-8');
        $hasRegimeFiscal = str_contains($lower, 'empresa possui regime fiscal');
        $hasIsentaIrpj = str_contains($lower, 'isenta do irpj');
        $hasProdutoCredito = str_contains($lower, 'produto de credito') || str_contains($lower, 'produto de crédito');

        if ($hasRegimeFiscal && $hasIsentaIrpj && $hasProdutoCredito) {
            return $preferred;
        }

        // Mensagens no formato "Produto ... -> Atenção: ...": remove preâmbulo e deduplica restrições.
        $restrictions = $this->extractDistinctHandmaisRestrictions($text);
        if (! empty($restrictions)) {
            return $this->truncate(implode(', ', $restrictions).'.', 500);
        }

        return $this->truncate($text, 500);
    }

    private function extractDistinctHandmaisRestrictions(string $text): array
    {
        $rawParts = preg_split('/Produto\s+[a-z0-9\-]+\s*->/iu', $text) ?: [$text];
        $out = [];
        $seen = [];

        foreach ($rawParts as $rawPart) {
            $part = trim((string) $rawPart);
            if ($part === '') {
                continue;
            }

            $part = preg_replace('/^Aten[^:]*:\s*/iu', '', $part) ?? $part;
            $part = preg_replace('/^Segue abaixo[^:]*:\s*/iu', '', $part) ?? $part;
            $part = preg_replace('/^as restri[^:]*:\s*/iu', '', $part) ?? $part;
            $part = trim($part);
            if ($part === '') {
                continue;
            }

            // Não quebrar valores decimais (ex.: "0,00"), apenas separadores reais de frases.
            $items = preg_split('/\s*(?:;\s*|\|\s*|,(?!\s*\d{2}\b)\s*)/u', $part) ?: [];
            foreach ($items as $itemRaw) {
                $item = trim((string) $itemRaw);
                if ($item === '') {
                    continue;
                }

                $item = preg_replace('/Produto\s+[a-z0-9\-]+\s*->.*/iu', '', $item) ?? $item;
                $item = preg_replace('/^Aten[^:]*:\s*/iu', '', $item) ?? $item;
                $item = preg_replace('/^Segue abaixo[^:]*:\s*/iu', '', $item) ?? $item;
                $item = trim($item, " .,\t\n\r\0\x0B");
                if ($item === '') {
                    continue;
                }

                $key = preg_replace('/\s+/', ' ', mb_strtolower($item, 'UTF-8')) ?? mb_strtolower($item, 'UTF-8');
                if (isset($seen[$key])) {
                    continue;
                }

                $seen[$key] = true;
                $out[] = $item;
            }
        }

        return $out;
    }

    private function updateConsultaById(int $id, array $payload): void
    {
        DB::connection(self::DB_CONNECTION)->update("
            UPDATE [consultas_handmais].[dbo].[consulta_handmais]
            SET
                [nome] = ?,
                [cpf] = ?,
                [telefone] = ?,
                [dataNascimento] = ?,
                [status] = ?,
                [descricao] = ?,
                [nome_tabela] = ?,
                [valor_margem] = ?,
                [id_tabela] = ?,
                [token_tabela] = ?,
                [id_user] = ?,
                [equipe_id] = ?,
                [id_consulta_hand] = ?
            WHERE [id] = ?
        ", [
            $payload['nome'] ?? '',
            $payload['cpf'] ?? '',
            $payload['telefone'] ?? '',
            $payload['dataNascimento'] ?? null,
            $payload['status'] ?? 'Erro',
            $payload['descricao'] ?? null,
            $payload['nome_tabela'] ?? null,
            $payload['valor_margem'] ?? null,
            $payload['id_tabela'] ?? null,
            $payload['token_tabela'] ?? null,
            $payload['id_user'] ?? null,
            $payload['equipe_id'] ?? null,
            $payload['id_consulta_hand'] ?? null,
            $id,
        ]);
    }

    private function insertConsultaResultDuplicate(array $payload): int
    {
        $row = DB::connection(self::DB_CONNECTION)->selectOne("
            INSERT INTO [consultas_handmais].[dbo].[consulta_handmais] (
                [nome],
                [cpf],
                [telefone],
                [dataNascimento],
                [status],
                [descricao],
                [nome_tabela],
                [valor_margem],
                [id_tabela],
                [token_tabela],
                [id_user],
                [equipe_id],
                [id_consulta_hand]
            )
            OUTPUT INSERTED.[id] AS [id]
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ", [
            $payload['nome'] ?? '',
            $payload['cpf'] ?? '',
            $payload['telefone'] ?? '',
            $payload['dataNascimento'] ?? null,
            $payload['status'] ?? 'Consultado',
            $payload['descricao'] ?? null,
            $payload['nome_tabela'] ?? null,
            $payload['valor_margem'] ?? null,
            $payload['id_tabela'] ?? null,
            $payload['token_tabela'] ?? null,
            $payload['id_user'] ?? null,
            $payload['equipe_id'] ?? null,
            $payload['id_consulta_hand'] ?? null,
        ]);

        return (int) ($row->id ?? 0);
    }

    private function markPendingAsProcessing(int $id): void
    {
        DB::connection(self::DB_CONNECTION)->update("
            UPDATE [consultas_handmais].[dbo].[consulta_handmais]
            SET [status] = 'Processando', [descricao] = NULL
            WHERE [id] = ?
        ", [$id]);
    }

    private function markPendingAsError(int $id, string $message): void
    {
        DB::connection(self::DB_CONNECTION)->update("
            UPDATE [consultas_handmais].[dbo].[consulta_handmais]
            SET
                [status] = 'Erro',
                [descricao] = ?,
                [valor_margem] = '0.00'
            WHERE [id] = ?
        ", [
            $this->truncate($message, 3900),
            $id,
        ]);
    }

    private function incrementConsultedCounter(int $accountId): void
    {
        DB::connection(self::DB_CONNECTION)->update("
            UPDATE [consultas_handmais].[dbo].[limites_handmais]
            SET
                [consultados] = ISNULL([consultados], 0) + 1,
                [updated_at] = SYSDATETIME()
            WHERE [id] = ?
        ", [$accountId]);
    }

    private function normalizeCpf($value): string
    {
        $digits = preg_replace('/\D+/', '', (string) $value);
        if ($digits === '') {
            return '';
        }

        if (strlen($digits) > 11) {
            $digits = substr($digits, -11);
        }

        return str_pad($digits, 11, '0', STR_PAD_LEFT);
    }

    private function normalizePersonName(string $value): string
    {
        $name = trim($value);
        if ($name === '') {
            return '';
        }
        $name = preg_replace('/\s+/', ' ', $name) ?? $name;

        return mb_substr(trim($name), 0, 255);
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

    private function isValidBrazilCellPhoneInRange(string $digits): bool
    {
        if (! preg_match('/^\d{11}$/', $digits)) {
            return false;
        }

        return $digits >= '11911111111' && $digits <= '99999999999';
    }

    private function generateRandomPhoneNumberInRange(): string
    {
        return (string) random_int(11911111111, 99999999999);
    }

    private function parseNullableCarbon($value): ?Carbon
    {
        if ($value === null || $value === '') {
            return null;
        }

        try {
            return Carbon::parse($value);
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function decodeJson(string $raw)
    {
        $trimmed = trim($raw);
        if ($trimmed === '') {
            return null;
        }

        $decoded = json_decode($trimmed, true);
        return json_last_error() === JSON_ERROR_NONE ? $decoded : null;
    }

    private function toNullableInt($value): ?int
    {
        if ($value === null || $value === '') {
            return null;
        }
        if (! is_numeric($value)) {
            return null;
        }

        return (int) $value;
    }

    private function truncate(string $value, int $max = 300): string
    {
        $text = trim($value);
        if (mb_strlen($text) <= $max) {
            return $text;
        }

        return mb_substr($text, 0, max(0, $max - 3)).'...';
    }

    private function toSafeString($value): string
    {
        if ($value === null) {
            return '';
        }

        if (is_scalar($value)) {
            return (string) $value;
        }

        if (is_array($value) || is_object($value)) {
            return (string) json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }

        return '';
    }

    private function sleepSeconds(int $seconds): void
    {
        if ($seconds <= 0) {
            return;
        }
        sleep($seconds);
    }
}
