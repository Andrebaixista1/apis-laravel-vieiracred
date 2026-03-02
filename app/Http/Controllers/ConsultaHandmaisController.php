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

    private const COL_NOME_MAX = 100;
    private const COL_STATUS_MAX = 20;
    private const COL_NOME_TABELA_MAX = 10;
    private const COL_VALOR_MARGEM_MAX = 15;
    private const COL_ID_TABELA_MAX = 150;
    private const COL_TOKEN_TABELA_MAX = 150;

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

                        if (($result['status'] ?? 'success') === 'error_handled') {
                            $summary['erros']++;
                            $loginSummary['erros']++;
                        } else {
                            $summary['processados']++;
                            $summary['duplicados_criados'] += (int) ($result['duplicates_created'] ?? 0);
                            $loginSummary['processados']++;
                            $loginSummary['duplicados_criados'] += (int) ($result['duplicates_created'] ?? 0);
                        }
                    } catch (\Throwable $e) {
                        $summary['erros']++;
                        $loginSummary['erros']++;
                        $this->markPendingAsError($pendingId, $e->getMessage());
                        $this->removeFinalizedCpfDuplicates($this->normalizeCpf($pendingRow->cpf ?? ''), [$pendingId]);
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
            'nome' => $this->fitConsultaField($nome, self::COL_NOME_MAX) ?? '',
            'cpf' => $cpf,
            'telefone' => mb_substr($telefone, 0, 20),
            'dataNascimento' => $dataNascimento,
            'status' => $this->fitConsultaField('Pendente', self::COL_STATUS_MAX) ?? 'Pendente',
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

            $updatedAt = $this->parseNullableCarbon($row->updated_at ?? null);
            $canResetByWindow = $consultados > 0
                && ($updatedAt === null || $updatedAt->lte($now->copy()->subHours(24)));

            if ($canResetByWindow) {
                DB::connection(self::DB_CONNECTION)->update("
                    UPDATE [consultas_handmais].[dbo].[limites_handmais]
                    SET [consultados] = 0, [updated_at] = SYSDATETIME()
                    WHERE [id] = ?
                ", [$id]);
                $consultados = 0;
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

        $person = [
            'nome' => $nome,
            'cpf' => $cpf,
            'telefone' => $telefone,
            'dataNascimento' => $dataNascimento,
        ];

        $simulacao = $this->callHandmaisSimulacao($tokenApi, $cpf);
        $simulacao = $this->retrySimulacaoAfterApproval($tokenApi, $cpf, null, $person, $simulacao);
        $telefone = (string) ($person['telefone'] ?? $telefone);
        $payload = $simulacao['payload'];

        $entries = $this->extractSuccessEntries($payload);
        if (empty($entries)) {
            $entries = $this->resolveEntriesFromConflictMatriculas($tokenApi, $cpf, $simulacao, $person);
            $telefone = (string) ($person['telefone'] ?? $telefone);
        }

        if (empty($entries)) {
            $marginConflict = $this->extractHandmaisMarginConflict($simulacao['status'], $payload, $simulacao['raw']);
            if ($marginConflict !== null) {
                $this->markPendingAsError(
                    $pendingId,
                    (string) ($marginConflict['descricao'] ?? 'Conflito na simulacao HandMais.'),
                    (string) ($marginConflict['valor_margem'] ?? '0.00')
                );
                $this->removeFinalizedCpfDuplicates($cpf, [$pendingId]);

                return [
                    'entries_count' => 0,
                    'duplicates_created' => 0,
                    'status' => 'error_handled',
                ];
            }

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
            'descricao' => $first['descricao'] ?? null,
            'nome_tabela' => $first['nome_tabela'],
            'valor_margem' => $first['valor_margem'],
            'id_tabela' => $first['id_tabela'],
            'token_tabela' => $first['token_tabela'],
            'id_user' => $idUser,
            'equipe_id' => $equipeId,
            'id_consulta_hand' => $idConsultaHand,
        ]);

        $duplicatesCreated = 0;
        $keepIds = [$pendingId];
        for ($i = 1; $i < count($entries); $i++) {
            $newId = $this->insertConsultaResultDuplicate([
                'nome' => $nome,
                'cpf' => $cpf,
                'telefone' => $telefone,
                'dataNascimento' => $dataNascimento,
                'status' => 'Consultado',
                'descricao' => $entries[$i]['descricao'] ?? null,
                'nome_tabela' => $entries[$i]['nome_tabela'],
                'valor_margem' => $entries[$i]['valor_margem'],
                'id_tabela' => $entries[$i]['id_tabela'],
                'token_tabela' => $entries[$i]['token_tabela'],
                'id_user' => $idUser,
                'equipe_id' => $equipeId,
                'id_consulta_hand' => $idConsultaHand,
            ]);
            if ($newId > 0) {
                $keepIds[] = $newId;
            }
            $duplicatesCreated++;
        }
        $this->removeFinalizedCpfDuplicates($cpf, $keepIds);

        return [
            'entries_count' => count($entries),
            'duplicates_created' => $duplicatesCreated,
        ];
    }

    private function callHandmaisSimulacao(string $tokenApi, string $cpf, ?string $matricula = null): array
    {
        $body = [
            'cpf' => $cpf,
        ];

        $matriculaValue = trim((string) $matricula);
        if ($matriculaValue !== '') {
            $body['matricula'] = $matriculaValue;
        }

        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->acceptJson()
            ->withHeaders([
                'Authorization' => $tokenApi,
                'Content-Type' => 'application/json',
            ])
            ->asJson()
            ->post(self::HANDMAIS_SIMULACAO_URL, $body);

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

        return $this->extractApprovalUrl($payload) !== '';
    }

    private function extractApprovalUrl($payload): string
    {
        if (! is_array($payload)) {
            return '';
        }

        $candidates = [
            $payload['mensagem'] ?? null,
            $payload['descricao'] ?? null,
            $payload['message'] ?? null,
            $payload['error'] ?? null,
            $payload['url'] ?? null,
            $payload['link'] ?? null,
        ];

        foreach ($candidates as $candidate) {
            $url = $this->extractFirstUrl($this->toSafeString($candidate));
            if ($this->isApprovalUrl($url)) {
                return $url;
            }
        }

        $serialized = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if (is_string($serialized) && $serialized !== '') {
            $url = $this->extractFirstUrl($serialized);
            if ($this->isApprovalUrl($url)) {
                return $url;
            }
        }

        return '';
    }

    private function extractFirstUrl(string $text): string
    {
        $cleanText = trim((string) $text);
        if ($cleanText === '') {
            return '';
        }

        if (preg_match('/https?:\/\/[^\s<>"\']+/iu', $cleanText, $matches) !== 1) {
            return '';
        }

        $url = trim((string) ($matches[0] ?? ''));
        if ($url === '') {
            return '';
        }

        return rtrim($url, ".,;:)]}");
    }

    private function isApprovalUrl(string $url): bool
    {
        $clean = trim($url);
        if ($clean === '') {
            return false;
        }

        $lower = mb_strtolower($clean, 'UTF-8');
        if (! str_contains($lower, 'autorizacao-clt')) {
            return false;
        }

        return str_contains($lower, '/info.html');
    }

    private function buildPendingApprovalMessage(string $url = ''): string
    {
        $message = 'Nao foi possivel validar seus dados informados. Verifique as informacoes e tente novamente.';
        $message .= ' A autorizacao digital da HandMais ainda esta pendente. Abra o link e conclua a autorizacao para liberar a consulta.';
        $cleanUrl = trim($url);
        if ($cleanUrl !== '') {
            $message .= ' Link: '.$cleanUrl;
        }

        return $this->truncate($message, 3900);
    }

    private function retrySimulacaoAfterApproval(string $tokenApi, string $cpf, ?string $matricula, array &$person, array $simulacao): array
    {
        $maxAttempts = 3;
        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            $payload = $simulacao['payload'] ?? null;
            $status = (int) ($simulacao['status'] ?? 0);

            if (! $this->isApprovalRequired($status, $payload)) {
                return $simulacao;
            }

            $approvalUrl = $this->extractApprovalUrl($payload);
            if ($approvalUrl === '') {
                throw new \RuntimeException('API informou aprovacao pendente, mas sem URL valida.');
            }

            try {
                $this->approveHandmaisLink($approvalUrl, $person);
            } catch (\RuntimeException $e) {
                if ($attempt < $maxAttempts && $this->shouldRetryApprovalWithNewPhone($e->getMessage())) {
                    $person['telefone'] = $this->generateRandomPhoneNumberInRange();
                    continue;
                }

                throw $e;
            }

            $this->sleepSeconds(self::RETRY_AFTER_APPROVAL_SECONDS);
            $simulacao = $this->callHandmaisSimulacao($tokenApi, $cpf, (string) ($matricula ?? ''));
        }

        $payload = $simulacao['payload'] ?? null;
        if ($this->isApprovalRequired((int) ($simulacao['status'] ?? 0), $payload)) {
            $approvalUrl = $this->extractApprovalUrl($payload);
            throw new \RuntimeException($this->buildPendingApprovalMessage($approvalUrl));
        }

        return $simulacao;
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
                    if (is_array($payload)) {
                        $ok = (bool) ($payload['ok'] ?? $payload['success'] ?? false);
                        $hasPositiveSignal = (bool) ($payload['details']['hasPositiveSignal'] ?? true);
                        if ($ok && $hasPositiveSignal) {
                            return;
                        }
                        $headlessErrors[] = '['.$serviceUrl.'] '.$this->summarizeApprovalServicePayload($payload, (string) $response->body());
                        continue;
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
            throw new \RuntimeException($this->buildApprovalFailureMessage($headlessErrors));
        }
    }

    private function buildApprovalFailureMessage(array $headlessErrors): string
    {
        $detailsRaw = trim(implode(' | ', array_filter($headlessErrors, static fn ($v): bool => trim((string) $v) !== '')));
        $detailsLower = mb_strtolower($detailsRaw, 'UTF-8');

        $messages = [
            'Nao foi possivel validar seus dados informados. Verifique as informacoes e tente novamente.',
        ];

        if ($detailsLower !== '') {
            if (str_contains($detailsLower, 'existing_auth') || str_contains($detailsLower, 'ja existe autorizacao vinculada ao numero de telefone')) {
                $messages[] = 'Ja existe uma autorizacao vinculada ao telefone informado.';
            } elseif (str_contains($detailsLower, 'validar seus dados informados')) {
                $messages[] = 'Os dados informados nao passaram na validacao da UY3.';
            } elseif (
                str_contains($detailsLower, 'browsertype.launch')
                || str_contains($detailsLower, 'headless http_500')
                || str_contains($detailsLower, 'curl error 7')
                || str_contains($detailsLower, 'could not resolve host')
            ) {
                $messages[] = 'O servico de autorizacao esta instavel no momento.';
            }

            $messages[] = 'Detalhe tecnico: '.$this->truncate($detailsRaw, 1500);
        }

        return $this->truncate(implode(' ', array_unique($messages)), 3900);
    }

    private function shouldRetryApprovalWithNewPhone(string $message): bool
    {
        $text = mb_strtolower(trim($message), 'UTF-8');
        if ($text === '') {
            return false;
        }

        return str_contains($text, 'existing_auth')
            || str_contains($text, 'ja existe uma autorizacao vinculada ao telefone informado')
            || str_contains($text, 'ja existe autorizacao vinculada ao numero de telefone')
            || str_contains($text, 'já existe autorização vinculada ao número de telefone');
    }

    private function summarizeApprovalServicePayload(array $payload, string $fallbackRaw = ''): string
    {
        $parts = [];

        $topMessage = trim($this->toSafeString($payload['error'] ?? $payload['message'] ?? ''));
        if ($topMessage !== '') {
            $parts[] = $topMessage;
        }

        $details = $payload['details'] ?? null;
        if (is_array($details)) {
            if (array_key_exists('hasPositiveSignal', $details)) {
                $parts[] = ((bool) $details['hasPositiveSignal']) ? 'hasPositiveSignal=true' : 'hasPositiveSignal=false';
            }

            $call = null;
            if (isset($details['challengeCall']) && is_array($details['challengeCall'])) {
                $call = $details['challengeCall'];
            } elseif (isset($details['lastCall']) && is_array($details['lastCall'])) {
                $call = $details['lastCall'];
            }

            if (is_array($call)) {
                $status = (int) ($call['status'] ?? 0);
                if ($status > 0) {
                    $parts[] = 'status='.$status;
                }

                $callBody = trim($this->toSafeString($call['body'] ?? ''));
                if ($callBody !== '') {
                    $decoded = $this->decodeJson($callBody);
                    if (is_array($decoded)) {
                        $code = trim($this->toSafeString($decoded['code'] ?? ''));
                        $message = trim($this->toSafeString($decoded['message'] ?? ''));
                        if ($code !== '') {
                            $parts[] = 'code='.$code;
                        }
                        if ($message !== '') {
                            $parts[] = $message;
                        }
                    } else {
                        $parts[] = $this->truncate($callBody, 220);
                    }
                }
            }
        }

        $parts = array_values(array_unique(array_filter($parts, static fn ($v): bool => trim((string) $v) !== '')));
        if (empty($parts)) {
            return $this->truncate($fallbackRaw, 700);
        }

        return $this->truncate(implode(' | ', $parts), 700);
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
                'nome_tabela' => $this->fitConsultaField($nomeTabela, self::COL_NOME_TABELA_MAX),
                'valor_margem' => $this->fitConsultaField($valorMargem, self::COL_VALOR_MARGEM_MAX),
                'id_tabela' => $this->fitConsultaField($idTabela, self::COL_ID_TABELA_MAX),
                'token_tabela' => $this->fitConsultaField($tokenTabela, self::COL_TOKEN_TABELA_MAX),
                'descricao' => null,
            ];
        }

        return $entries;
    }


    private function extractHandmaisMarginConflict(int $httpStatus, $payload, string $raw = ''): ?array
    {
        if (! is_array($payload)) {
            return null;
        }

        $payloadHttpCode = (int) ($payload['http_code'] ?? 0);
        if ($httpStatus !== 409 && $payloadHttpCode !== 409) {
            return null;
        }

        $rows = $this->extractHandmaisMarginRows($payload['mensagem'] ?? null);
        if (empty($rows)) {
            return null;
        }

        $selected = $this->selectBestHandmaisMarginRow($rows);
        if ($selected === null) {
            return null;
        }

        $valor = (float) ($selected['valor'] ?? 0.0);

        return [
            'valor_margem' => number_format($valor, 2, '.', ''),
            'descricao' => $this->buildHandmaisConflictDescriptionForLayperson($selected, count($rows)),
        ];
    }

    private function extractHandmaisMarginRows($source): array
    {
        if (! is_array($source) || ! array_is_list($source)) {
            return [];
        }

        $rows = [];
        foreach ($source as $row) {
            if (! is_array($row)) {
                continue;
            }

            $value = $this->toNullableFloat($row['valorMargemDisponivel'] ?? $row['valor_margem'] ?? $row['valorMargem'] ?? null);
            if ($value === null) {
                continue;
            }

            $rows[] = [
                'matricula' => trim($this->toSafeString($row['matricula'] ?? '')),
                'nome' => trim($this->toSafeString($row['nome'] ?? '')),
                'valor' => $value,
            ];
        }

        return $rows;
    }

    private function selectBestHandmaisMarginRow(array $rows): ?array
    {
        if (empty($rows)) {
            return null;
        }

        $positives = array_values(array_filter($rows, static fn (array $row): bool => (float) ($row['valor'] ?? 0.0) > 0));
        if (! empty($positives)) {
            usort($positives, static fn (array $a, array $b): int => ((float) $b['valor']) <=> ((float) $a['valor']));
            return $positives[0];
        }

        $negatives = array_values(array_filter($rows, static fn (array $row): bool => (float) ($row['valor'] ?? 0.0) < 0));
        if (! empty($negatives)) {
            usort($negatives, static fn (array $a, array $b): int => ((float) $a['valor']) <=> ((float) $b['valor']));
            return $negatives[0];
        }

        return $rows[0];
    }

    private function buildHandmaisConflictDescriptionForLayperson(array $row, int $totalRows): string
    {
        $valor = (float) ($row['valor'] ?? 0.0);
        $valorFmt = $this->formatMoneyBR($valor);
        $matricula = trim((string) ($row['matricula'] ?? ''));
        $nome = trim((string) ($row['nome'] ?? ''));

        $prefix = $totalRows > 1
            ? 'A HandMais retornou mais de uma matricula para este CPF.'
            : 'A HandMais retornou conflito de matricula para este CPF.';

        if ($valor > 0) {
            $message = $prefix.' A margem disponivel identificada foi '.$valorFmt.'.';
        } elseif ($valor < 0) {
            $message = $prefix.' A margem encontrada esta negativa em '.$valorFmt.', indicando margem comprometida no momento.';
        } else {
            $message = $prefix.' Nao ha margem disponivel no momento ('.$valorFmt.').';
        }

        if ($matricula !== '') {
            $message .= ' Matricula considerada: '.$matricula.'.';
        }

        if ($nome !== '') {
            $message .= ' Nome retornado: '.$nome.'.';
        }

        $message .= ' Confirme os dados da matricula junto ao RH/empresa para seguir com a simulacao.';

        return $this->truncate($message, 3900);
    }

    private function formatMoneyBR(float $value): string
    {
        $prefix = $value < 0 ? '-R$ ' : 'R$ ';
        return $prefix.number_format(abs($value), 2, ',', '.');
    }

    private function resolveEntriesFromConflictMatriculas(string $tokenApi, string $cpf, array $simulacao, array &$person): array
    {
        $payload = $simulacao['payload'] ?? null;
        if (! is_array($payload)) {
            return [];
        }

        $rows = $this->extractHandmaisMarginRows($payload['mensagem'] ?? null);
        if (empty($rows)) {
            return [];
        }

        $matriculas = [];
        foreach ($rows as $row) {
            $matricula = trim((string) ($row['matricula'] ?? ''));
            if ($matricula === '' || isset($matriculas[$matricula])) {
                continue;
            }
            $matriculas[$matricula] = true;
        }

        if (empty($matriculas)) {
            $baseDescription = $this->extractFailureMessage($simulacao['status'] ?? 0, $payload, (string) ($simulacao['raw'] ?? ''));
            return $this->extractEntriesFromMarginRows($rows, '', $baseDescription);
        }

        $merged = [];
        $seen = [];
        $retryDescriptions = [];

        foreach (array_keys($matriculas) as $matricula) {
            $retry = $this->callHandmaisSimulacao($tokenApi, $cpf, $matricula);
            $retry = $this->retrySimulacaoAfterApproval($tokenApi, $cpf, $matricula, $person, $retry);
            $retryPayload = $retry['payload'];
            $retryDescription = '';

            $entries = $this->extractSuccessEntries($retryPayload);
            if (empty($entries)) {
                $retryDescription = $this->extractFailureMessage($retry['status'], $retryPayload, $retry['raw']);
                $retryRows = $this->extractHandmaisMarginRows(is_array($retryPayload) ? ($retryPayload['mensagem'] ?? null) : null);
                $entries = $this->extractEntriesFromMarginRows($retryRows, $matricula, $retryDescription);
            }
            if ($retryDescription !== '') {
                $retryDescriptions[] = $retryDescription;
                $entries = $this->applyFallbackDescriptionToEntries($entries, $retryDescription);
            }

            $this->appendUniqueHandmaisEntries($merged, $seen, $entries);
        }

        $mergedRetryDescription = $this->mergeHandmaisDescriptions($retryDescriptions);
        if (! empty($merged)) {
            return $this->applyFallbackDescriptionToEntries($merged, $mergedRetryDescription);
        }

        if (empty($merged)) {
            $baseDescription = $mergedRetryDescription !== ''
                ? $mergedRetryDescription
                : $this->extractFailureMessage($simulacao['status'] ?? 0, $payload, (string) ($simulacao['raw'] ?? ''));
            $merged = $this->extractEntriesFromMarginRows($rows, '', $baseDescription);
        }

        return $merged;
    }

    private function applyFallbackDescriptionToEntries(array $entries, string $fallbackDescription): array
    {
        $fallback = $this->truncate(trim($fallbackDescription), 3900);
        if ($fallback === '') {
            return $entries;
        }

        foreach ($entries as $idx => $entry) {
            if (! is_array($entry)) {
                continue;
            }

            $current = trim($this->toSafeString($entry['descricao'] ?? ''));
            if ($current !== '') {
                continue;
            }

            $entries[$idx]['descricao'] = $fallback;
        }

        return $entries;
    }

    private function mergeHandmaisDescriptions(array $messages): string
    {
        $restrictions = [];
        $genericMessages = [];

        foreach ($messages as $message) {
            $normalized = $this->normalizeHandmaisFailureMessage((string) $message);
            $normalized = trim($normalized);
            if ($normalized === '') {
                continue;
            }

            $items = $this->extractDistinctHandmaisRestrictions($normalized);
            if (! empty($items)) {
                foreach ($items as $item) {
                    $clean = trim((string) $item);
                    if ($clean === '') {
                        continue;
                    }

                    $key = mb_strtolower(preg_replace('/\s+/', ' ', $clean) ?? $clean, 'UTF-8');
                    if (isset($restrictions[$key])) {
                        continue;
                    }

                    $restrictions[$key] = $clean;
                }
                continue;
            }

            $key = mb_strtolower(preg_replace('/\s+/', ' ', $normalized) ?? $normalized, 'UTF-8');
            if (! isset($genericMessages[$key])) {
                $genericMessages[$key] = $normalized;
            }
        }

        if (! empty($restrictions)) {
            return $this->truncate(implode(', ', array_values($restrictions)).'.', 3900);
        }

        if (! empty($genericMessages)) {
            return $this->truncate(implode(' | ', array_values($genericMessages)), 3900);
        }

        return '';
    }

    private function extractEntriesFromMarginRows(array $rows, string $fallbackMatricula = '', ?string $descricao = null): array
    {
        $entries = [];
        foreach ($rows as $row) {
            if (! is_array($row)) {
                continue;
            }

            $valor = $this->toNullableFloat($row['valor'] ?? $row['valorMargemDisponivel'] ?? $row['valor_margem'] ?? null);
            if ($valor === null) {
                continue;
            }

            $matricula = trim((string) ($row['matricula'] ?? $fallbackMatricula));
            $nomeTabela = 'Matricula';

            $entries[] = [
                'nome_tabela' => $this->fitConsultaField($nomeTabela, self::COL_NOME_TABELA_MAX),
                'valor_margem' => $this->fitConsultaField(number_format($valor, 2, '.', ''), self::COL_VALOR_MARGEM_MAX),
                'id_tabela' => $this->fitConsultaField($matricula, self::COL_ID_TABELA_MAX),
                'token_tabela' => null,
                'descricao' => $this->truncate((string) ($descricao ?? ''), 3900) ?: null,
            ];
        }

        return $entries;
    }

    private function appendUniqueHandmaisEntries(array &$target, array &$seen, array $entries): void
    {
        foreach ($entries as $entry) {
            if (! is_array($entry)) {
                continue;
            }

            $nomeTabela = trim($this->toSafeString($entry['nome_tabela'] ?? ''));
            $valorMargem = trim($this->toSafeString($entry['valor_margem'] ?? ''));
            $idTabela = trim($this->toSafeString($entry['id_tabela'] ?? ''));
            $tokenTabela = trim($this->toSafeString($entry['token_tabela'] ?? ''));
            $descricao = trim($this->toSafeString($entry['descricao'] ?? ''));

            if ($nomeTabela === '' && $valorMargem === '' && $idTabela === '' && $tokenTabela === '') {
                continue;
            }

            $key = mb_strtolower($nomeTabela.'|'.$valorMargem.'|'.$idTabela.'|'.$tokenTabela.'|'.$descricao, 'UTF-8');
            if (isset($seen[$key])) {
                continue;
            }

            $seen[$key] = true;
            $target[] = [
                'nome_tabela' => $this->fitConsultaField($nomeTabela, self::COL_NOME_TABELA_MAX),
                'valor_margem' => $this->fitConsultaField($valorMargem, self::COL_VALOR_MARGEM_MAX),
                'id_tabela' => $this->fitConsultaField($idTabela, self::COL_ID_TABELA_MAX),
                'token_tabela' => $this->fitConsultaField($tokenTabela, self::COL_TOKEN_TABELA_MAX),
                'descricao' => $this->truncate($descricao, 3900) ?: null,
            ];
        }
    }

    private function extractFailureMessage(int $httpStatus, $payload, string $raw): string
    {
        $marginConflict = $this->extractHandmaisMarginConflict($httpStatus, $payload, $raw);
        if ($marginConflict !== null) {
            return (string) ($marginConflict['descricao'] ?? 'Conflito na simulacao HandMais.');
        }

        if (is_array($payload)) {
            $message = trim($this->toSafeString($payload['descricao'] ?? $payload['mensagem'] ?? $payload['message'] ?? $payload['error'] ?? ''));
            if ($message !== '') {
                $approvalUrl = $this->extractFirstUrl($message);
                if ($this->isApprovalUrl($approvalUrl)) {
                    return $this->buildPendingApprovalMessage($approvalUrl);
                }
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

        $companyRequirement = $this->extractCompanyRequirementMessage($text);
        if ($companyRequirement !== null) {
            return $companyRequirement;
        }

        // Mensagens no formato "Produto ... -> Atenção: ...": remove preâmbulo e deduplica restrições.
        $restrictions = $this->extractDistinctHandmaisRestrictions($text);
        if (! empty($restrictions)) {
            return $this->truncate(implode(', ', $restrictions).'.', 500);
        }

        return $this->truncate($text, 500);
    }

    private function extractCompanyRequirementMessage(string $text): ?string
    {
        $patterns = [
            '/A empresa\s+[0-9\.\-\/]{8,20}\s+n(?:a|\x{00E3})o atende aos requisitos m(?:i|\x{00ED})nimos(?:[^.;]*)/iu',
            '/A empresa\s+[^,;:.]{2,120}\s+n(?:a|\x{00E3})o atende aos requisitos m(?:i|\x{00ED})nimos(?:[^.;]*)/iu',
        ];

        foreach ($patterns as $pattern) {
            $matches = [];
            if (preg_match($pattern, $text, $matches) !== 1) {
                continue;
            }

            $message = trim((string) ($matches[0] ?? ''));
            if ($message === '') {
                continue;
            }

            $message = preg_replace('/\s+/', ' ', $message) ?? $message;
            $message = rtrim($message, " .,;\t\n\r\0\x0B");
            if ($message !== '') {
                return $this->truncate($message.'.', 500);
            }
        }

        return null;
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
                [id_consulta_hand] = ?,
                [updated_at] = SYSDATETIME()
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
                [id_consulta_hand],
                [created_at],
                [updated_at]
            )
            OUTPUT INSERTED.[id] AS [id]
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATETIME(), SYSDATETIME())
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
            SET
                [status] = 'Processando',
                [descricao] = NULL,
                [updated_at] = SYSDATETIME()
            WHERE [id] = ?
        ", [$id]);
    }

    private function markPendingAsError(int $id, string $message, ?string $valorMargem = null): void
    {
        $marginValue = $this->toNullableFloat($valorMargem);
        $safeMargin = $marginValue === null ? '0.00' : number_format($marginValue, 2, '.', '');

        DB::connection(self::DB_CONNECTION)->update("
            UPDATE [consultas_handmais].[dbo].[consulta_handmais]
            SET
                [status] = 'Erro',
                [descricao] = ?,
                [valor_margem] = ?,
                [updated_at] = SYSDATETIME()
            WHERE [id] = ?
        ", [
            $this->truncate($message, 3900),
            $safeMargin,
            $id,
        ]);
    }

    private function removeFinalizedCpfDuplicates(string $cpf, array $keepIds = []): void
    {
        $cleanCpf = $this->normalizeCpf($cpf);
        if ($cleanCpf === '') {
            return;
        }

        $keepIds = array_values(array_unique(array_filter(array_map('intval', $keepIds), static fn (int $id): bool => $id > 0)));

        $bindings = [$cleanCpf];
        $keepFilter = '';
        if (! empty($keepIds)) {
            $placeholders = implode(', ', array_fill(0, count($keepIds), '?'));
            $keepFilter = " AND [id] NOT IN ($placeholders)";
            foreach ($keepIds as $id) {
                $bindings[] = $id;
            }
        }

        DB::connection(self::DB_CONNECTION)->statement("
            WITH ranked AS (
                SELECT
                    [id],
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            LTRIM(RTRIM(COALESCE([cpf], ''))),
                            UPPER(LTRIM(RTRIM(COALESCE([status], '')))),
                            LTRIM(RTRIM(COALESCE([nome_tabela], ''))),
                            LTRIM(RTRIM(COALESCE([id_tabela], ''))),
                            LTRIM(RTRIM(COALESCE([token_tabela], '')))
                        ORDER BY
                            COALESCE([updated_at], [created_at], SYSDATETIME()) DESC,
                            [id] DESC
                    ) AS [rn]
                FROM [consultas_handmais].[dbo].[consulta_handmais]
                WHERE LTRIM(RTRIM(COALESCE([cpf], ''))) = ?
                  AND UPPER(LTRIM(RTRIM(COALESCE([status], '')))) IN ('CONSULTADO', 'ERRO')
            )
            DELETE FROM [consultas_handmais].[dbo].[consulta_handmais]
            WHERE [id] IN (
                SELECT [id]
                FROM ranked
                WHERE [rn] > 1{$keepFilter}
            )
        ", $bindings);
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

        return mb_substr(trim($name), 0, self::COL_NOME_MAX);
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

        if (substr($digits, 2, 1) !== '9') {
            return false;
        }

        return $digits >= '11911111111' && $digits <= '99999999999';
    }

    private function generateRandomPhoneNumberInRange(): string
    {
        for ($i = 0; $i < 50; $i++) {
            $ddd = (int) random_int(11, 99);
            $suffix = str_pad((string) random_int(0, 99999999), 8, '0', STR_PAD_LEFT);
            $phone = (string) $ddd.'9'.$suffix;
            if ($this->isValidBrazilCellPhoneInRange($phone)) {
                return $phone;
            }
        }

        return '11991111111';
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


    private function fitConsultaField($value, int $max): ?string
    {
        if ($value === null) {
            return null;
        }

        $text = trim($this->toSafeString($value));
        if ($text === '') {
            return null;
        }

        return mb_substr($text, 0, max(1, $max));
    }

    private function toNullableFloat($value): ?float
    {
        if ($value === null || $value === '') {
            return null;
        }

        if (is_int($value) || is_float($value)) {
            return (float) $value;
        }

        $text = trim((string) $value);
        if ($text === '') {
            return null;
        }

        $text = str_replace(['R$', ' '], '', $text);

        if (str_contains($text, ',') && str_contains($text, '.')) {
            $text = str_replace('.', '', $text);
            $text = str_replace(',', '.', $text);
        } elseif (str_contains($text, ',')) {
            $text = str_replace(',', '.', $text);
        }

        if (! is_numeric($text)) {
            return null;
        }

        return (float) $text;
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
