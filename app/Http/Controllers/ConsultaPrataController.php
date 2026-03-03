<?php

namespace App\Http\Controllers;

use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use RuntimeException;

class ConsultaPrataController extends Controller
{
    private const LIMIT_PER_HOUR = 80;
    private const BALANCE_POLL_SECONDS = 5;
    private const BALANCE_POLL_ATTEMPTS = 12;
    private const LOGIN_URL = 'https://api.bancoprata.com.br/v1/users/login';
    private const AUTHORIZATION_TERM_URL = 'https://api.bancoprata.com.br/v1/private-payroll/authorization_term';
    private const BALANCE_URL = 'https://api.bancoprata.com.br/v1/private-payroll/balance';

    public function run(Request $request): JsonResponse
    {
        $idUserScope = $this->toIntOrNull($request->query('id_user', $request->input('id_user')));
        $idEquipeScope = $this->toIntOrNull($request->query('id_equipe', $request->input('id_equipe')));
        $limit = max(1, min((int) $request->query('limit', 20), 200));

        if ($idUserScope !== null && $idUserScope <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'id_user invalido.',
            ], 422);
        }

        if ($idEquipeScope !== null && $idEquipeScope <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'id_equipe invalido.',
            ], 422);
        }

        $scopeKey = 'all';
        if ($idUserScope !== null) {
            $scopeKey = 'user_'.$idUserScope;
            if ($idEquipeScope !== null) {
                $scopeKey .= '_eq_'.$idEquipeScope;
            }
        } elseif ($idEquipeScope !== null) {
            $scopeKey = 'eq_'.$idEquipeScope;
        }

        $lockKey = 'consulta-prata-manual-run:'.$scopeKey;
        $lock = cache()->lock($lockKey, 3600);

        if (! $lock->get()) {
            return response()->json([
                'ok' => false,
                'message' => 'Ja existe uma execucao em andamento para este escopo.',
                'scope' => [
                    'id_user' => $idUserScope,
                    'id_equipe' => $idEquipeScope,
                    'scope_key' => $scopeKey,
                ],
            ], 409);
        }

        $startedAt = microtime(true);
        $summary = [
            'ok' => true,
            'started_at' => now()->toIso8601String(),
            'finished_at' => null,
            'duration_ms' => 0,
            'scope' => [
                'id_user' => $idUserScope,
                'id_equipe' => $idEquipeScope,
                'scope_key' => $scopeKey,
            ],
            'lock_key' => $lockKey,
            'pendentes_encontrados' => 0,
            'processados' => 0,
            'erros' => 0,
            'detalhes' => [],
        ];

        try {
            $pendingRows = $this->loadPendingRows($limit, $idUserScope, $idEquipeScope);
            $summary['pendentes_encontrados'] = count($pendingRows);

            if (empty($pendingRows)) {
                $summary['message'] = 'Nenhuma consulta pendente encontrada.';
                $summary['finished_at'] = now()->toIso8601String();
                $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

                return response()->json($summary);
            }

            foreach ($pendingRows as $row) {
                $idConsultaPrata = $this->toIntOrNull($row->id_consulta_prata ?? null);
                if ($idConsultaPrata === null || $idConsultaPrata <= 0) {
                    $summary['erros']++;
                    $summary['detalhes'][] = [
                        'id_consulta_prata' => null,
                        'ok' => false,
                        'status' => 422,
                        'message' => 'Registro pendente sem id_consulta_prata valido.',
                    ];
                    continue;
                }

                $name = $this->pickFirstText($row, [
                    'nome',
                    'name',
                    'cliente_nome',
                ]);
                $document = $this->digitsOnly($this->pickFirstText($row, [
                    'document',
                    'cpf',
                    'document_number',
                    'numero_documento',
                    'cliente_cpf',
                ]));

                if ($name === '' || strlen($document) !== 11) {
                    $summary['erros']++;
                    $summary['detalhes'][] = [
                        'id_consulta_prata' => $idConsultaPrata,
                        'ok' => false,
                        'status' => 422,
                        'message' => 'Pendente sem nome ou documento valido para consulta.',
                    ];
                    continue;
                }

                $payload = [
                    'id_consulta_prata' => $idConsultaPrata,
                    'name' => $name,
                    'document' => $document,
                    'email' => $this->pickFirstText($row, ['email', 'cliente_email']) ?: 'cliente@gmail.com',
                    'number' => $this->digitsOnly($this->pickFirstText($row, ['number', 'telefone', 'phone'])) ?: '980733602',
                    'area_code' => $this->digitsOnly($this->pickFirstText($row, ['area_code', 'ddd'])) ?: '11',
                    'ip_address' => $this->pickFirstText($row, ['ip_address']) ?: '192.168.0.1',
                    'lat' => $this->pickFirstText($row, ['lat']) ?: '-23.5505',
                    'long' => $this->pickFirstText($row, ['long']) ?: '-46.6333',
                    'model' => $this->pickFirstText($row, ['model']) ?: 'Mozilla/5.0',
                ];

                $childRequest = Request::create('/api/prata/consultar', 'POST', $payload);
                $response = $this->consultar($childRequest);
                $statusCode = $response->getStatusCode();
                $responseBody = json_decode($response->getContent(), true);

                $detail = [
                    'id_consulta_prata' => $idConsultaPrata,
                    'ok' => $statusCode >= 200 && $statusCode < 300,
                    'status' => $statusCode,
                    'message' => is_array($responseBody) ? ($responseBody['message'] ?? null) : null,
                    'status_consulta' => is_array($responseBody) ? ($responseBody['status_consulta'] ?? null) : null,
                ];
                $summary['detalhes'][] = $detail;

                if ($detail['ok']) {
                    $summary['processados']++;
                } else {
                    $summary['erros']++;
                }

                usleep(200000);
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

    public function consultar(Request $request): JsonResponse
    {
        $data = $request->validate([
            'id_consulta_prata' => ['required', 'integer', 'min:1'],
            'name' => ['required', 'string', 'max:255'],
            'document' => ['nullable', 'string'],
            'cpf' => ['nullable', 'string'],
            'email' => ['nullable', 'email', 'max:255'],
            'number' => ['nullable', 'string', 'max:20'],
            'area_code' => ['nullable', 'string', 'max:5'],
            'ip_address' => ['nullable', 'string', 'max:45'],
            'lat' => ['nullable', 'string', 'max:30'],
            'long' => ['nullable', 'string', 'max:30'],
            'model' => ['nullable', 'string', 'max:255'],
        ]);

        $document = preg_replace('/\D/', '', $data['document'] ?? $data['cpf'] ?? '');
        if (strlen($document) !== 11) {
            return response()->json([
                'message' => 'CPF/documento invalido. Informe 11 digitos.',
            ], 422);
        }

        $prataClient = trim((string) env('PRATA_CLIENT_KEY', 'giQ2KhtcAmrpogbI7GJNz3beMvV1H96C'));
        if ($prataClient === '') {
            return response()->json([
                'message' => 'PRATA_CLIENT_KEY nao configurado no ambiente.',
            ], 500);
        }

        $idConsultaPrata = (int) $data['id_consulta_prata'];
        $pendingConsulta = $this->getPendingConsultaById($idConsultaPrata);
        if (!$pendingConsulta) {
            return response()->json([
                'message' => 'Consulta Prata nao encontrada para o id_consulta_prata informado.',
                'id_consulta_prata' => $idConsultaPrata,
            ], 404);
        }
        if (strtolower(trim((string) ($pendingConsulta['status_consulta'] ?? ''))) !== 'pendente') {
            return response()->json([
                'message' => 'Somente consultas com status_consulta = Pendente podem ser processadas.',
                'id_consulta_prata' => $idConsultaPrata,
                'status_atual' => $pendingConsulta['status_consulta'] ?? null,
            ], 409);
        }

        try {
            $login = $this->pickLoginById($idConsultaPrata);
        } catch (\Throwable $e) {
            return response()->json([
                'message' => 'Nenhum login disponivel para consulta no momento.',
                'error' => $e->getMessage(),
            ], 409);
        }

        $loginResponse = Http::acceptJson()
            ->timeout(30)
            ->withHeaders([
                'Content-Type' => 'application/json',
                'x-prata-client' => $prataClient,
            ])
            ->post(self::LOGIN_URL, [
                'email' => $login['login'],
                'password' => $login['senha'],
            ]);

        if (!$loginResponse->successful()) {
            return response()->json([
                'message' => 'Falha ao gerar token na API Prata.',
                'login_id' => $login['id'],
                'login' => $login['login'],
                'id_consulta_prata' => $idConsultaPrata,
                'status' => $loginResponse->status(),
                'body' => $loginResponse->json() ?? $loginResponse->body(),
            ], 502);
        }

        $bearerToken = $loginResponse->json('data.token');
        $accountId = $loginResponse->json('data.account.id');
        $accountToken = $loginResponse->json('data.account.token');

        if (!$bearerToken || !$accountId || !$accountToken) {
            return response()->json([
                'message' => 'Campos obrigatorios nao retornados na API de login Prata.',
                'login_id' => $login['id'],
                'id_consulta_prata' => $idConsultaPrata,
                'body' => $loginResponse->json() ?? $loginResponse->body(),
            ], 502);
        }

        DB::connection('sqlsrv_kinghost_vps')
            ->table('consultas_prata.dbo.limites_prata')
            ->where('id', $login['id'])
            ->update([
                'token' => $bearerToken,
                'account_id' => (string) $accountId,
                'account_token' => (string) $accountToken,
                'updated_at' => now(),
            ]);

        $authorizationTermResponse = Http::acceptJson()
            ->timeout(30)
            ->withHeaders([
                'Content-Type' => 'application/json',
                'x-prata-client' => $prataClient,
            ])
            ->post(self::AUTHORIZATION_TERM_URL, [
                'name' => $data['name'],
                'document' => $document,
                'email' => $data['email'] ?? 'cliente@gmail.com',
                'number' => $data['number'] ?? '980733602',
                'area_code' => $data['area_code'] ?? '11',
                'ip_address' => $data['ip_address'] ?? '192.168.0.1',
                'lat' => $data['lat'] ?? '-23.5505',
                'long' => $data['long'] ?? '-46.6333',
                'model' => $data['model'] ?? 'Mozilla/5.0',
                'account_id' => (int) $accountId,
                'token' => $accountToken,
            ]);

        if (!$authorizationTermResponse->successful()) {
            return response()->json([
                'message' => 'Falha ao gerar authorization_term na API Prata.',
                'login_id' => $login['id'],
                'login' => $login['login'],
                'id_consulta_prata' => $idConsultaPrata,
                'status' => $authorizationTermResponse->status(),
                'body' => $authorizationTermResponse->json() ?? $authorizationTermResponse->body(),
            ], 502);
        }

        $authorizationPayload = $authorizationTermResponse->json() ?? [];
        $authorizationStatus = strtolower((string) data_get($authorizationPayload, 'data.status', ''));
        if ($authorizationStatus !== 'authorized') {
            return response()->json([
                'message' => 'Authorization term nao autorizado na API Prata.',
                'login_id' => $login['id'],
                'login' => $login['login'],
                'id_consulta_prata' => $idConsultaPrata,
                'authorization_status' => $authorizationStatus ?: null,
                'body' => $authorizationPayload,
            ], 409);
        }

        try {
            $quota = $this->consumeQuota($login['id']);
        } catch (\Throwable $e) {
            return response()->json([
                'message' => 'Nao foi possivel atualizar consumo em limites_prata.',
                'login_id' => $login['id'],
                'id_consulta_prata' => $idConsultaPrata,
                'error' => $e->getMessage(),
            ], 409);
        }

        $balanceResult = $this->pollBalance($bearerToken, $prataClient, $document);
        if (!$balanceResult['ok']) {
            return response()->json([
                'message' => 'Falha ao consultar saldo na API Prata.',
                'login_id' => $login['id'],
                'login' => $login['login'],
                'id_consulta_prata' => $idConsultaPrata,
                'status' => $balanceResult['status'] ?? 502,
                'body' => $balanceResult['body'] ?? null,
            ], 502);
        }

        $balancePayload = $balanceResult['payload'] ?? [];
        $businessError = data_get($balancePayload, 'error.message');
        $statusConsulta = $this->resolveStatusConsulta($balancePayload, $businessError, (bool) ($balanceResult['pending'] ?? false));

        try {
            $this->mergeConsultaPrata(
                $balancePayload,
                $authorizationPayload,
                $login,
                $pendingConsulta,
                $idConsultaPrata,
                $statusConsulta,
                $businessError
            );
        } catch (\Throwable $e) {
            return response()->json([
                'message' => 'Consulta realizada, mas falhou ao persistir em consulta_prata.',
                'id_consulta_prata' => $idConsultaPrata,
                'login_utilizado' => [
                    'id' => $login['id'],
                    'login' => $login['login'],
                    'id_user' => $login['id_user'] ?? null,
                    'equipe_id' => $login['equipe_id'] ?? null,
                ],
                'consumo' => $quota,
                'authorization_term' => $authorizationPayload,
                'resultado' => $balancePayload,
                'business_error' => $businessError,
                'status_consulta' => $statusConsulta,
                'persist_error' => $e->getMessage(),
            ], 500);
        }

        return response()->json([
            'message' => $businessError
                ? 'Consulta Prata concluida com retorno de negocio.'
                : 'Consulta Prata concluida com sucesso.',
            'login_utilizado' => [
                'id' => $login['id'],
                'login' => $login['login'],
                'id_user' => $login['id_user'] ?? null,
                'equipe_id' => $login['equipe_id'] ?? null,
            ],
            'id_consulta_prata' => $idConsultaPrata,
            'consumo' => $quota,
            'authorization_term' => $authorizationPayload,
            'resultado' => $balancePayload,
            'business_error' => $businessError,
            'status_consulta' => $statusConsulta,
            'polling' => [
                'attempts' => $balanceResult['attempts'] ?? 1,
                'pending_timeout' => (bool) ($balanceResult['pending'] ?? false),
            ],
        ]);
    }

    private function loadPendingRows(int $limit, ?int $idUserScope, ?int $idEquipeScope): array
    {
        $query = DB::connection('sqlsrv_kinghost_vps')
            ->table('consultas_prata.dbo.consulta_prata')
            ->select('*')
            ->whereRaw('LOWER(LTRIM(RTRIM([status_consulta]))) = ?', ['pendente']);

        if ($idUserScope !== null) {
            $query->where('id_user', $idUserScope);
        }

        if ($idEquipeScope !== null) {
            $query->where('equipe_id', $idEquipeScope);
        }

        return $query
            ->orderBy('updated_at')
            ->orderBy('created_at')
            ->orderBy('id_consulta_prata')
            ->limit($limit)
            ->get()
            ->all();
    }

    private function pickFirstText(object $row, array $fields): string
    {
        foreach ($fields as $field) {
            if (!property_exists($row, $field)) {
                continue;
            }
            $value = trim((string) ($row->{$field} ?? ''));
            if ($value !== '') {
                return $value;
            }
        }
        return '';
    }

    private function digitsOnly(string $value): string
    {
        return preg_replace('/\D/', '', $value);
    }

    private function getPendingConsultaById(int $idConsultaPrata): ?array
    {
        $row = DB::connection('sqlsrv_kinghost_vps')
            ->table('consultas_prata.dbo.consulta_prata')
            ->select(['id_consulta_prata', 'status_consulta', 'nome', 'id_user', 'equipe_id'])
            ->where('id_consulta_prata', $idConsultaPrata)
            ->first();

        if (!$row) return null;

        return [
            'id_consulta_prata' => (int) $row->id_consulta_prata,
            'status_consulta' => $row->status_consulta,
            'nome' => $row->nome,
            'id_user' => $row->id_user ? (int) $row->id_user : null,
            'equipe_id' => $row->equipe_id ? (int) $row->equipe_id : null,
        ];
    }

    private function pollBalance(string $bearerToken, string $prataClient, string $document): array
    {
        $attempts = max((int) env('PRATA_BALANCE_POLL_ATTEMPTS', self::BALANCE_POLL_ATTEMPTS), 1);
        $intervalSeconds = max((int) env('PRATA_BALANCE_POLL_SECONDS', self::BALANCE_POLL_SECONDS), 1);
        $lastPayload = null;

        for ($attempt = 1; $attempt <= $attempts; $attempt++) {
            $response = Http::acceptJson()
                ->timeout(30)
                ->withHeaders([
                    'Authorization' => "Bearer {$bearerToken}",
                    'x-prata-client' => $prataClient,
                ])
                ->get(self::BALANCE_URL, [
                    'document' => $document,
                ]);

            if (!$response->successful()) {
                return [
                    'ok' => false,
                    'status' => $response->status(),
                    'body' => $response->json() ?? $response->body(),
                    'attempts' => $attempt,
                ];
            }

            $payload = $response->json() ?? [];
            $lastPayload = $payload;

            $businessError = data_get($payload, 'error.message');
            if ($businessError) {
                return [
                    'ok' => true,
                    'payload' => $payload,
                    'pending' => false,
                    'attempts' => $attempt,
                ];
            }

            $status = strtolower((string) (
                data_get($payload, 'data.status')
                ?: data_get($payload, 'data.status_description')
                ?: ''
            ));
            $isPending = in_array($status, ['pending', 'processing', 'in_progress'], true);

            if (!$isPending) {
                return [
                    'ok' => true,
                    'payload' => $payload,
                    'pending' => false,
                    'attempts' => $attempt,
                ];
            }

            if ($attempt < $attempts) {
                sleep($intervalSeconds);
            }
        }

        return [
            'ok' => true,
            'payload' => $lastPayload ?? [],
            'pending' => true,
            'attempts' => $attempts,
        ];
    }

    private function resolveStatusConsulta(array $balancePayload, ?string $businessError, bool $pendingTimeout): string
    {
        if ($businessError) {
            return 'sem_margem';
        }
        if ($pendingTimeout) {
            return 'pending_timeout';
        }

        $status = strtolower((string) (
            data_get($balancePayload, 'data.status_description')
            ?: data_get($balancePayload, 'data.status')
            ?: ''
        ));

        return $status !== '' ? $status : 'sem_status';
    }

    private function mergeConsultaPrata(
        array $balancePayload,
        array $authorizationPayload,
        array $login,
        array $pendingConsulta,
        int $idConsultaPrata,
        string $statusConsulta,
        ?string $businessError
    ): void {
        $balanceData = (array) data_get($balancePayload, 'data', []);
        $authorizationData = (array) data_get($authorizationPayload, 'data', []);

        $nome = $this->toStringOrNull(data_get($balanceData, 'name'));
        if ($nome === null) {
            $nome = $this->toStringOrNull($pendingConsulta['nome'] ?? null);
        }
        $sexo = $this->toStringOrNull(data_get($balanceData, 'gender'));
        $elegivel = $this->toBitOrNull(data_get($balanceData, 'eligible'));
        $dtNascimento = $this->toStringOrNull(data_get($balanceData, 'birth_date'));
        $tipoBloqueio = $this->toStringOrNull(data_get($balanceData, 'block_type'));
        $blockedAt = $this->toStringOrNull(data_get($balanceData, 'blocked_at'));
        $nomeMae = $this->toStringOrNull(data_get($balanceData, 'mother_name'));
        $expiraConsulta = $this->toStringOrNull(
            data_get($balanceData, 'authorization_term.expiration_date')
            ?? data_get($authorizationData, 'expiration_date')
        );
        $margemBase = $this->toDecimalOrNull(data_get($balanceData, 'base_margin_amount'));
        $motivoInelegibilidade = $this->toStringOrNull(
            $businessError ?: data_get($balanceData, 'ineligibility_reason')
        );
        $qtdContratosSuspensos = $this->toIntOrNull(data_get($balanceData, 'suspended_loans_count'));
        $margemDisponivel = $this->toDecimalOrNull(data_get($balanceData, 'available_margin_amount'));
        $fornecedorId = $this->toIntOrNull(data_get($balanceData, 'supplier_id') ?? data_get($authorizationData, 'supplier_id'));
        $margemTotalDisponivel = $this->toDecimalOrNull(data_get($balanceData, 'total_available_margin_amount'));
        $saldo6 = $this->toDecimalOrNull(data_get($balanceData, 'available_total_balance_with_6_installments'));
        $emissao6 = $this->toDecimalOrNull(data_get($balanceData, 'issue_amount_with_6_installments'));
        $saldo12 = $this->toDecimalOrNull(data_get($balanceData, 'available_total_balance_with_12_installments'));
        $emissao12 = $this->toDecimalOrNull(data_get($balanceData, 'issue_amount_with_12_installments'));
        $saldo24 = $this->toDecimalOrNull(data_get($balanceData, 'available_total_balance_with_24_installments'));
        $emissao24 = $this->toDecimalOrNull(data_get($balanceData, 'issue_amount_with_24_installments'));
        $veioDoCache = $this->toBitOrNull(data_get($balanceData, 'cache'));
        $idUser = $this->toIntOrNull($login['id_user'] ?? $pendingConsulta['id_user'] ?? null);
        $equipeId = $this->toIntOrNull($login['equipe_id'] ?? $pendingConsulta['equipe_id'] ?? null);

        $sql = <<<SQL
MERGE [consultas_prata].[dbo].[consulta_prata] AS target
USING (SELECT CAST(? AS BIGINT) AS id_consulta_prata) AS source
ON target.id_consulta_prata = source.id_consulta_prata
WHEN MATCHED THEN
  UPDATE SET
    nome = ?,
    sexo = ?,
    elegivel = ?,
    dt_nascimento = ?,
    tipo_bloqueio = ?,
    blocked_at = ?,
    nome_mae = ?,
    expira_consulta = ?,
    margem_base = ?,
    motivo_inelegibilidade = ?,
    qtd_contratos_suspensos = ?,
    margem_disponivel = ?,
    status_consulta = ?,
    fornecedor_id = ?,
    margem_total_disponivel = ?,
    saldo_total_disp_6_parcelas = ?,
    valor_emissao_6_parcelas = ?,
    saldo_total_disp_12_parcelas = ?,
    valor_emissao_12_parcelas = ?,
    saldo_total_disp_24_parcelas = ?,
    valor_emissao_24_parcelas = ?,
    veio_do_cache = ?,
    id_user = ?,
    equipe_id = ?,
    updated_at = GETDATE()
WHEN NOT MATCHED THEN
  INSERT (
    id_consulta_prata,
    nome,
    sexo,
    elegivel,
    dt_nascimento,
    tipo_bloqueio,
    blocked_at,
    nome_mae,
    expira_consulta,
    margem_base,
    motivo_inelegibilidade,
    qtd_contratos_suspensos,
    margem_disponivel,
    status_consulta,
    fornecedor_id,
    margem_total_disponivel,
    saldo_total_disp_6_parcelas,
    valor_emissao_6_parcelas,
    saldo_total_disp_12_parcelas,
    valor_emissao_12_parcelas,
    saldo_total_disp_24_parcelas,
    valor_emissao_24_parcelas,
    veio_do_cache,
    created_at,
    updated_at,
    id_user,
    equipe_id
  )
  VALUES (
    source.id_consulta_prata,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?
  );
SQL;

        DB::connection('sqlsrv_kinghost_vps')->statement($sql, [
            $idConsultaPrata,
            $nome,
            $sexo,
            $elegivel,
            $dtNascimento,
            $tipoBloqueio,
            $blockedAt,
            $nomeMae,
            $expiraConsulta,
            $margemBase,
            $motivoInelegibilidade,
            $qtdContratosSuspensos,
            $margemDisponivel,
            $statusConsulta,
            $fornecedorId,
            $margemTotalDisponivel,
            $saldo6,
            $emissao6,
            $saldo12,
            $emissao12,
            $saldo24,
            $emissao24,
            $veioDoCache,
            $idUser,
            $equipeId,
            $nome,
            $sexo,
            $elegivel,
            $dtNascimento,
            $tipoBloqueio,
            $blockedAt,
            $nomeMae,
            $expiraConsulta,
            $margemBase,
            $motivoInelegibilidade,
            $qtdContratosSuspensos,
            $margemDisponivel,
            $statusConsulta,
            $fornecedorId,
            $margemTotalDisponivel,
            $saldo6,
            $emissao6,
            $saldo12,
            $emissao12,
            $saldo24,
            $emissao24,
            $veioDoCache,
            $idUser,
            $equipeId,
        ]);
    }

    private function toIntOrNull($value): ?int
    {
        if ($value === null || $value === '') return null;
        if (!is_numeric($value)) return null;
        return (int) $value;
    }

    private function toDecimalOrNull($value): ?float
    {
        if ($value === null || $value === '') return null;
        if (!is_numeric($value)) return null;
        return (float) $value;
    }

    private function toBitOrNull($value): ?int
    {
        if ($value === null || $value === '') return null;
        return filter_var($value, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE) ? 1 : 0;
    }

    private function toStringOrNull($value): ?string
    {
        if ($value === null) return null;
        $text = trim((string) $value);
        return $text === '' ? null : $text;
    }

    private function pickLoginById(int $limiteId): array
    {
        $row = DB::connection('sqlsrv_kinghost_vps')
            ->table('consultas_prata.dbo.limites_prata')
            ->select(['id', 'login', 'senha', 'total', 'consultados', 'limite', 'updated_at', 'id_user', 'equipe_id'])
            ->where('id', $limiteId)
            ->whereNotNull('login')
            ->whereNotNull('senha')
            ->first();

        if (!$row) {
            throw new RuntimeException('Login nao encontrado em limites_prata para o id_consulta_prata informado.');
        }

        $total = max((int) ($row->total ?? self::LIMIT_PER_HOUR), 0);
        if ($total <= 0) {
            $total = self::LIMIT_PER_HOUR;
        }
        $consultados = max((int) ($row->consultados ?? 0), 0);
        $limite = (int) ($row->limite ?? ($total - $consultados));
        $limite = max($limite, 0);

        $updatedAt = $row->updated_at ? Carbon::parse($row->updated_at) : null;
        if (!$updatedAt || $updatedAt->lte(now()->subHour())) {
            $consultados = 0;
            $limite = $total;
        }

        if ($limite <= 0) {
            throw new RuntimeException('Login vinculado sem saldo de consultas disponivel.');
        }

        return [
            'id' => (int) $row->id,
            'login' => $row->login,
            'senha' => $row->senha,
            'id_user' => $row->id_user ? (int) $row->id_user : null,
            'equipe_id' => $row->equipe_id ? (int) $row->equipe_id : null,
        ];
    }

    private function consumeQuota(int $loginId): array
    {
        return DB::connection('sqlsrv_kinghost_vps')->transaction(function () use ($loginId) {
            $row = DB::connection('sqlsrv_kinghost_vps')
                ->table('consultas_prata.dbo.limites_prata')
                ->where('id', $loginId)
                ->lockForUpdate()
                ->first();

            if (!$row) {
                throw new RuntimeException('Login selecionado nao encontrado em limites_prata.');
            }

            $total = max((int) ($row->total ?? self::LIMIT_PER_HOUR), 0);
            if ($total <= 0) {
                $total = self::LIMIT_PER_HOUR;
            }
            $consultados = max((int) ($row->consultados ?? 0), 0);
            $limite = (int) ($row->limite ?? ($total - $consultados));
            $limite = max($limite, 0);

            $updatedAt = $row->updated_at ? Carbon::parse($row->updated_at) : null;
            if (!$updatedAt || $updatedAt->lte(now()->subHour())) {
                $consultados = 0;
                $limite = $total;
            }

            if ($limite <= 0) {
                throw new RuntimeException('Login sem saldo de consultas disponivel.');
            }

            $consultados++;
            $limite--;

            DB::connection('sqlsrv_kinghost_vps')
                ->table('consultas_prata.dbo.limites_prata')
                ->where('id', $loginId)
                ->update([
                    'consultados' => $consultados,
                    'limite' => $limite,
                    'updated_at' => now(),
                ]);

            return [
                'id' => $loginId,
                'total' => $total,
                'consultados' => $consultados,
                'limite' => $limite,
                'limite_por_hora' => self::LIMIT_PER_HOUR,
            ];
        });
    }
}
