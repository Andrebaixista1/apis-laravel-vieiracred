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
    private const RETRYABLE_V8_STATUSES = [
        'WAITING_CONSENT',
        'WAITING_CONSULT',
        'WAITING_CREDIT_ANALYSIS',
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

        $cursor = 0;
        $totalAccounts = count($accounts);

        foreach ($clients as $client) {
            $tries = 0;
            while ($tries < $totalAccounts && $accounts[$cursor]['remaining'] <= 0) {
                $cursor = ($cursor + 1) % $totalAccounts;
                $tries++;
            }

            if ($tries >= $totalAccounts && $accounts[$cursor]['remaining'] <= 0) {
                break;
            }

            $distribution[$accounts[$cursor]['id']][] = $client;
            $accounts[$cursor]['remaining']--;
            $cursor = ($cursor + 1) % $totalAccounts;
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

            $newId = $this->duplicateClientRow((int) $client->id);
            $this->mergeClientRow($newId, $payload);
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
}
