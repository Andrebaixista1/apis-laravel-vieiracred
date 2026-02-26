<?php

namespace App\Http\Controllers;

use Carbon\Carbon;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;

class ConsultaPresencaController extends Controller
{
    private const DB_CONNECTION = 'sqlsrv_kinghost_vps';

    private const PRESENCA_BASE_URL = 'https://presenca-bank-api.azurewebsites.net';
    private const PRESENCA_LOGIN_URL = self::PRESENCA_BASE_URL.'/login';
    private const PRESENCA_TERMO_INSS_URL = self::PRESENCA_BASE_URL.'/consultas/termo-inss';
    private const PRESENCA_CONSULTAR_VINCULOS_URL = self::PRESENCA_BASE_URL.'/v3/operacoes/consignado-privado/consultar-vinculos';
    private const PRESENCA_CONSULTAR_MARGEM_URL = self::PRESENCA_BASE_URL.'/v3/operacoes/consignado-privado/consultar-margem';
    private const PRESENCA_DISPONIVEIS_URL = self::PRESENCA_BASE_URL.'/v5/operacoes/simulacao/disponiveis';

    private const HTTP_TIMEOUT_SECONDS = 60;
    private const STEP_DELAY_SECONDS = 2;
    private const QUEUE_DELAY_SECONDS = 5;
    private const TERMO_MAX_RETRIES = 3;
    private const TERMO_HEADLESS_TIMEOUT_SECONDS = 60;
    private const TERMO_HEADLESS_SERVICE_URL = 'http://172.17.0.1:3211/accept-termo';
    private const TERMO_HEADLESS_SERVICE_TIMEOUT_SECONDS = 90;

    public function run(Request $request): JsonResponse
    {
        $startedAt = microtime(true);
        $summary = [
            'ok' => true,
            'started_at' => now()->toIso8601String(),
            'finished_at' => null,
            'duration_ms' => 0,
            'total_logins' => 0,
            'logins_com_saldo' => 0,
            'logins_bloqueados' => 0,
            'pendentes_encontrados' => 0,
            'pendentes_alocados' => 0,
            'pendentes_sem_login' => 0,
            'processados' => 0,
            'erros' => 0,
            'logins' => [],
        ];

        $accountLocks = [];
        $loginSummariesById = [];
        $claimedRowsByAccount = [];

        try {
            $accounts = $this->loadAccountsWithAvailableLimit();
            $summary['total_logins'] = count($accounts);
            $summary['logins_com_saldo'] = count(array_filter($accounts, static fn(array $a) => ($a['remaining'] ?? 0) > 0));

            if (empty($accounts)) {
                $summary['message'] = 'Nenhum login com saldo disponivel.';
                $summary['finished_at'] = now()->toIso8601String();
                $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

                return response()->json($summary);
            }

            foreach ($accounts as $account) {
                $accountId = (int) ($account['id'] ?? 0);
                if ($accountId <= 0) {
                    continue;
                }

                $loginSummariesById[$accountId] = [
                    'id' => $accountId,
                    'login' => $account['login'],
                    'saldo_inicio' => (int) ($account['remaining'] ?? 0),
                    'alocados' => 0,
                    'processados' => 0,
                    'erros' => 0,
                ];

                $remaining = max(0, (int) ($account['remaining'] ?? 0));
                if ($remaining <= 0) {
                    continue;
                }

                $lockKey = 'consulta-presenca-run-login:'.$accountId;
                $accountLock = cache()->lock($lockKey, 3600);
                if (! $accountLock->get()) {
                    $summary['logins_bloqueados']++;
                    $loginSummariesById[$accountId]['lock_busy'] = true;
                    continue;
                }

                $accountLocks[$accountId] = $accountLock;

                $claimedRows = $this->claimPendingRowsForAccount($accountId, $remaining);
                if (empty($claimedRows)) {
                    continue;
                }

                $claimedRowsByAccount[$accountId] = $claimedRows;
                $loginSummariesById[$accountId]['alocados'] = count($claimedRows);
                $summary['pendentes_encontrados'] += count($claimedRows);
                $summary['pendentes_alocados'] += count($claimedRows);
            }

            if (empty($claimedRowsByAccount)) {
                $summary['message'] = 'Nenhuma pendencia reservada nesta execucao.';
                $summary['finished_at'] = now()->toIso8601String();
                $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

                foreach ($accounts as $account) {
                    $accountId = (int) ($account['id'] ?? 0);
                    if ($accountId > 0 && isset($loginSummariesById[$accountId])) {
                        $summary['logins'][] = $loginSummariesById[$accountId];
                    }
                }

                return response()->json($summary);
            }

            foreach ($accounts as $account) {
                $accountId = (int) ($account['id'] ?? 0);
                if ($accountId <= 0) {
                    continue;
                }

                $rowsForAccount = $claimedRowsByAccount[$accountId] ?? [];
                if (empty($rowsForAccount)) {
                    continue;
                }

                foreach ($rowsForAccount as $pendingRow) {
                    $pendingId = (int) ($pendingRow->id ?? 0);
                    if ($pendingId <= 0) {
                        continue;
                    }

                    try {
                        $doneRows = $this->processPendingRow($pendingRow, $account);
                        if (empty($doneRows)) {
                            throw new \RuntimeException('Consulta finalizou sem linhas concluidas para upsert.');
                        }

                        $this->upsertPendingResultRows($pendingRow, $doneRows);
                        $this->incrementConsultedCounter($accountId);

                        $summary['processados']++;
                        $loginSummariesById[$accountId]['processados']++;
                    } catch (\Throwable $e) {
                        $summary['erros']++;
                        $loginSummariesById[$accountId]['erros']++;
                        $this->incrementConsultedCounter($accountId);
                        $this->markPendingAsError($pendingId, $e->getMessage());
                    } finally {
                        $this->sleepSeconds(self::QUEUE_DELAY_SECONDS);
                    }
                }
            }

            foreach ($accounts as $account) {
                $accountId = (int) ($account['id'] ?? 0);
                if ($accountId > 0 && isset($loginSummariesById[$accountId])) {
                    $summary['logins'][] = $loginSummariesById[$accountId];
                }
            }

            $summary['finished_at'] = now()->toIso8601String();
            $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

            return response()->json($summary);
        } catch (\Throwable $e) {
            $summary['ok'] = false;
            $summary['message'] = $e->getMessage();
            $summary['finished_at'] = now()->toIso8601String();
            $summary['duration_ms'] = (int) round((microtime(true) - $startedAt) * 1000);

            foreach ($accounts ?? [] as $account) {
                $accountId = (int) ($account['id'] ?? 0);
                if ($accountId > 0 && isset($loginSummariesById[$accountId])) {
                    $summary['logins'][] = $loginSummariesById[$accountId];
                }
            }

            return response()->json($summary, 500);
        } finally {
            foreach ($accountLocks as $accountLock) {
                optional($accountLock)->release();
            }
        }
    }

    public function deleteConsultasByLote(Request $request): JsonResponse
    {
        $idUser = $this->toNullableInt($request->input('id_user'));
        $idEquipe = $this->toNullableInt($request->input('equipe_id') ?? $request->input('id_equipe'));
        $idConsultaPresenca = $this->toNullableBigInt($request->input('id_consulta_presenca'));
        $tipoConsulta = trim((string) $request->input('tipoConsulta', ''));

        if ($idUser === null || $idUser <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'id_user invalido.',
            ], 422);
        }

        if ($idEquipe === null || $idEquipe <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'equipe_id invalido.',
            ], 422);
        }

        if ($idConsultaPresenca === null || $idConsultaPresenca <= 0) {
            return response()->json([
                'ok' => false,
                'message' => 'id_consulta_presenca invalido.',
            ], 422);
        }

        if ($tipoConsulta === '') {
            return response()->json([
                'ok' => false,
                'message' => 'tipoConsulta invalido.',
            ], 422);
        }

        $deleted = 0;
        do {
            $deletedBatch = (int) DB::connection(self::DB_CONNECTION)->delete("
                DELETE TOP (1000) FROM [consultas_presenca].[dbo].[consulta_presenca]
                WHERE [id_user] = ?
                  AND [equipe_id] = ?
                  AND [id_consulta_presenca] = ?
                  AND [tipoConsulta] = ?
            ", [$idUser, $idEquipe, $idConsultaPresenca, $tipoConsulta]);

            $deleted += $deletedBatch;
        } while ($deletedBatch > 0);

        return response()->json([
            'ok' => true,
            'message' => $deleted > 0
                ? 'Lote removido com sucesso.'
                : 'Nenhum registro encontrado para os filtros informados.',
            'deleted_count' => (int) $deleted,
            'filters' => [
                'id_user' => $idUser,
                'equipe_id' => $idEquipe,
                'id_consulta_presenca' => $idConsultaPresenca,
                'tipoConsulta' => $tipoConsulta,
            ],
        ]);
    }

    private function loadAccountsWithAvailableLimit(): array
    {
        $sql = <<<'SQL'
SELECT
    [id],
    [login],
    [senha],
    [total],
    [consultados],
    [limite],
    [created_at],
    [updated_at]
FROM [consultas_presenca].[dbo].[limites_presenca]
ORDER BY [id] ASC
SQL;

        $rows = DB::connection(self::DB_CONNECTION)->select($sql);

        $now = Carbon::now();
        $accounts = [];

        foreach ($rows as $row) {
            $id = (int) ($row->id ?? 0);
            $login = trim((string) ($row->login ?? ''));
            $senha = trim((string) ($row->senha ?? ''));
            $total = max(0, (int) ($row->total ?? 0));
            $consultados = max(0, (int) ($row->consultados ?? 0));

            $updatedAt = $this->parseNullableCarbon($row->updated_at ?? null);
            $canResetDailyCounter = $consultados > 0 && $updatedAt !== null && $updatedAt->lte($now->copy()->subHours(24));

            if ($canResetDailyCounter) {
                $resetSql = <<<'SQL'
UPDATE [consultas_presenca].[dbo].[limites_presenca]
SET [consultados] = 0,
    [updated_at] = SYSDATETIME()
WHERE [id] = ?
SQL;
                DB::connection(self::DB_CONNECTION)->update($resetSql, [$id]);
                $consultados = 0;
            }

            $remaining = max(0, $total - $consultados);

            if ($id <= 0 || $remaining <= 0 || $login === '' || $senha === '') {
                continue;
            }

            $accounts[] = [
                'id' => $id,
                'login' => $login,
                'senha' => $senha,
                'total' => $total,
                'consultados' => $consultados,
                'remaining' => $remaining,
            ];
        }

        return $accounts;
    }

    private function claimPendingRowsForAccount(int $accountId, int $limit): array
    {
        $safeAccountId = max(0, $accountId);
        $safeLimit = max(0, $limit);

        if ($safeAccountId <= 0 || $safeLimit <= 0) {
            return [];
        }

        $sql = <<<SQL
;WITH [rows_to_claim] AS (
    SELECT TOP ($safeLimit)
        [id],
        [cpf],
        [nome],
        [telefone],
        [created_at],
        [updated_at],
        [tipoConsulta],
        [status],
        [mensagem],
        [id_user],
        [equipe_id],
        [id_role],
        [id_consulta_presenca]
    FROM [consultas_presenca].[dbo].[consulta_presenca] WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE UPPER(LTRIM(RTRIM(COALESCE([status], '')))) = 'PENDENTE'
      AND [id_consulta_presenca] = ?
    ORDER BY [id] ASC
)
UPDATE [rows_to_claim]
SET [status] = 'Processando',
    [mensagem] = NULL,
    [updated_at] = SYSDATETIME()
OUTPUT
    INSERTED.[id],
    INSERTED.[cpf],
    INSERTED.[nome],
    INSERTED.[telefone],
    INSERTED.[created_at],
    INSERTED.[updated_at],
    INSERTED.[tipoConsulta],
    INSERTED.[status],
    INSERTED.[mensagem],
    INSERTED.[id_user],
    INSERTED.[equipe_id],
    INSERTED.[id_role],
    INSERTED.[id_consulta_presenca];
SQL;

        return DB::connection(self::DB_CONNECTION)->select($sql, [$safeAccountId]);
    }

    private function loadPendingRows(int $limit): array
    {
        $safeLimit = max(0, $limit);
        if ($safeLimit === 0) {
            return [];
        }

        $sql = <<<SQL
SELECT TOP ($safeLimit)
    [id],
    [cpf],
    [nome],
    [telefone],
    [created_at],
    [updated_at],
    [tipoConsulta],
    [status],
    [mensagem],
    [id_user],
    [equipe_id],
    [id_role],
    [id_consulta_presenca]
FROM [consultas_presenca].[dbo].[consulta_presenca]
WHERE UPPER(LTRIM(RTRIM(COALESCE([status], '')))) = 'PENDENTE'
ORDER BY [id] ASC
SQL;

        return DB::connection(self::DB_CONNECTION)->select($sql);
    }

    private function processPendingRow(object $pendingRow, array $account): array
    {
        $input = $this->buildInputFromPendingRow($pendingRow);
        $token = $this->presencaLogin($account['login'], $account['senha']);

        $this->sleepSeconds(self::STEP_DELAY_SECONDS);
        $termo = $this->presencaCriarTermoInss($token, $input);

        $shortUrl = trim((string) ($termo['shortUrl'] ?? ''));
        if ($shortUrl === '') {
            throw new \RuntimeException('API termo-inss retornou sem shortUrl.');
        }

        if (! $this->presencaAceitarTermo($shortUrl, (string) ($termo['autorizacaoId'] ?? ''))) {
            throw new \RuntimeException('Nao foi possivel confirmar o termo no shortUrl.');
        }

        $this->sleepSeconds(self::STEP_DELAY_SECONDS);
        $vinculos = $this->presencaConsultarVinculos($token, $input['cpf']);
        $vinculo = $this->selecionarVinculo($vinculos);

        $this->sleepSeconds(self::STEP_DELAY_SECONDS);
        $margem = $this->presencaConsultarMargem(
            $token,
            $input['cpf'],
            (string) ($vinculo['matricula'] ?? ''),
            (string) ($vinculo['numeroInscricaoEmpregador'] ?? '')
        );

        $this->sleepSeconds(self::STEP_DELAY_SECONDS);
        $disponiveis = $this->presencaConsultarDisponiveis($token, $pendingRow, $input, $vinculo, $margem);

        $rows = $this->buildFlowResultRows($pendingRow, $input, $vinculo, $margem, $disponiveis, $termo);
        if (empty($rows)) {
            throw new \RuntimeException('Fluxo concluido sem linhas para persistir.');
        }

        return $this->dedupeRows($rows);
    }

    private function presencaLogin(string $login, string $senha): string
    {
        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->acceptJson()
            ->asJson()
            ->post(self::PRESENCA_LOGIN_URL, [
                'login' => $login,
                'senha' => $senha,
            ]);

        if (! $response->successful()) {
            throw new \RuntimeException('Falha login Presenca: '.$response->status().' '.$this->truncate((string) $response->body(), 350));
        }

        $payload = $this->decodeJson((string) $response->body());
        $token = trim((string) $this->pick($payload, ['token'], ''));
        if ($token === '') {
            throw new \RuntimeException('Login Presenca sem token na resposta.');
        }

        return $token;
    }

    private function presencaCriarTermoInss(string $token, array $input): array
    {
        $payload = [
            'cpf' => $input['cpf'],
            'nome' => $input['nome'],
            'telefone' => $input['telefone'],
            'produtoId' => 28,
        ];

        $response = null;
        $lastStatus = 0;
        $lastBody = '';
        for ($attempt = 1; $attempt <= self::TERMO_MAX_RETRIES; $attempt++) {
            $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
                ->acceptJson()
                ->withToken($token)
                ->asJson()
                ->post(self::PRESENCA_TERMO_INSS_URL, $payload);

            $lastStatus = (int) $response->status();
            $lastBody = (string) $response->body();

            if ($response->successful()) {
                break;
            }

            $retryable = in_array($lastStatus, [429, 500, 502, 503, 504], true);
            if (! $retryable || $attempt >= self::TERMO_MAX_RETRIES) {
                break;
            }

            $this->sleepSeconds(self::STEP_DELAY_SECONDS);
        }

        if (! $response || ! $response->successful()) {
            throw new \RuntimeException('Falha termo-inss: '.$lastStatus.' '.$this->truncate($lastBody, 350));
        }

        $payload = $this->decodeJson((string) $response->body());
        $autorizacaoId = trim((string) $this->pick($payload, ['autorizacaoId'], ''));
        $shortUrl = trim((string) $this->pick($payload, ['shortUrl'], ''));

        return [
            'autorizacaoId' => $autorizacaoId,
            'shortUrl' => $shortUrl,
        ];
    }

    private function presencaAceitarTermo(string $shortUrl, string $autorizacaoId = ''): bool
    {
        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->withHeaders(['Accept' => 'text/html,application/xhtml+xml'])
            ->get($shortUrl);

        if (! $response->successful()) {
            throw new \RuntimeException('Falha ao abrir shortUrl: '.$response->status().' '.$this->truncate((string) $response->body(), 260));
        }

        $html = (string) $response->body();
        if ($this->hasPositiveTermoHtmlSignal($html)) {
            return true;
        }

        $form = $this->extractFirstForm($html, $shortUrl);
        if ($form === null) {
            // SPA sem form tradicional: usa aceite headless real via Playwright.
            return $this->presencaAceitarTermoHeadless($shortUrl, $autorizacaoId);
        }

        $fields = $form['fields'];
        $foundConsentField = false;
        foreach (array_keys($fields) as $name) {
            $key = mb_strtolower((string) $name);
            if (
                str_contains($key, 'aceit')
                || str_contains($key, 'termo')
                || str_contains($key, 'consent')
                || str_contains($key, 'autoriz')
                || str_contains($key, 'lgpd')
            ) {
                $fields[$name] = $fields[$name] !== '' ? $fields[$name] : 'on';
                $foundConsentField = true;
            }
        }

        if (! $foundConsentField) {
            $fields['aceite'] = 'true';
            $fields['aceito'] = 'true';
            $fields['confirmar'] = '1';
        }

        $method = strtoupper((string) ($form['method'] ?? 'POST'));
        $action = (string) ($form['action'] ?? $shortUrl);

        $submitResponse = $method === 'GET'
            ? Http::timeout(self::HTTP_TIMEOUT_SECONDS)->get($action, $fields)
            : Http::timeout(self::HTTP_TIMEOUT_SECONDS)->asForm()->post($action, $fields);

        if (! $submitResponse->successful()) {
            throw new \RuntimeException('Falha ao confirmar termo no shortUrl: '.$submitResponse->status().' '.$this->truncate((string) $submitResponse->body(), 260));
        }

        $submitHtml = (string) $submitResponse->body();
        if ($this->hasNegativeTermoHtmlSignal($submitHtml)) {
            return $this->presencaAceitarTermoHeadless($shortUrl, $autorizacaoId);
        }

        if ($this->hasPositiveTermoHtmlSignal($submitHtml)) {
            return true;
        }

        return $this->presencaAceitarTermoHeadless($shortUrl, $autorizacaoId);
    }

    private function presencaAceitarTermoHeadless(string $shortUrl, string $autorizacaoId = ''): bool
    {
        $response = Http::timeout(self::TERMO_HEADLESS_SERVICE_TIMEOUT_SECONDS)
            ->acceptJson()
            ->asJson()
            ->post(self::TERMO_HEADLESS_SERVICE_URL, [
                'shortUrl' => $shortUrl,
                'autorizacaoId' => $autorizacaoId,
                'timeoutSeconds' => self::TERMO_HEADLESS_TIMEOUT_SECONDS,
            ]);

        if (! $response->successful()) {
            throw new \RuntimeException(
                'Falha aceite headless (service): '.$response->status().' '.$this->truncate((string) $response->body(), 450)
            );
        }

        $payload = $this->decodeJson((string) $response->body());
        $ok = (bool) $this->pick($payload, ['ok'], false);
        if ($ok) {
            return true;
        }

        throw new \RuntimeException(
            'Falha aceite headless (service): '.$this->truncate((string) $response->body(), 450)
        );
    }

    private function extractFirstForm(string $html, string $baseUrl): ?array
    {
        $dom = new \DOMDocument();
        $loaded = @$dom->loadHTML($html, LIBXML_NOWARNING | LIBXML_NOERROR);
        if (! $loaded) {
            return null;
        }

        $forms = $dom->getElementsByTagName('form');
        if ($forms->length === 0) {
            return null;
        }

        $form = $forms->item(0);
        if (! $form) {
            return null;
        }

        $actionRaw = trim((string) $form->getAttribute('action'));
        $methodRaw = trim((string) $form->getAttribute('method'));
        $action = $this->resolveUrl($baseUrl, $actionRaw !== '' ? $actionRaw : $baseUrl);
        $method = $methodRaw !== '' ? strtoupper($methodRaw) : 'POST';

        $fields = [];

        foreach ($form->getElementsByTagName('input') as $input) {
            $name = trim((string) $input->getAttribute('name'));
            if ($name === '') {
                continue;
            }

            $type = mb_strtolower(trim((string) $input->getAttribute('type')));
            $value = (string) $input->getAttribute('value');

            if ($type === 'checkbox' || $type === 'radio') {
                $isChecked = $input->hasAttribute('checked');
                $isConsentField = str_contains(mb_strtolower($name), 'aceit')
                    || str_contains(mb_strtolower($name), 'termo')
                    || str_contains(mb_strtolower($name), 'consent')
                    || str_contains(mb_strtolower($name), 'autoriz')
                    || str_contains(mb_strtolower($name), 'lgpd');

                if (! $isChecked && ! $isConsentField) {
                    continue;
                }

                $fields[$name] = $value !== '' ? $value : 'on';
                continue;
            }

            $fields[$name] = $value;
        }

        foreach ($form->getElementsByTagName('textarea') as $textarea) {
            $name = trim((string) $textarea->getAttribute('name'));
            if ($name === '') {
                continue;
            }
            $fields[$name] = trim((string) $textarea->textContent);
        }

        foreach ($form->getElementsByTagName('select') as $select) {
            $name = trim((string) $select->getAttribute('name'));
            if ($name === '') {
                continue;
            }

            $selectedValue = '';
            foreach ($select->getElementsByTagName('option') as $option) {
                if ($option->hasAttribute('selected')) {
                    $selectedValue = (string) $option->getAttribute('value');
                    break;
                }
                if ($selectedValue === '') {
                    $selectedValue = (string) $option->getAttribute('value');
                }
            }

            $fields[$name] = $selectedValue;
        }

        return [
            'action' => $action,
            'method' => $method,
            'fields' => $fields,
        ];
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

    private function hasPositiveTermoHtmlSignal(string $html): bool
    {
        $normalized = mb_strtolower($this->normalizeText($html));
        return str_contains($normalized, 'termo aceito')
            || str_contains($normalized, 'autorizacao confirmada')
            || str_contains($normalized, 'enviado com sucesso')
            || str_contains($normalized, 'assinatura concluida')
            || str_contains($normalized, 'obrigado');
    }

    private function hasNegativeTermoHtmlSignal(string $html): bool
    {
        $normalized = mb_strtolower($this->normalizeText($html));
        return str_contains($normalized, 'erro')
            || str_contains($normalized, 'expirado')
            || str_contains($normalized, 'invalido')
            || str_contains($normalized, 'inválido')
            || str_contains($normalized, 'nao autorizado')
            || str_contains($normalized, 'não autorizado');
    }

    private function presencaConsultarVinculos(string $token, string $cpf): array
    {
        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->acceptJson()
            ->withToken($token)
            ->asJson()
            ->post(self::PRESENCA_CONSULTAR_VINCULOS_URL, [
                'cpf' => $cpf,
            ]);

        if (! $response->successful()) {
            throw new \RuntimeException('Falha consultar-vinculos: '.$response->status().' '.$this->truncate((string) $response->body(), 350));
        }

        $payload = $this->decodeJson((string) $response->body());
        $vinculos = $this->extractVinculos($payload);
        if (empty($vinculos)) {
            throw new \RuntimeException('consultar-vinculos retornou vazio.');
        }

        return $vinculos;
    }

    private function extractVinculos($payload): array
    {
        $rows = [];
        if (is_array($payload)) {
            if (isset($payload['id']) && is_array($payload['id'])) {
                $rows = $payload['id'];
            } elseif (isset($payload['data']) && is_array($payload['data'])) {
                $rows = $payload['data'];
            } elseif (isset($payload['rows']) && is_array($payload['rows'])) {
                $rows = $payload['rows'];
            } elseif (isset($payload['vinculos']) && is_array($payload['vinculos'])) {
                $rows = $payload['vinculos'];
            } elseif ($this->isAssoc($payload)) {
                $rows = [$payload];
            } else {
                $rows = $payload;
            }
        }

        $out = [];
        foreach ($rows as $row) {
            $matricula = trim((string) $this->pick($row, ['matricula', 'registroEmpregaticio', 'registro_empregaticio'], ''));
            $cnpj = trim((string) $this->pick($row, ['numeroInscricaoEmpregador', 'cnpj', 'cnpjEmpregador'], ''));
            if ($matricula === '' || $cnpj === '') {
                continue;
            }

            $out[] = [
                'matricula' => $matricula,
                'numeroInscricaoEmpregador' => $cnpj,
                'elegivel' => $this->pick($row, ['elegivel'], null),
                'cpf' => $this->pick($row, ['cpf'], null),
            ];
        }

        return $out;
    }

    private function selecionarVinculo(array $vinculos): array
    {
        foreach ($vinculos as $vinculo) {
            $token = $this->normalizeBooleanToken($this->pick($vinculo, ['elegivel'], null));
            if ($token === 'true') {
                return $vinculo;
            }
        }

        if (! empty($vinculos)) {
            return $vinculos[0];
        }

        throw new \RuntimeException('Nenhum vinculo valido encontrado.');
    }

    private function presencaConsultarMargem(string $token, string $cpf, string $matricula, string $cnpj): array
    {
        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->acceptJson()
            ->withToken($token)
            ->asJson()
            ->post(self::PRESENCA_CONSULTAR_MARGEM_URL, [
                'cpf' => $cpf,
                'matricula' => $matricula,
                'cnpj' => $cnpj,
            ]);

        if (! $response->successful()) {
            throw new \RuntimeException('Falha consultar-margem: '.$response->status().' '.$this->truncate((string) $response->body(), 350));
        }

        $payload = $this->decodeJson((string) $response->body());
        if (is_array($payload) && ! $this->isAssoc($payload) && isset($payload[0]) && is_array($payload[0])) {
            $payload = $payload[0];
        }

        return [
            'valorMargemDisponivel' => $this->pick($payload, ['valorMargemDisponivel'], null),
            'valorMargemBase' => $this->pick($payload, ['valorMargemBase'], null),
            'valorTotalDevido' => $this->pick($payload, ['valorTotalDevido'], null),
            'registroEmpregaticio' => $this->pick($payload, ['registroEmpregaticio', 'matricula'], $matricula),
            'cnpjEmpregador' => $this->pick($payload, ['cnpjEmpregador', 'cnpj'], $cnpj),
            'dataAdmissao' => $this->pick($payload, ['dataAdmissao'], null),
            'dataNascimento' => $this->pick($payload, ['dataNascimento'], null),
            'nomeMae' => $this->pick($payload, ['nomeMae'], null),
            'sexo' => $this->pick($payload, ['sexo'], null),
        ];
    }

    private function presencaConsultarDisponiveis(string $token, object $pendingRow, array $input, array $vinculo, array $margem): array
    {
        [$ddd, $numero] = $this->formatTelefoneParaPayload($input['telefone']);
        $valorParcela = $this->toFloat($this->pick($margem, ['valorMargemDisponivel'], 0.0));
        if ($valorParcela < 0) {
            $valorParcela = 0.0;
        }

        $payload = [
            'tomador' => [
                'cpf' => $input['cpf'],
                'nome' => $input['nome'],
                'telefone' => [
                    'ddd' => $ddd,
                    'numero' => $numero,
                ],
                'dataNascimento' => $this->toNullableDate($this->pick($margem, ['dataNascimento'], null)),
                'email' => (string) $this->pick($pendingRow, ['email'], 'emailmock@mock.com.br'),
                'sexo' => $this->pick($margem, ['sexo'], null),
                'nomeMae' => $this->pick($margem, ['nomeMae'], null),
                'vinculoEmpregaticio' => [
                    'cnpjEmpregador' => (string) $this->pick($vinculo, ['numeroInscricaoEmpregador'], ''),
                    'registroEmpregaticio' => (string) $this->pick($vinculo, ['matricula'], ''),
                ],
                'dadosBancarios' => [
                    'codigoBanco' => null,
                    'agencia' => null,
                    'conta' => null,
                    'digitoConta' => null,
                    'formaCredito' => null,
                ],
                'endereco' => [
                    'cep' => '',
                    'rua' => '',
                    'numero' => '',
                    'complemento' => '',
                    'cidade' => '',
                    'estado' => '',
                    'bairro' => '',
                ],
            ],
            'proposta' => [
                'valorSolicitado' => 0,
                'quantidadeParcelas' => 0,
                'produtoId' => 28,
                'valorParcela' => $valorParcela,
            ],
            'documentos' => [],
        ];

        $response = Http::timeout(self::HTTP_TIMEOUT_SECONDS)
            ->acceptJson()
            ->withToken($token)
            ->asJson()
            ->post(self::PRESENCA_DISPONIVEIS_URL, $payload);

        if (! $response->successful()) {
            throw new \RuntimeException('Falha disponiveis: '.$response->status().' '.$this->truncate((string) $response->body(), 350));
        }

        $payload = $this->decodeJson((string) $response->body());
        return $this->extractDisponiveis($payload);
    }

    private function extractDisponiveis($payload): array
    {
        $rows = [];
        if (is_array($payload)) {
            if (isset($payload['data']) && is_array($payload['data'])) {
                $rows = $payload['data'];
            } elseif (isset($payload['rows']) && is_array($payload['rows'])) {
                $rows = $payload['rows'];
            } elseif ($this->isAssoc($payload)) {
                $rows = [$payload];
            } else {
                $rows = $payload;
            }
        }

        $out = [];
        foreach ($rows as $row) {
            if (! is_array($row)) {
                continue;
            }
            $out[] = [
                'id' => $this->pick($row, ['id'], null),
                'nome' => $this->pick($row, ['nome'], null),
                'prazo' => $this->pick($row, ['prazo'], null),
                'taxaJuros' => $this->pick($row, ['taxaJuros'], null),
                'valorLiberado' => $this->pick($row, ['valorLiberado'], null),
                'valorParcela' => $this->pick($row, ['valorParcela'], null),
                'taxaSeguro' => $this->pick($row, ['taxaSeguro'], null),
                'valorSeguro' => $this->pick($row, ['valorSeguro'], null),
            ];
        }

        return $out;
    }

    private function buildFlowResultRows(object $pendingRow, array $input, array $vinculo, array $margem, array $disponiveis, array $termo): array
    {
        $baseRow = [
            'cpf' => $input['cpf'],
            'nome' => $input['nome'],
            'telefone' => $input['telefone'],
            'matricula' => $this->pick($vinculo, ['matricula'], null),
            'numeroInscricaoEmpregador' => $this->pick($vinculo, ['numeroInscricaoEmpregador'], null),
            'elegivel' => $this->pick($vinculo, ['elegivel'], null),
            'valorMargemDisponivel' => $this->pick($margem, ['valorMargemDisponivel'], null),
            'valorMargemBase' => $this->pick($margem, ['valorMargemBase'], null),
            'valorTotalDevido' => $this->pick($margem, ['valorTotalDevido'], null),
            'dataAdmissao' => $this->pick($margem, ['dataAdmissao'], null),
            'dataNascimento' => $this->pick($margem, ['dataNascimento'], null),
            'nomeMae' => $this->pick($margem, ['nomeMae'], null),
            'sexo' => $this->pick($margem, ['sexo'], null),
            'tipoConsulta' => $this->pick($pendingRow, ['tipoConsulta'], 'individual'),
            'status' => 'Concluido',
            'mensagem' => 'Fluxo completo OK',
            'autorizacaoId' => $this->pick($termo, ['autorizacaoId'], null),
            'vinculo' => $vinculo,
            'margem_data' => $margem,
        ];

        if (empty($disponiveis)) {
            $row = $baseRow;
            $row['mensagem'] = 'Fluxo completo OK (sem tabelas disponiveis)';
            return [$row];
        }

        $rows = [];
        foreach ($disponiveis as $tabela) {
            $row = $baseRow;
            $row['nomeTipo'] = $this->pick($tabela, ['nome'], null);
            $row['prazo'] = $this->pick($tabela, ['prazo'], null);
            $row['taxaJuros'] = $this->pick($tabela, ['taxaJuros'], null);
            $row['valorLiberado'] = $this->pick($tabela, ['valorLiberado'], null);
            $row['valorParcela'] = $this->pick($tabela, ['valorParcela'], null);
            $row['taxaSeguro'] = $this->pick($tabela, ['taxaSeguro'], null);
            $row['valorSeguro'] = $this->pick($tabela, ['valorSeguro'], null);
            $rows[] = $row;
        }

        return $rows;
    }

    private function formatTelefoneParaPayload(string $telefone): array
    {
        $digits = $this->normalizeDigits($telefone);
        if ($digits === '') {
            return ['11', '999999999'];
        }

        if (strlen($digits) < 10) {
            $digits = str_pad($digits, 10, '0', STR_PAD_LEFT);
        }
        if (strlen($digits) > 11) {
            $digits = substr($digits, -11);
        }

        $ddd = substr($digits, 0, 2);
        $numero = substr($digits, 2);

        return [$ddd !== '' ? $ddd : '11', $numero !== '' ? $numero : '999999999'];
    }

    private function toFloat($value): float
    {
        if ($value === null || $value === '') {
            return 0.0;
        }

        if (is_numeric($value)) {
            return (float) $value;
        }

        $txt = str_replace(['R$', ' '], '', (string) $value);
        if (str_contains($txt, ',') && str_contains($txt, '.')) {
            if (strrpos($txt, ',') > strrpos($txt, '.')) {
                $txt = str_replace('.', '', $txt);
                $txt = str_replace(',', '.', $txt);
            } else {
                $txt = str_replace(',', '', $txt);
            }
        } elseif (str_contains($txt, ',')) {
            $txt = str_replace('.', '', $txt);
            $txt = str_replace(',', '.', $txt);
        }

        return is_numeric($txt) ? (float) $txt : 0.0;
    }

    private function sleepSeconds(int $seconds): void
    {
        if ($seconds <= 0) {
            return;
        }

        usleep($seconds * 1000000);
    }

    private function upsertPendingResultRows(object $pendingRow, array $doneRows): void
    {
        $pendingId = (int) ($pendingRow->id ?? 0);
        if ($pendingId <= 0) {
            return;
        }

        $payloads = [];
        foreach ($doneRows as $row) {
            $payloads[] = $this->buildPersistPayload($row, $pendingRow);
        }

        if (empty($payloads)) {
            return;
        }

        $this->updateConsultaPresencaById($pendingId, $payloads[0]);

        for ($i = 1; $i < count($payloads); $i++) {
            $this->upsertAdditionalRow($payloads[$i], $pendingId);
        }
    }

    private function updateConsultaPresencaById(int $id, array $p): void
    {
        $sql = <<<'SQL'
UPDATE [consultas_presenca].[dbo].[consulta_presenca]
SET
    [cpf] = ?,
    [nome] = ?,
    [telefone] = ?,
    [updated_at] = SYSDATETIME(),
    [matricula] = ?,
    [numeroInscricaoEmpregador] = ?,
    [elegivel] = ?,
    [valorMargemDisponivel] = ?,
    [valorMargemBase] = ?,
    [valorTotalDevido] = ?,
    [dataAdmissao] = ?,
    [dataNascimento] = ?,
    [nomeMae] = ?,
    [sexo] = ?,
    [nomeTipo] = ?,
    [prazo] = ?,
    [taxaJuros] = ?,
    [valorLiberado] = ?,
    [valorParcela] = ?,
    [taxaSeguro] = ?,
    [valorSeguro] = ?,
    [tipoConsulta] = ?,
    [status] = ?,
    [mensagem] = ?,
    [id_user] = ?,
    [equipe_id] = ?,
    [id_role] = ?,
    [id_consulta_presenca] = ?
WHERE [id] = ?
SQL;

        DB::connection(self::DB_CONNECTION)->update($sql, [
            $p['cpf'],
            $p['nome'],
            $p['telefone'],
            $p['matricula'],
            $p['numeroInscricaoEmpregador'],
            $p['elegivel'],
            $p['valorMargemDisponivel'],
            $p['valorMargemBase'],
            $p['valorTotalDevido'],
            $p['dataAdmissao'],
            $p['dataNascimento'],
            $p['nomeMae'],
            $p['sexo'],
            $p['nomeTipo'],
            $p['prazo'],
            $p['taxaJuros'],
            $p['valorLiberado'],
            $p['valorParcela'],
            $p['taxaSeguro'],
            $p['valorSeguro'],
            $p['tipoConsulta'],
            $p['status'],
            $p['mensagem'],
            $p['id_user'],
            $p['equipe_id'],
            $p['id_role'],
            $p['id_consulta_presenca'],
            $id,
        ]);
    }

    private function upsertAdditionalRow(array $p, int $excludeId): void
    {
        $findSql = <<<'SQL'
SELECT TOP (1) [id]
FROM [consultas_presenca].[dbo].[consulta_presenca]
WHERE [id] <> ?
  AND [id_consulta_presenca] = ?
  AND [id_user] = ?
  AND [equipe_id] = ?
  AND [cpf] = ?
  AND ISNULL([matricula], '') = ISNULL(?, '')
  AND ISNULL([numeroInscricaoEmpregador], '') = ISNULL(?, '')
  AND ISNULL([nomeTipo], '') = ISNULL(?, '')
  AND ISNULL([prazo], '') = ISNULL(?, '')
  AND ISNULL([valorLiberado], '') = ISNULL(?, '')
ORDER BY [id] DESC
SQL;

        $existing = DB::connection(self::DB_CONNECTION)->selectOne($findSql, [
            $excludeId,
            $p['id_consulta_presenca'],
            $p['id_user'],
            $p['equipe_id'],
            $p['cpf'],
            $p['matricula'],
            $p['numeroInscricaoEmpregador'],
            $p['nomeTipo'],
            $p['prazo'],
            $p['valorLiberado'],
        ]);

        $existingId = (int) ($existing->id ?? 0);
        if ($existingId > 0) {
            $this->updateConsultaPresencaById($existingId, $p);
            return;
        }

        $insertSql = <<<'SQL'
INSERT INTO [consultas_presenca].[dbo].[consulta_presenca] (
    [cpf], [nome], [telefone], [created_at], [updated_at],
    [matricula], [numeroInscricaoEmpregador], [elegivel],
    [valorMargemDisponivel], [valorMargemBase], [valorTotalDevido],
    [dataAdmissao], [dataNascimento], [nomeMae], [sexo],
    [nomeTipo], [prazo], [taxaJuros], [valorLiberado], [valorParcela], [taxaSeguro], [valorSeguro],
    [tipoConsulta], [status], [mensagem], [id_user], [equipe_id], [id_role], [id_consulta_presenca]
)
VALUES (
    ?, ?, ?, COALESCE(?, SYSDATETIME()), SYSDATETIME(),
    ?, ?, ?,
    ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?
)
SQL;

        DB::connection(self::DB_CONNECTION)->insert($insertSql, [
            $p['cpf'],
            $p['nome'],
            $p['telefone'],
            $p['created_at'],
            $p['matricula'],
            $p['numeroInscricaoEmpregador'],
            $p['elegivel'],
            $p['valorMargemDisponivel'],
            $p['valorMargemBase'],
            $p['valorTotalDevido'],
            $p['dataAdmissao'],
            $p['dataNascimento'],
            $p['nomeMae'],
            $p['sexo'],
            $p['nomeTipo'],
            $p['prazo'],
            $p['taxaJuros'],
            $p['valorLiberado'],
            $p['valorParcela'],
            $p['taxaSeguro'],
            $p['valorSeguro'],
            $p['tipoConsulta'],
            $p['status'],
            $p['mensagem'],
            $p['id_user'],
            $p['equipe_id'],
            $p['id_role'],
            $p['id_consulta_presenca'],
        ]);
    }

    private function buildPersistPayload(array $row, object $pendingRow): array
    {
        $vinculo = $this->pick($row, ['vinculo'], []);
        $margem = $this->pick($row, ['margem_data', 'margemData'], []);

        $cpfDigits = $this->normalizeDigits($this->pick($row, ['cpf'], $pendingRow->cpf ?? ''));
        $telDigits = $this->normalizeDigits($this->pick($row, ['telefone'], $pendingRow->telefone ?? ''));

        $status = trim((string) $this->pick($row, ['status', 'final_status', 'situacao', 'status_presenca'], ''));
        if ($status === '') {
            $status = 'Concluido';
        }

        $mensagem = trim((string) $this->pick($row, ['mensagem', 'Mensagem', 'final_message', 'finalMessage', 'message'], ''));
        if ($mensagem === '') {
            $mensagem = trim((string) ($pendingRow->mensagem ?? 'Fluxo completo OK'));
        }

        $tipoConsulta = trim((string) $this->pick($row, ['tipoConsulta', 'tipo_consulta', 'tipo'], $pendingRow->tipoConsulta ?? 'individual'));
        if ($tipoConsulta === '') {
            $tipoConsulta = 'individual';
        }

        return [
            'cpf' => $this->toNullableBigInt($cpfDigits),
            'nome' => $this->toNullableString($this->pick($row, ['nome'], $pendingRow->nome ?? ''), 100),
            'telefone' => $this->toNullableBigInt($telDigits),
            'created_at' => $this->toNullableDateTime($pendingRow->created_at ?? null),
            'matricula' => $this->toNullableString($this->pick($row, ['matricula'], $this->pick($vinculo, ['matricula'], null)), 255),
            'numeroInscricaoEmpregador' => $this->toNullableString($this->pick($row, ['numeroInscricaoEmpregador'], $this->pick($vinculo, ['numeroInscricaoEmpregador'], null)), 50),
            'elegivel' => $this->toNullableString($this->normalizeBooleanToken($this->pick($row, ['elegivel'], $this->pick($vinculo, ['elegivel'], null))), 10),
            'valorMargemDisponivel' => $this->toNullableString($this->pick($row, ['valorMargemDisponivel'], $this->pick($margem, ['valorMargemDisponivel'], null)), 20),
            'valorMargemBase' => $this->toNullableString($this->pick($row, ['valorMargemBase'], $this->pick($margem, ['valorMargemBase'], null)), 20),
            'valorTotalDevido' => $this->toNullableString($this->pick($row, ['valorTotalDevido'], $this->pick($margem, ['valorTotalDevido'], null)), 20),
            'dataAdmissao' => $this->toNullableDateTime($this->pick($row, ['dataAdmissao'], $this->pick($margem, ['dataAdmissao'], null))),
            'dataNascimento' => $this->toNullableDateTime($this->pick($row, ['dataNascimento'], $this->pick($margem, ['dataNascimento'], null))),
            'nomeMae' => $this->toNullableString($this->pick($row, ['nomeMae'], $this->pick($margem, ['nomeMae'], null)), 100),
            'sexo' => $this->toNullableString($this->pick($row, ['sexo'], $this->pick($margem, ['sexo'], null)), 10),
            'nomeTipo' => $this->toNullableString($this->pick($row, ['nomeTipo', 'nome_tabela', 'nomeTipoCredito', 'nome'], null), 100),
            'prazo' => $this->toNullableString($this->pick($row, ['prazo'], null), 10),
            'taxaJuros' => $this->toNullableString($this->pick($row, ['taxaJuros', 'taxa_juros'], null), 10),
            'valorLiberado' => $this->toNullableString($this->pick($row, ['valorLiberado', 'valor_liberado', 'valor'], null), 10),
            'valorParcela' => $this->toNullableString($this->pick($row, ['valorParcela', 'valor_parcela'], null), 10),
            'taxaSeguro' => $this->toNullableString($this->pick($row, ['taxaSeguro', 'taxa_seguro'], null), 10),
            'valorSeguro' => $this->toNullableString($this->pick($row, ['valorSeguro', 'valor_seguro'], null), 10),
            'tipoConsulta' => $this->toNullableString($tipoConsulta, 100),
            'status' => $this->toNullableString($status, 20),
            'mensagem' => $this->toNullableString($mensagem, null),
            'id_user' => $this->toNullableBigInt($pendingRow->id_user ?? null),
            'equipe_id' => $this->toNullableBigInt($pendingRow->equipe_id ?? null),
            'id_role' => $this->toNullableBigInt($pendingRow->id_role ?? null),
            'id_consulta_presenca' => $this->toNullableBigInt($pendingRow->id_consulta_presenca ?? null),
        ];
    }

    private function filterRowsForPending(array $rows, object $pendingRow, array $account, array $input): array
    {
        $expectedCpf = $this->normalizeDigits($input['cpf'] ?? '');
        $expectedTel = $this->normalizeDigits($input['telefone'] ?? '');
        $expectedNome = mb_strtolower(trim((string) ($input['nome'] ?? '')));
        $expectedLogin = mb_strtolower(trim((string) ($account['login'] ?? '')));
        $expectedUser = (string) ((int) ($pendingRow->id_user ?? 0));

        return array_values(array_filter($rows, function ($row) use ($expectedCpf, $expectedTel, $expectedNome, $expectedLogin, $expectedUser): bool {
            $rowCpf = $this->normalizeDigits($this->pick($row, ['cpf'], ''));
            $rowTel = $this->normalizeDigits($this->pick($row, ['telefone'], ''));
            $rowNome = mb_strtolower(trim((string) $this->pick($row, ['nome'], '')));
            $rowLogin = mb_strtolower(trim((string) $this->pick($row, ['loginP', 'login'], '')));
            $rowUser = trim((string) $this->pick($row, ['id_user', 'idUser', 'user_id'], ''));

            $byCpf = $expectedCpf === '' || $rowCpf === '' || $rowCpf === $expectedCpf;
            $byTel = $expectedTel === '' || $rowTel === '' || $rowTel === $expectedTel;
            $byNome = $expectedNome === '' || $rowNome === '' || $rowNome === $expectedNome;
            $byLogin = $expectedLogin === '' || $rowLogin === '' || $rowLogin === $expectedLogin;
            $byUser = $expectedUser === '0' || $rowUser === '' || $rowUser === $expectedUser;

            return $byCpf && $byTel && $byNome && $byLogin && $byUser;
        }));
    }

    private function dedupeRows(array $rows): array
    {
        $seen = [];
        $out = [];

        foreach ($rows as $row) {
            $fingerprint = implode('|', [
                $this->normalizeDigits($this->pick($row, ['cpf'], '')),
                $this->normalizeDigits($this->pick($row, ['telefone'], '')),
                mb_strtolower(trim((string) $this->pick($row, ['nome'], ''))),
                trim((string) $this->pick($row, ['matricula'], '')),
                trim((string) $this->pick($row, ['numeroInscricaoEmpregador'], '')),
                trim((string) $this->pick($row, ['nomeTipo', 'nome_tabela', 'nome'], '')),
                trim((string) $this->pick($row, ['prazo'], '')),
                trim((string) $this->pick($row, ['valorLiberado', 'valor_liberado', 'valor'], '')),
            ]);

            if (isset($seen[$fingerprint])) {
                continue;
            }

            $seen[$fingerprint] = true;
            $out[] = $row;
        }

        return $out;
    }

    private function incrementConsultedCounter(int $accountId): void
    {
        $sql = <<<'SQL'
UPDATE [consultas_presenca].[dbo].[limites_presenca]
SET [consultados] = CASE
        WHEN COALESCE([consultados], 0) < COALESCE([total], 0)
            THEN COALESCE([consultados], 0) + 1
        ELSE COALESCE([consultados], 0)
    END,
    [updated_at] = SYSDATETIME()
WHERE [id] = ?
SQL;

        DB::connection(self::DB_CONNECTION)->update($sql, [$accountId]);
    }

    private function markPendingAsProcessing(int $pendingId): void
    {
        $sql = <<<'SQL'
UPDATE [consultas_presenca].[dbo].[consulta_presenca]
SET [status] = 'Processando',
    [mensagem] = NULL,
    [updated_at] = SYSDATETIME()
WHERE [id] = ?
SQL;

        DB::connection(self::DB_CONNECTION)->update($sql, [$pendingId]);
    }

    private function markPendingAsError(int $pendingId, string $message): void
    {
        $normalizedMessage = $this->normalizeErrorMessageForStorage($message);

        $sql = <<<'SQL'
UPDATE [consultas_presenca].[dbo].[consulta_presenca]
SET [status] = 'Erro',
    [mensagem] = ?,
    [updated_at] = SYSDATETIME()
WHERE [id] = ?
SQL;

        DB::connection(self::DB_CONNECTION)->update($sql, [$this->truncate($normalizedMessage, 3900), $pendingId]);
    }

    private function normalizeErrorMessageForStorage(string $message): string
    {
        $message = trim($message);
        if ($message === '') {
            return $message;
        }

        $candidates = [$message];
        $jsonStartPos = strpos($message, '{');
        if ($jsonStartPos !== false) {
            $jsonSlice = trim(substr($message, $jsonStartPos));
            if ($jsonSlice !== '' && $jsonSlice !== $message) {
                $candidates[] = $jsonSlice;
            }
        }

        foreach ($candidates as $candidate) {
            $decoded = $this->decodeJson($candidate);
            if (! is_array($decoded) || $decoded === []) {
                continue;
            }

            $errors = $this->extractNestedErrorMessages($decoded);
            if (! empty($errors)) {
                return implode(' | ', array_values(array_unique($errors)));
            }
        }

        return $message;
    }

    private function extractNestedErrorMessages($payload): array
    {
        $out = [];
        $stack = [$payload];

        while (! empty($stack)) {
            $current = array_shift($stack);

            if (is_string($current)) {
                $decoded = $this->decodeJson($current);
                if (is_array($decoded) && ! empty($decoded)) {
                    $stack[] = $decoded;
                }
                continue;
            }

            if (! is_array($current)) {
                continue;
            }

            foreach (['Erros', 'erros', 'errors'] as $key) {
                if (! array_key_exists($key, $current)) {
                    continue;
                }

                $value = $current[$key];
                if (is_array($value)) {
                    foreach ($value as $item) {
                        if (is_scalar($item)) {
                            $text = trim((string) $item);
                            if ($text !== '') {
                                $out[] = $text;
                            }
                        } elseif (is_array($item)) {
                            $stack[] = $item;
                        }
                    }
                } elseif (is_scalar($value)) {
                    $text = trim((string) $value);
                    if ($text !== '') {
                        $out[] = $text;
                    }
                }
            }

            foreach ($current as $value) {
                if (is_array($value) || is_string($value)) {
                    $stack[] = $value;
                }
            }
        }

        return $out;
    }

    private function buildInputFromPendingRow(object $pendingRow): array
    {
        $cpf = $this->normalizeDigits((string) ($pendingRow->cpf ?? ''));
        if ($cpf !== '' && strlen($cpf) < 11) {
            // CPF salvo em coluna numerica perde zero(s) a esquerda; recomp?e antes da consulta.
            $cpf = str_pad($cpf, 11, '0', STR_PAD_LEFT);
        }
        $nome = trim((string) ($pendingRow->nome ?? ''));
        $telefone = $this->normalizeDigits((string) ($pendingRow->telefone ?? ''));

        if ($cpf === '' || strlen($cpf) !== 11) {
            throw new \RuntimeException('CPF invalido na linha pendente ID '.$pendingRow->id);
        }

        if ($nome === '') {
            throw new \RuntimeException('Nome vazio na linha pendente ID '.$pendingRow->id);
        }

        if ($telefone === '') {
            $telefone = '119'.str_pad((string) random_int(0, 99999999), 8, '0', STR_PAD_LEFT);
        }

        if (strlen($telefone) < 10 || strlen($telefone) > 11) {
            throw new \RuntimeException('Telefone invalido na linha pendente ID '.$pendingRow->id);
        }

        return [
            'cpf' => $cpf,
            'nome' => $nome,
            'telefone' => $telefone,
        ];
    }

    private function decodeJson(string $raw)
    {
        $raw = trim($raw);
        if ($raw === '') {
            return [];
        }

        $decoded = json_decode($raw, true);
        if (json_last_error() === JSON_ERROR_NONE) {
            return $decoded;
        }

        return [];
    }

    private function normalizeRows($payload): array
    {
        if (is_array($payload)) {
            if ($this->isAssoc($payload) && isset($payload['data']) && is_array($payload['data'])) {
                return $payload['data'];
            }

            if ($this->isAssoc($payload) && isset($payload['rows']) && is_array($payload['rows'])) {
                return $payload['rows'];
            }

            if ($this->isAssoc($payload)) {
                return [$payload];
            }

            return $payload;
        }

        return [];
    }

    private function isAssoc(array $array): bool
    {
        if ($array === []) {
            return false;
        }

        return array_keys($array) !== range(0, count($array) - 1);
    }

    private function pick($data, array $keys, $fallback = null)
    {
        foreach ($keys as $key) {
            if (is_array($data) && array_key_exists($key, $data)) {
                $value = $data[$key];
                if ($value !== null && $value !== '') {
                    return $value;
                }
            }

            if (is_object($data) && isset($data->{$key})) {
                $value = $data->{$key};
                if ($value !== null && $value !== '') {
                    return $value;
                }
            }
        }

        return $fallback;
    }

    private function normalizeDigits($value): string
    {
        return preg_replace('/\D+/', '', (string) ($value ?? '')) ?? '';
    }

    private function isDoneStatus($status): bool
    {
        $token = mb_strtolower(trim((string) $status));
        return $token === 'concluido' || $token === 'concluído';
    }

    private function toNullableString($value, ?int $maxLen = null): ?string
    {
        if ($value === null) {
            return null;
        }

        $txt = trim((string) $value);
        if ($txt === '') {
            return null;
        }

        if ($maxLen !== null && $maxLen > 0) {
            $txt = mb_substr($txt, 0, $maxLen);
        }

        return $txt;
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

    private function toNullableBigInt($value): ?int
    {
        if ($value === null || $value === '') {
            return null;
        }

        $digits = $this->normalizeDigits($value);
        if ($digits === '') {
            return null;
        }

        return (int) $digits;
    }

    private function toNullableDateTime($value): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }

        try {
            return Carbon::parse($value)->format('Y-m-d H:i:s');
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function toNullableDate($value): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }

        try {
            return Carbon::parse($value)->format('Y-m-d');
        } catch (\Throwable $e) {
            return null;
        }
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

    private function normalizeBooleanToken($value): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        $token = mb_strtolower(trim((string) $value));
        if (in_array($token, ['1', 'true', 'sim', 'yes'], true)) {
            return 'true';
        }

        if (in_array($token, ['0', 'false', 'nao', 'não', 'no'], true)) {
            return 'false';
        }

        return $token;
    }

    private function normalizeText(string $value): string
    {
        $txt = trim($value);
        if ($txt === '') {
            return '';
        }

        $decoded = html_entity_decode($txt, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $clean = strip_tags($decoded);
        $normalized = iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $clean);
        if ($normalized === false) {
            return $clean;
        }

        return $normalized;
    }

    private function truncate(?string $value, int $max): string
    {
        $txt = trim((string) ($value ?? ''));
        if ($txt === '') {
            return '';
        }

        if (mb_strlen($txt) <= $max) {
            return $txt;
        }

        return mb_substr($txt, 0, max(0, $max - 3)).'...';
    }
}
