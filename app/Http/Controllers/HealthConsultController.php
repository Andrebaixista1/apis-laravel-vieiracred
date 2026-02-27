<?php

namespace App\Http\Controllers;

use Carbon\Carbon;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class HealthConsultController extends Controller
{
    public function index(): JsonResponse
    {
        $results = [];

        foreach ($this->servers() as $server) {
            $results[] = $this->buildServerHealth($server['name_database'], $server['connection']);
        }

        return response()->json([
            'generated_at' => now()->format('Y-m-d H:i:s'),
            'servers' => $results,
        ]);
    }

    public function forceBackup(Request $request): JsonResponse
    {
        $validated = $request->validate([
            'name_database' => 'required|string',
            'type' => 'required|string|in:daily,weekly,monthly,DAILY,WEEKLY,MONTHLY',
            'pending' => 'nullable',
            'database' => 'nullable|string',
        ]);

        $server = $this->findServerByName($validated['name_database']);
        if ($server === null) {
            return response()->json([
                'ok' => false,
                'message' => 'name_database invalido. Use Local, Hostinger ou Kinghost.',
            ], 422);
        }

        $tier = strtoupper($validated['type']);
        $connection = $server['connection'];

        $requested = $this->normalizeRequestedDatabases(
            $request->input('pending'),
            $request->input('database')
        );

        $onlineRows = DB::connection($connection)->select(
            "SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE'"
        );

        $onlineByLower = [];
        foreach ($onlineRows as $row) {
            $dbName = (string) $row->name;
            $onlineByLower[mb_strtolower($dbName)] = $dbName;
        }

        if (empty($requested)) {
            $targetDatabases = array_values($onlineByLower);
        } else {
            $targetDatabases = [];
            foreach ($requested as $db) {
                $key = mb_strtolower($db);
                if (isset($onlineByLower[$key])) {
                    $targetDatabases[] = $onlineByLower[$key];
                }
            }
            $targetDatabases = array_values(array_unique($targetDatabases));
        }

        $missing = [];
        if (! empty($requested)) {
            foreach ($requested as $db) {
                if (! isset($onlineByLower[mb_strtolower($db)])) {
                    $missing[] = $db;
                }
            }
        }

        if (empty($targetDatabases)) {
            return response()->json([
                'ok' => false,
                'message' => 'Nenhum banco ONLINE encontrado para executar backup.',
                'name_database' => $server['name_database'],
                'type' => $tier,
                'missing' => $missing,
            ], 422);
        }

        $tierPath = rtrim($server['base_path'], "\\/") . $server['path_separator'] . $tier;

        // Try to ensure backup folders exist.
        $this->createDirIfNeeded($connection, rtrim($server['base_path'], "\\/"));
        $this->createDirIfNeeded($connection, $tierPath);

        $startedAt = now()->format('Y-m-d H:i:s');
        $success = [];
        $errors = [];

        foreach ($targetDatabases as $database) {
            try {
                $before = $this->getLastFullBackupMetadata($connection, $database);

                $stamp = now()->format('Ymd_His');
                $safeDb = preg_replace('/[^A-Za-z0-9_.-]/', '_', $database) ?: 'database';
                $filePath = $tierPath . $server['path_separator'] . $safeDb . '_' . $tier . '_' . $stamp . '.bak';

                $this->executeBackupViaSqlsrv($connection, $database, $filePath);

                $after = $this->getLastFullBackupMetadata($connection, $database);
                if ($after === null) {
                    throw new \RuntimeException('Backup concluido sem registro no msdb.');
                }

                $sameAsBefore = $before !== null
                    && ($before['backup_finish'] ?? null) === ($after['backup_finish'] ?? null)
                    && ($before['file_path'] ?? null) === ($after['file_path'] ?? null);

                if ($sameAsBefore) {
                    throw new \RuntimeException('Backup nao gerou novo registro no historico (msdb).');
                }

                $move = $this->moveBackupToWindowsIfNeeded(
                    $server['name_database'],
                    $after['file_path'] ?? $filePath,
                    $tier
                );

                if (($move['ok'] ?? false) !== true) {
                    throw new \RuntimeException('Backup criado, mas falha ao mover para Windows: '.($move['message'] ?? 'erro desconhecido'));
                }

                $success[] = [
                    'database' => $database,
                    'datetime' => $after['backup_finish'] ?? now()->format('Y-m-d H:i:s'),
                    'type' => strtolower($tier),
                    'file' => $move['destination_file'] ?? ($after['file_path'] ?? $filePath),
                    'moved_to_windows' => (bool) ($move['moved'] ?? false),
                ];
            } catch (\Throwable $e) {
                $errors[] = [
                    'database' => $database,
                    'datetime' => now()->format('Y-m-d H:i:s'),
                    'type' => strtolower($tier),
                    'message' => Str::limit($e->getMessage(), 280),
                ];
            }
        }

        return response()->json([
            'ok' => count($errors) === 0,
            'message' => 'Execucao de backup concluida.',
            'name_database' => $server['name_database'],
            'type' => strtolower($tier),
            'started_at' => $startedAt,
            'finished_at' => now()->format('Y-m-d H:i:s'),
            'requested' => $requested,
            'processed_databases' => $targetDatabases,
            'missing' => $missing,
            'summary' => [
                'requested_count' => count($requested),
                'processed_count' => count($targetDatabases),
                'success_count' => count($success),
                'error_count' => count($errors),
            ],
            'success' => $success,
            'errors' => $errors,
        ]);
    }

    private function buildServerHealth(string $nameDatabase, string $connection): array
    {
        try {
            $serverNowRow = DB::connection($connection)->selectOne("SELECT GETDATE() AS server_now");
            $serverNow = Carbon::parse($serverNowRow->server_now);

            $onlineDbRows = DB::connection($connection)->select(
                "SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE'"
            );

            $onlineDatabases = collect($onlineDbRows)
                ->pluck('name')
                ->values()
                ->all();

            $lastFullRows = DB::connection($connection)->select(
                "
                SELECT
                    database_name,
                    MAX(backup_finish_date) AS last_backup
                FROM msdb.dbo.backupset
                WHERE type = 'D'
                GROUP BY database_name
                "
            );

            $lastBackupByDatabase = [];
            foreach ($lastFullRows as $row) {
                if (! empty($row->database_name) && ! empty($row->last_backup)) {
                    $lastBackupByDatabase[$row->database_name] = Carbon::parse($row->last_backup);
                }
            }

            $dailyMetrics = $this->getTierMetrics($connection, 'DAILY');
            $weeklyMetrics = $this->getTierMetrics($connection, 'WEEKLY');
            $monthlyMetrics = $this->getTierMetrics($connection, 'MONTHLY');

            $runningRow = DB::connection($connection)->selectOne(
                "
                SELECT COUNT(*) AS running_count
                FROM sys.dm_exec_requests
                WHERE command LIKE 'BACKUP%'
                   OR command LIKE 'RESTORE%'
                "
            );

            $backedUpDatabases = $dailyMetrics['databases'];

            $latestBackup = null;
            $pending = [];
            $errors = [];
            $databasesLastBackup = [];

            foreach ($onlineDatabases as $databaseName) {
                $lastBackup = $lastBackupByDatabase[$databaseName] ?? null;

                $databasesLastBackup[] = [
                    'database' => $databaseName,
                    'last_backup' => $lastBackup?->format('Y-m-d H:i:s'),
                ];

                if ($lastBackup === null) {
                    $pending[] = $databaseName;
                    $errors[] = [
                        'database' => $databaseName,
                        'datetime' => null,
                        'type' => 'month',
                    ];
                    continue;
                }

                if ($latestBackup === null || $lastBackup->greaterThan($latestBackup)) {
                    $latestBackup = $lastBackup;
                }

                if ($lastBackup->lt($serverNow->copy()->subDays(30))) {
                    $errors[] = [
                        'database' => $databaseName,
                        'datetime' => $lastBackup->format('Y-m-d H:i:s'),
                        'type' => 'month',
                    ];
                    $pending[] = $databaseName;
                    continue;
                }

                if ($lastBackup->lt($serverNow->copy()->subDays(7))) {
                    $errors[] = [
                        'database' => $databaseName,
                        'datetime' => $lastBackup->format('Y-m-d H:i:s'),
                        'type' => 'week',
                    ];
                    $pending[] = $databaseName;
                    continue;
                }

                if ($lastBackup->toDateString() !== $serverNow->toDateString()) {
                    $errors[] = [
                        'database' => $databaseName,
                        'datetime' => $lastBackup->format('Y-m-d H:i:s'),
                        'type' => 'daily',
                    ];
                    $pending[] = $databaseName;
                }
            }

            return [
                'name_database' => $nameDatabase,
                'lastead_backup' => $latestBackup?->format('Y-m-d H:i:s'),
                'latest_backup' => $latestBackup?->format('Y-m-d H:i:s'),
                'quantity_databases' => count($onlineDatabases),
                'backed_up_databases' => $backedUpDatabases,
                'backed_up_databases_by_type' => [
                    'daily' => $dailyMetrics['databases'],
                    'weekly' => $weeklyMetrics['databases'],
                    'monthly' => $monthlyMetrics['databases'],
                ],
                'databases_last_backup' => $databasesLastBackup,
                'pending' => $pending,
                'daily' => [
                    'timer_hours' => $dailyMetrics['timer_hours'],
                    'quantity' => $dailyMetrics['quantity'],
                    'first_start' => $dailyMetrics['first_start'],
                    'last_finish' => $dailyMetrics['last_finish'],
                ],
                'weekly' => [
                    'timer_hours' => $weeklyMetrics['timer_hours'],
                    'quantity' => $weeklyMetrics['quantity'],
                    'first_start' => $weeklyMetrics['first_start'],
                    'last_finish' => $weeklyMetrics['last_finish'],
                ],
                'monthly' => [
                    'timer_hours' => $monthlyMetrics['timer_hours'],
                    'quantity' => $monthlyMetrics['quantity'],
                    'first_start' => $monthlyMetrics['first_start'],
                    'last_finish' => $monthlyMetrics['last_finish'],
                ],
                'errors' => $errors,
                'running_backup_count' => (int) ($runningRow->running_count ?? 0),
                'collected_at' => $serverNow->format('Y-m-d H:i:s'),
            ];
        } catch (\Throwable $e) {
            return [
                'name_database' => $nameDatabase,
                'lastead_backup' => null,
                'latest_backup' => null,
                'quantity_databases' => 0,
                'backed_up_databases' => [],
                'backed_up_databases_by_type' => [
                    'daily' => [],
                    'weekly' => [],
                    'monthly' => [],
                ],
                'databases_last_backup' => [],
                'pending' => [],
                'daily' => [
                    'timer_hours' => null,
                    'quantity' => 0,
                    'first_start' => null,
                    'last_finish' => null,
                ],
                'weekly' => [
                    'timer_hours' => null,
                    'quantity' => 0,
                    'first_start' => null,
                    'last_finish' => null,
                ],
                'monthly' => [
                    'timer_hours' => null,
                    'quantity' => 0,
                    'first_start' => null,
                    'last_finish' => null,
                ],
                'errors' => [
                    [
                        'database' => null,
                        'datetime' => now()->format('Y-m-d H:i:s'),
                        'type' => 'connection',
                        'message' => Str::limit($e->getMessage(), 250),
                    ],
                ],
                'running_backup_count' => null,
                'collected_at' => now()->format('Y-m-d H:i:s'),
            ];
        }
    }

    private function getTierMetrics(string $connection, string $tier): array
    {
        $tier = strtoupper(trim($tier));
        $windowStartSql = match ($tier) {
            'DAILY' => 'CONVERT(date, GETDATE())',
            'WEEKLY' => 'DATEADD(day, -7, GETDATE())',
            'MONTHLY' => 'DATEADD(day, -30, GETDATE())',
            default => 'CONVERT(date, GETDATE())',
        };

        $tierLikeBackslash = "%\\{$tier}\\%";
        $tierLikeSlash = "%/{$tier}/%";
        $tierLikeUnderscore = "%_{$tier}_%";

        $summaryRow = DB::connection($connection)->selectOne(
            "
            SELECT
                MAX(bs.backup_start_date) AS first_start,
                MAX(bs.backup_finish_date) AS last_finish,
                COUNT(*) AS quantity
            FROM msdb.dbo.backupset bs
            JOIN msdb.dbo.backupmediafamily bmf ON bmf.media_set_id = bs.media_set_id
            WHERE bs.type = 'D'
              AND bs.backup_start_date >= {$windowStartSql}
              AND (
                    UPPER(bmf.physical_device_name) LIKE ?
                 OR UPPER(bmf.physical_device_name) LIKE ?
                 OR UPPER(bmf.physical_device_name) LIKE ?
              )
            ",
            [$tierLikeBackslash, $tierLikeSlash, $tierLikeUnderscore]
        );

        $databaseRows = DB::connection($connection)->select(
            "
            SELECT DISTINCT bs.database_name
            FROM msdb.dbo.backupset bs
            JOIN msdb.dbo.backupmediafamily bmf ON bmf.media_set_id = bs.media_set_id
            WHERE bs.type = 'D'
              AND bs.backup_start_date >= {$windowStartSql}
              AND (
                    UPPER(bmf.physical_device_name) LIKE ?
                 OR UPPER(bmf.physical_device_name) LIKE ?
                 OR UPPER(bmf.physical_device_name) LIKE ?
              )
            ORDER BY bs.database_name
            ",
            [$tierLikeBackslash, $tierLikeSlash, $tierLikeUnderscore]
        );

        $firstStart = ! empty($summaryRow->first_start) ? Carbon::parse($summaryRow->first_start) : null;
        $lastFinish = ! empty($summaryRow->last_finish) ? Carbon::parse($summaryRow->last_finish) : null;
        $timerHours = null;

        if ($firstStart !== null && $lastFinish !== null && $lastFinish->greaterThanOrEqualTo($firstStart)) {
            $timerHours = round($firstStart->floatDiffInSeconds($lastFinish) / 3600, 2);
        }

        $databases = collect($databaseRows)
            ->pluck('database_name')
            ->filter(fn ($db) => is_string($db) && $db !== '')
            ->values()
            ->all();

        return [
            'timer_hours' => $timerHours,
            'quantity' => (int) ($summaryRow->quantity ?? 0),
            'first_start' => $firstStart?->format('Y-m-d H:i:s'),
            'last_finish' => $lastFinish?->format('Y-m-d H:i:s'),
            'databases' => $databases,
        ];
    }

    private function moveBackupToWindowsIfNeeded(string $nameDatabase, string $sourceFile, string $tier): array
    {
        if (strtoupper($nameDatabase) === 'LOCAL') {
            return [
                'ok' => true,
                'moved' => false,
                'destination_file' => $sourceFile,
                'message' => 'Backup local ja gravado no servidor Windows.',
            ];
        }

        $sourceSsh = $this->getBackupMoveSourceSsh($nameDatabase);
        if ($sourceSsh === null) {
            return [
                'ok' => false,
                'moved' => false,
                'message' => 'Configuracao SSH de origem nao encontrada para migracao.',
            ];
        }

        $windowsHost = (string) env('BACKUP_MOVE_WINDOWS_HOST', '');
        $windowsUser = (string) env('BACKUP_MOVE_WINDOWS_USER', '');
        $windowsPass = (string) env('BACKUP_MOVE_WINDOWS_PASS', '');

        if ($windowsHost === '' || $windowsUser === '' || $windowsPass === '') {
            return [
                'ok' => false,
                'moved' => false,
                'message' => 'Configuracao de destino Windows ausente (BACKUP_MOVE_WINDOWS_*).',
            ];
        }

        if (! $this->commandExists('sshpass') || ! $this->commandExists('ssh')) {
            return [
                'ok' => false,
                'moved' => false,
                'message' => 'Comandos ssh/sshpass nao disponiveis no container da API.',
            ];
        }

        $remoteMoveCommand = sprintf(
            '/usr/local/bin/move_backup_to_windows.sh %s %s %s %s %s',
            escapeshellarg($sourceFile),
            escapeshellarg($tier),
            escapeshellarg($windowsHost),
            escapeshellarg($windowsUser),
            escapeshellarg($windowsPass)
        );

        $sshTarget = $sourceSsh['user'].'@'.$sourceSsh['host'];
        $command = sprintf(
            'sshpass -p %s ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null %s %s 2>&1',
            escapeshellarg($sourceSsh['pass']),
            escapeshellarg($sshTarget),
            escapeshellarg($remoteMoveCommand)
        );

        $output = [];
        $exitCode = 1;
        exec($command, $output, $exitCode);

        if ($exitCode !== 0) {
            return [
                'ok' => false,
                'moved' => false,
                'message' => Str::limit(trim(implode(' | ', $output)), 250),
            ];
        }

        return [
            'ok' => true,
            'moved' => true,
            'destination_file' => 'E:\\Backups-Database\\'.strtoupper($tier).'\\'.basename($sourceFile),
            'message' => 'Backup movido para o servidor Windows.',
        ];
    }

    private function getBackupMoveSourceSsh(string $nameDatabase): ?array
    {
        $normalized = strtoupper(trim($nameDatabase));

        if ($normalized === 'HOSTINGER') {
            $host = (string) env('BACKUP_MOVE_HOSTINGER_SSH_HOST', '');
            $user = (string) env('BACKUP_MOVE_HOSTINGER_SSH_USER', '');
            $pass = (string) env('BACKUP_MOVE_HOSTINGER_SSH_PASS', '');

            if ($host !== '' && $user !== '' && $pass !== '') {
                return ['host' => $host, 'user' => $user, 'pass' => $pass];
            }

            return null;
        }

        if ($normalized === 'KINGHOST') {
            $host = (string) env('BACKUP_MOVE_KINGHOST_SSH_HOST', '');
            $user = (string) env('BACKUP_MOVE_KINGHOST_SSH_USER', '');
            $pass = (string) env('BACKUP_MOVE_KINGHOST_SSH_PASS', '');

            if ($host !== '' && $user !== '' && $pass !== '') {
                return ['host' => $host, 'user' => $user, 'pass' => $pass];
            }

            return null;
        }

        return null;
    }

    private function commandExists(string $command): bool
    {
        $output = [];
        $exitCode = 1;
        exec('command -v '.escapeshellarg($command).' >/dev/null 2>&1', $output, $exitCode);

        return $exitCode === 0;
    }

    private function getLastFullBackupMetadata(string $connection, string $database): ?array
    {
        $row = DB::connection($connection)->selectOne(
            "
            SELECT TOP 1
                CONVERT(varchar(19), bs.backup_finish_date, 120) AS backup_finish,
                bmf.physical_device_name AS file_path
            FROM msdb.dbo.backupset bs
            JOIN msdb.dbo.backupmediafamily bmf ON bmf.media_set_id = bs.media_set_id
            WHERE bs.database_name = ?
              AND bs.type = 'D'
            ORDER BY bs.backup_finish_date DESC
            ",
            [$database]
        );

        if ($row === null) {
            return null;
        }

        return [
            'backup_finish' => $row->backup_finish,
            'file_path' => $row->file_path,
        ];
    }

    private function executeBackupViaSqlsrv(string $connection, string $database, string $filePath): void
    {
        if (! function_exists('sqlsrv_connect')) {
            throw new \RuntimeException('Extensao sqlsrv nao disponivel no container da API.');
        }

        $cfg = config("database.connections.$connection");
        if (! is_array($cfg)) {
            throw new \RuntimeException("Configuracao da conexao '$connection' nao encontrada.");
        }

        $host = (string) ($cfg['host'] ?? '');
        $port = (string) ($cfg['port'] ?? '1433');
        $username = (string) ($cfg['username'] ?? '');
        $password = (string) ($cfg['password'] ?? '');
        $defaultDb = (string) ($cfg['database'] ?? 'master');

        if ($host === '' || $username === '') {
            throw new \RuntimeException("Conexao '$connection' sem host/username configurado.");
        }

        $serverName = $host . ',' . $port;

        sqlsrv_configure('WarningsReturnAsErrors', 0);

        $connectionInfo = [
            'UID' => $username,
            'PWD' => $password,
            'Database' => $defaultDb !== '' ? $defaultDb : 'master',
            'LoginTimeout' => 30,
            'CharacterSet' => 'UTF-8',
            'ReturnDatesAsStrings' => true,
        ];

        if (array_key_exists('encrypt', $cfg)) {
            $connectionInfo['Encrypt'] = $this->toBool($cfg['encrypt']);
        }

        if (array_key_exists('trust_server_certificate', $cfg)) {
            $connectionInfo['TrustServerCertificate'] = $this->toBool($cfg['trust_server_certificate']);
        }

        $conn = sqlsrv_connect($serverName, $connectionInfo);
        if ($conn === false) {
            throw new \RuntimeException($this->formatSqlsrvErrors());
        }

        $quotedDb = '[' . str_replace(']', ']]', $database) . ']';
        $escapedPath = str_replace("'", "''", $filePath);
        $sql = "BACKUP DATABASE {$quotedDb} TO DISK = N'{$escapedPath}' WITH CHECKSUM, STATS = 10";

        $stmt = sqlsrv_query($conn, $sql);
        if ($stmt === false) {
            $message = $this->formatSqlsrvErrors();
            sqlsrv_close($conn);
            throw new \RuntimeException($message);
        }

        while (sqlsrv_next_result($stmt) !== null) {
            // Consume informational result sets from STATS.
        }

        sqlsrv_free_stmt($stmt);
        sqlsrv_close($conn);
    }

    private function formatSqlsrvErrors(): string
    {
        if (! function_exists('sqlsrv_errors')) {
            return 'Erro SQLSRV sem detalhes.';
        }

        $errors = sqlsrv_errors(SQLSRV_ERR_ALL);
        if (! is_array($errors) || empty($errors)) {
            return 'Erro SQLSRV sem detalhes.';
        }

        $messages = [];
        foreach ($errors as $error) {
            $state = $error['SQLSTATE'] ?? 'N/A';
            $code = $error['code'] ?? 'N/A';
            $message = $error['message'] ?? 'Erro desconhecido';
            $messages[] = "[$state/$code] $message";
        }

        return implode(' | ', $messages);
    }

    private function toBool(mixed $value): bool
    {
        if (is_bool($value)) {
            return $value;
        }

        $normalized = mb_strtolower(trim((string) $value));

        return in_array($normalized, ['1', 'true', 'yes', 'on'], true);
    }

    private function normalizeRequestedDatabases(mixed $pendingInput, ?string $databaseInput): array
    {
        $requested = [];

        if (is_array($pendingInput)) {
            foreach ($pendingInput as $item) {
                if (is_string($item) && trim($item) !== '') {
                    $requested[] = trim($item);
                }
            }
        } elseif (is_string($pendingInput) && trim($pendingInput) !== '') {
            $requested[] = trim($pendingInput);
        }

        if (is_string($databaseInput) && trim($databaseInput) !== '') {
            $requested[] = trim($databaseInput);
        }

        return array_values(array_unique($requested));
    }

    private function createDirIfNeeded(string $connection, string $path): void
    {
        $escapedPath = str_replace("'", "''", $path);

        try {
            DB::connection($connection)->statement("EXEC master.dbo.xp_create_subdir N'{$escapedPath}'");
        } catch (\Throwable) {
            // Ignore if folder already exists or permission differs by host.
        }
    }

    private function findServerByName(string $nameDatabase): ?array
    {
        $search = mb_strtoupper(trim($nameDatabase));

        foreach ($this->servers() as $server) {
            if (mb_strtoupper($server['name_database']) === $search) {
                return $server;
            }
        }

        return null;
    }

    private function servers(): array
    {
        return [
            [
                'name_database' => 'Local',
                'connection' => 'sqlsrv_servidor_planejamento',
                'base_path' => 'E:\\Backups-Database',
                'path_separator' => '\\',
            ],
            [
                'name_database' => 'Hostinger',
                'connection' => 'sqlsrv_hostinger_vps',
                'base_path' => '/mnt/mssql_backups/backups-ubuntu',
                'path_separator' => '/',
            ],
            [
                'name_database' => 'Kinghost',
                'connection' => 'sqlsrv_kinghost_vps',
                'base_path' => '/var/opt/mssql/data/AutoScheduled',
                'path_separator' => '/',
            ],
        ];
    }
}
