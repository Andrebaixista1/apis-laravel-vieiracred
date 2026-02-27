<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class SyncRemoteBackupsCommand extends Command
{
    protected $signature = 'backups:sync-remote
                            {--server=all : all|hostinger|kinghost}
                            {--type=all : all|daily|weekly|monthly}
                            {--min-age=3 : Idade minima do arquivo (minutos)}';

    protected $description = 'Move backups dos servidores Linux para Windows e remove o original apos copia.';

    public function handle()
    {
        if (! $this->commandExists('sshpass') || ! $this->commandExists('ssh')) {
            $this->error('Comandos ssh/sshpass nao disponiveis no container.');

            return self::FAILURE;
        }

        $serverOption = strtolower((string) $this->option('server'));
        $tiers = $this->resolveTiers((string) $this->option('type'));
        $minAge = max(1, (int) $this->option('min-age'));

        if ($tiers === null) {
            $this->error('Opcao --type invalida. Use: all, daily, weekly, monthly.');

            return self::FAILURE;
        }

        $servers = [
            'hostinger' => [
                'label' => 'Hostinger',
                'base_path' => '/mnt/mssql_backups/backups-ubuntu',
                'host_env' => 'BACKUP_MOVE_HOSTINGER_SSH_HOST',
                'user_env' => 'BACKUP_MOVE_HOSTINGER_SSH_USER',
                'pass_env' => 'BACKUP_MOVE_HOSTINGER_SSH_PASS',
            ],
            'kinghost' => [
                'label' => 'Kinghost',
                'base_path' => '/var/opt/mssql/data/AutoScheduled',
                'host_env' => 'BACKUP_MOVE_KINGHOST_SSH_HOST',
                'user_env' => 'BACKUP_MOVE_KINGHOST_SSH_USER',
                'pass_env' => 'BACKUP_MOVE_KINGHOST_SSH_PASS',
            ],
        ];

        if ($serverOption !== 'all' && ! isset($servers[$serverOption])) {
            $this->error('Opcao --server invalida. Use: all, hostinger, kinghost.');

            return self::FAILURE;
        }

        $windowsHost = (string) env('BACKUP_MOVE_WINDOWS_HOST', '');
        $windowsUser = (string) env('BACKUP_MOVE_WINDOWS_USER', '');
        $windowsPass = (string) env('BACKUP_MOVE_WINDOWS_PASS', '');

        if ($windowsHost === '' || $windowsUser === '' || $windowsPass === '') {
            $this->error('Configuracao de destino Windows ausente (BACKUP_MOVE_WINDOWS_*).');

            return self::FAILURE;
        }

        $selectedServers = $serverOption === 'all'
            ? $servers
            : [$serverOption => $servers[$serverOption]];

        $totalMoved = 0;
        $totalErrors = 0;

        foreach ($selectedServers as $serverKey => $server) {
            $sourceHost = (string) env($server['host_env'], '');
            $sourceUser = (string) env($server['user_env'], '');
            $sourcePass = (string) env($server['pass_env'], '');

            if ($sourceHost === '' || $sourceUser === '' || $sourcePass === '') {
                $this->error("{$server['label']}: configuracao SSH ausente.");
                $totalErrors++;
                continue;
            }

            $this->line('');
            $this->info("Sincronizando {$server['label']} ({$serverKey})...");

            $remoteScript = $this->buildRemoteScript(
                $server['base_path'],
                $tiers,
                $minAge,
                $windowsHost,
                $windowsUser,
                $windowsPass
            );

            $remoteCommand = 'bash -lc '.escapeshellarg($remoteScript);
            $sshTarget = $sourceUser.'@'.$sourceHost;

            $command = sprintf(
                'sshpass -p %s ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null %s %s 2>&1',
                escapeshellarg($sourcePass),
                escapeshellarg($sshTarget),
                escapeshellarg($remoteCommand)
            );

            $output = [];
            $exitCode = 1;
            exec($command, $output, $exitCode);

            $serverMoved = 0;
            $serverErrors = 0;

            foreach ($output as $line) {
                $line = trim($line);
                if ($line === '') {
                    continue;
                }

                if (str_starts_with($line, 'MOVED|')) {
                    $serverMoved++;
                    $this->line($line);
                    continue;
                }

                if (str_starts_with($line, 'ERROR|')) {
                    $serverErrors++;
                    $this->error($line);
                    continue;
                }

                if (str_starts_with($line, 'SUMMARY|') || str_starts_with($line, 'SKIP|')) {
                    $this->line($line);
                    continue;
                }

                $this->line($line);
            }

            if ($exitCode !== 0) {
                $serverErrors++;
                $this->error("{$server['label']}: comando retornou codigo {$exitCode}.");
            }

            $this->info("{$server['label']} finalizado: moved={$serverMoved}, errors={$serverErrors}");

            $totalMoved += $serverMoved;
            $totalErrors += $serverErrors;
        }

        $this->line('');
        $this->info("Resumo geral: moved={$totalMoved}, errors={$totalErrors}");

        return $totalErrors > 0 ? self::FAILURE : self::SUCCESS;
    }

    private function buildRemoteScript(
        string $basePath,
        array $tiers,
        int $minAge,
        string $windowsHost,
        string $windowsUser,
        string $windowsPass
    ): string {
        $tiersCsv = implode(',', $tiers);

        return <<<BASH
set -euo pipefail
BASE_PATH={$this->quote($basePath)}
WIN_HOST={$this->quote($windowsHost)}
WIN_USER={$this->quote($windowsUser)}
WIN_PASS={$this->quote($windowsPass)}
MIN_AGE_MINUTES={$this->quote((string) $minAge)}
TIERS_RAW={$this->quote($tiersCsv)}

IFS=',' read -r -a TIERS <<< "\$TIERS_RAW"
checked=0
moved=0
errors=0

for tier_input in "\${TIERS[@]}"; do
  tier="\$(echo "\$tier_input" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]')"

  case "\$tier" in
    DAILY|WEEKLY|MONTHLY) ;;
    *)
      echo "SKIP|\$tier_input|INVALID_TIER"
      continue
      ;;
  esac

  tier_dir="\${BASE_PATH%/}/\$tier"

  if [[ ! -d "\$tier_dir" ]]; then
    echo "SKIP|\$tier|DIR_MISSING"
    continue
  fi

  while IFS= read -r -d '' file; do
    checked=\$((checked + 1))

    if out="\$(/usr/local/bin/move_backup_to_windows.sh "\$file" "\$tier" "\$WIN_HOST" "\$WIN_USER" "\$WIN_PASS" < /dev/null 2>&1)"; then
      moved=\$((moved + 1))
      echo "MOVED|\$tier|\$(basename "\$file")"
    else
      clean_out="\$(echo "\$out" | tr '\\r\\n' ' ' | sed -E 's/[[:space:]]+/ /g')"
      if [[ "\$clean_out" == *"Arquivo origem nao encontrado"* ]]; then
        echo "SKIP|\$tier|\$(basename "\$file")|SOURCE_MISSING"
        continue
      fi
      errors=\$((errors + 1))
      echo "ERROR|\$tier|\$(basename "\$file")|\${clean_out:0:240}"
    fi
  done < <(find "\$tier_dir" -maxdepth 1 -type f -name '*.bak' -mmin +"\$MIN_AGE_MINUTES" -print0 | sort -z)
done

echo "SUMMARY|checked=\$checked|moved=\$moved|errors=\$errors"

if [[ "\$errors" -gt 0 ]]; then
  exit 1
fi
BASH;
    }

    private function quote(string $value): string
    {
        return escapeshellarg($value);
    }

    private function resolveTiers(string $type): ?array
    {
        $normalized = strtoupper(trim($type));

        if ($normalized === 'ALL') {
            return ['DAILY', 'WEEKLY', 'MONTHLY'];
        }

        if ($normalized === 'DAILY') {
            return ['DAILY'];
        }

        if ($normalized === 'WEEKLY') {
            return ['WEEKLY'];
        }

        if ($normalized === 'MONTHLY') {
            return ['MONTHLY'];
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
}
