<?php

use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Route;
use App\Http\Controllers\Api\ConsultaController;

Route::get('/health', function (): JsonResponse {
    $checkedAt = now()->toISOString();

    try {
        $row = DB::connection('ssms_planejamento')->selectOne('SELECT DB_NAME() AS db_name, @@SERVERNAME AS server_name');

        return response()->json([
            'status' => 'ok',
            'connection' => 'ssms_planejamento',
            'database' => $row->db_name ?? null,
            'server' => $row->server_name ?? null,
            'checked_at' => $checkedAt,
        ]);
    } catch (\Throwable $e) {
        return response()->json([
            'status' => 'error',
            'connection' => 'ssms_planejamento',
            'message' => 'Falha ao consultar o banco.',
            'checked_at' => $checkedAt,
        ], 503);
    }
});

Route::get('/consultas', [ConsultaController::class, 'index'])->middleware('consulta.ip');
