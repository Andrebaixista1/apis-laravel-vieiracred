<?php

use App\Http\Controllers\ConsultaV8Controller;
use App\Http\Controllers\ConsultaPresencaController;
use App\Http\Controllers\ConsultaHandmaisController;
use App\Http\Controllers\HealthConsultController;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| API Routes
|--------------------------------------------------------------------------
|
| Here is where you can register API routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| is assigned the api middleware group. Enjoy building your API!
|
*/

Route::middleware('auth:sanctum')->get('/user', function (Request $request) {
    return $request->user();
});

Route::get('/db-health', function () {
    $connections = [
        'sqlsrv_kinghost_vps',
        'sqlsrv_hostinger_vps',
        'sqlsrv_servidor_planejamento',
    ];

    $results = [];

    foreach ($connections as $connection) {
        $start = microtime(true);

        try {
            DB::connection($connection)->select('SELECT 1 AS ok');

            $results[$connection] = [
                'status' => 'up',
                'latency_ms' => round((microtime(true) - $start) * 1000, 2),
            ];
        } catch (\Throwable $e) {
            $results[$connection] = [
                'status' => 'down',
                'latency_ms' => round((microtime(true) - $start) * 1000, 2),
                'error' => $e->getMessage(),
            ];
        }
    }

    return response()->json([
        'message' => 'Database connectivity check (read-only)',
        'timestamp' => now()->toIso8601String(),
        'results' => $results,
    ]);
});

Route::get('/health-consult', [HealthConsultController::class, 'index']);
Route::post('/health-consult/force-backup', [HealthConsultController::class, 'forceBackup']);

Route::get('/consulta-v8/run', [ConsultaV8Controller::class, 'run']);
Route::get('/consulta-presenca/run', [ConsultaPresencaController::class, 'run']);
Route::get('/consulta-handmais/run', [ConsultaHandmaisController::class, 'run']);
Route::delete('/consulta-presenca/consultas', [ConsultaPresencaController::class, 'deleteConsultasByLote']);
Route::post('/consulta-presenca/consultas', [ConsultaPresencaController::class, 'deleteConsultasByLote']);

Route::post('/consulta-v8', [ConsultaV8Controller::class, 'store']);
Route::post('/consulta-v8/individual', [ConsultaV8Controller::class, 'storeIndividual']);
Route::post('/consulta-v8/liberar-pendentes', [ConsultaV8Controller::class, 'releasePendingByScope']);
Route::delete('/consulta-v8/consultas', [ConsultaV8Controller::class, 'deleteConsultasByLote']);
Route::post('/consulta-handmais', [ConsultaHandmaisController::class, 'store']);
Route::post('/consulta-handmais/individual', [ConsultaHandmaisController::class, 'storeIndividual']);
Route::get('/consulta-handmais/limites', [ConsultaHandmaisController::class, 'listLimites']);
Route::get('/consulta-handmais/consultas', [ConsultaHandmaisController::class, 'listConsultas']);
