<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

class RestrictConsultaIp
{
    public function handle(Request $request, Closure $next): Response
    {
        $allowedIp = (string) env('API_ALLOWED_IP', '45.188.243.80');
        $clientIp = (string) $request->ip();
        $allowedLocalIps = ['127.0.0.1', '::1'];

        if ($clientIp !== $allowedIp && !in_array($clientIp, $allowedLocalIps, true)) {
            return response()->json([
                'status' => 'forbidden',
                'message' => 'Acesso permitido somente para o IP autorizado.',
            ], 403);
        }

        return $next($request);
    }
}
