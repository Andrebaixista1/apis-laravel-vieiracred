<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;

class ConsultaController extends Controller
{
    public function index(Request $request): JsonResponse
    {
        $cpf = $this->onlyDigits($request->query('cpf'));
        $nb = $this->onlyDigits($request->query('nb'));

        if ($cpf === '' && $nb === '') {
            return response()->json([
                'status' => 'error',
                'message' => 'Informe ao menos um parâmetro: cpf, nb ou ambos.',
            ], 422);
        }

        $mailingRows = $this->fetchMailingRows($cpf, $nb);
        $macicaRows = $this->fetchMacicaRows($cpf, $nb);

        $mailingRows = $mailingRows
            ->sortByDesc(fn (array $row) => $this->sortTimestamp($row['data_update'] ?? null))
            ->values();

        $macicaRows = $macicaRows
            ->sortByDesc(fn (array $row) => $this->sortTimestamp($row['data_update'] ?? null))
            ->values();

        return response()->json([
            'status' => 'ok',
            'count' => $mailingRows->count() + $macicaRows->count(),
            'filters' => [
                'cpf' => $cpf !== '' ? $cpf : null,
                'nb' => $nb !== '' ? $nb : null,
            ],
            'data' => [
                'entrantes' => $mailingRows,
                'macica' => $macicaRows,
            ],
        ]);
    }

    /**
     * @return Collection<int, array<string, mixed>>
     */
    private function fetchMacicaRows(string $cpf, string $nb): Collection
    {
        $baseQuery = DB::connection('ssms_planejamento')
            ->table('MacicaCompleta.dbo.consignados_unificados_TEXT as c')
            ->selectRaw(<<<SQL
                DISTINCT
                c.[nb],
                c.[nome_segurado],
                c.[dt_nascimento],
                c.[nu_cpf],
                c.[esp],
                c.[dib],
                c.[ddb],
                c.[vl_beneficio],
                c.[id_banco_pagto],
                c.[id_agencia_banco],
                c.[id_orgao_pagador],
                c.[nu_conta_corrente],
                c.[aps_benef],
                c.[cs_meio_pagto],
                c.[id_banco_empres],
                c.[id_contrato_empres],
                c.[vl_empres],
                c.[comp_ini_desconto],
                c.[comp_fim_desconto],
                c.[quant_parcelas],
                c.[vl_parcela],
                c.[tipo_empres],
                c.[endereco],
                c.[bairro],
                c.[municipio],
                c.[uf],
                c.[cep],
                c.[situacao_empres],
                c.[dt_averbacao_consig],
                c.[idade],
                c.[pagas],
                c.[restantes],
                c.[nb_tratado],
                c.[dt_nascimento_tratado],
                c.[nu_cpf_tratado],
                c.[vl_beneficio_tratado],
                c.[comp_ini_desconto_tratado],
                c.[comp_fim_desconto_tratado],
                c.[quant_parcelas_tratado],
                c.[vl_parcela_tratado],
                c.[vl_empres_tratado],
                c.[data_update],
                c.[nu_cpf_ix],
                c.[nb_ix]
            SQL);

        if ($cpf !== '') {
            $baseQuery->where('c.nu_cpf_ix', $cpf);
        }

        if ($nb !== '') {
            $baseQuery->where('c.nb_ix', $nb);
        }

        return DB::connection('ssms_planejamento')
            ->query()
            ->fromSub($baseQuery, 'base')
            ->select([
                'nb',
                'nome_segurado',
                'dt_nascimento',
                'nu_cpf',
                'esp',
                'dib',
                'ddb',
                'vl_beneficio',
                'id_banco_pagto',
                'id_agencia_banco',
                'id_orgao_pagador',
                'nu_conta_corrente',
                'aps_benef',
                'cs_meio_pagto',
                'id_banco_empres',
                'id_contrato_empres',
                'vl_empres',
                'comp_ini_desconto',
                'comp_fim_desconto',
                'quant_parcelas',
                'vl_parcela',
                'tipo_empres',
                'endereco',
                'bairro',
                'municipio',
                'uf',
                'cep',
                'situacao_empres',
                'dt_averbacao_consig',
                'idade',
                'pagas',
                'restantes',
                'nb_tratado',
                'dt_nascimento_tratado',
                'nu_cpf_tratado',
                'vl_beneficio_tratado',
                'comp_ini_desconto_tratado',
                'comp_fim_desconto_tratado',
                'quant_parcelas_tratado',
                'vl_parcela_tratado',
                'vl_empres_tratado',
                'data_update',
                'nu_cpf_ix',
                'nb_ix',
            ])
            ->orderByDesc('data_update')
            ->limit(50)
            ->get()
            ->map(fn ($row) => (array) $row);
    }

    /**
     * @return Collection<int, array<string, mixed>>
     */
    private function fetchMailingRows(string $cpf, string $nb): Collection
    {
        $baseQuery = DB::connection('ssms_planejamento')
            ->table('Mailing.dbo.MAILING_UNIFICADO as m')
            ->selectRaw(<<<SQL
                DISTINCT
                m.[NOME],
                m.[CPF],
                m.[IDADE],
                m.[Data_Nascimento],
                m.[Beneficio],
                m.[CODIGO_ESPECIE],
                m.[DDB],
                m.[Municipio],
                m.[UF],
                m.[VALOR_BENEFICIO],
                m.[MARGEM_RMC],
                m.[MARGEM_DISPONIVEL],
                m.[Margem_RCC],
                m.[Banco],
                m.[Agencia],
                m.[Conta],
                m.[Meio_Pagamento],
                m.[CELULAR1],
                m.[CELULAR2],
                m.[CELULAR3],
                m.[CPF_LIMPO],
                m.[BENEFICIO_LIMPO],
                m.[CELULAR4],
                m.[Data_Lemit],
                m.[valor_liberador_RCC],
                m.[valor_liberador_RMC],
                m.[Total_Valor_Liberado(0.02801)] AS [Total_Valor_Liberado_002801],
                m.[total_cartao],
                m.[Total_Valor_Liberado]
            SQL);

        if ($cpf !== '') {
            $baseQuery->where('m.CPF_LIMPO', $cpf);
        }

        if ($nb !== '') {
            $baseQuery->where('m.BENEFICIO_LIMPO', $nb);
        }

        return DB::connection('ssms_planejamento')
            ->query()
            ->fromSub($baseQuery, 'base')
            ->select([
                'NOME',
                'CPF',
                'IDADE',
                'Data_Nascimento',
                'Beneficio',
                'CODIGO_ESPECIE',
                'DDB',
                'Municipio',
                'UF',
                'VALOR_BENEFICIO',
                'MARGEM_RMC',
                'MARGEM_DISPONIVEL',
                'Margem_RCC',
                'Banco',
                'Agencia',
                'Conta',
                'Meio_Pagamento',
                'CELULAR1',
                'CELULAR2',
                'CELULAR3',
                'CPF_LIMPO',
                'BENEFICIO_LIMPO',
                'CELULAR4',
                'Data_Lemit',
                'valor_liberador_RCC',
                'valor_liberador_RMC',
                'Total_Valor_Liberado_002801',
                'total_cartao',
                'Total_Valor_Liberado',
            ])
            ->orderByDesc('Data_Lemit')
            ->limit(50)
            ->get()
            ->map(function ($row) {
                $data = (array) $row;
                $data['Total_Valor_Liberado(0.02801)'] = $data['Total_Valor_Liberado_002801'] ?? null;
                unset($data['Total_Valor_Liberado_002801']);

                return $data;
            });
    }

    private function onlyDigits(mixed $value): string
    {
        return preg_replace('/\D+/', '', (string) $value) ?? '';
    }

    private function sortTimestamp(mixed $value): int
    {
        if (!is_string($value) || trim($value) === '') {
            return 0;
        }

        $parsed = strtotime($value);
        return $parsed === false ? 0 : $parsed;
    }
}
