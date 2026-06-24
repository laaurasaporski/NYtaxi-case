# NYC Taxi Case — Pipeline de Dados com Data Quality em primeiro lugar

Pipeline de ingestão, modelagem e análise dos dados de táxi amarelo de NYC (jan–mai/2023),
Construído em arquitetura **Medallion** no Databricks. 



---

## 🏛️ Arquitetura (Medallion)

```
TLC (parquet) → Landing → Bronze → Silver → Gold → Análises
                                      │
                                      ├── Quarentena (registros reprovados)
                                      └── dq_results (scorecard / histórico)
```

| Camada  | Papel                                                        | Tabela                                     |
| ------- | ------------------------------------------------------------ | ------------------------------------------ |
| Landing | arquivos originais preservados                               | Volume `landing`                           |
| Bronze  | dado cru + linhagem (`_source_file`, `_ingestion_ts`)        | `bronze_yellow_taxi`                       |
| Silver  | regras de qualidade, quarentena, dedup, colunas obrigatórias | `silver_yellow_taxi` (+ `_quarantine`)     |
| Gold    | camada de consumo modelada para SQL                          | `gold_yellow_taxi` + view `vw_yellow_taxi` |
| DQ      | scorecard de qualidade com histórico                         | `dq_results`                               |

---

## ⚙️ Decisões técnicas

- **Databricks Free Edition** — o Community Edition foi descontinuado em jan/2026; a solução foi adaptada para o Free Edition (serverless).
- **PySpark + Delta Lake** — requisito do case + transações ACID, time travel e evolução de schema.
- **Unity Catalog** — governança e linhagem nativas (catálogo → schema → tabela/volume).
- **Cast explícito de tipos na ingestão** — a fonte da TLC publica colunas com tipos inconsistentes entre meses (`timestamp_ntz` vs `timestamp`, `int` vs `long`); padronizei o schema na entrada para uma Bronze estável.
- **Quarentena em vez de descarte** — registros reprovados são isolados e auditáveis, cada um com o motivo da rejeição. Dado ruim não some: vira evidência.
- **DQ em PySpark puro** — transparente e fácil de demonstrar; em produção, na escala real, equivaleria a *expectations* declarativas (Spark Declarative Pipelines / Lakeflow) somadas à observability contínua.

---

## 🔎 Qualidade de dados — resultados

Foram aplicadas **11 regras** cobrindo **6 dimensões** de qualidade (completeness, validity,
accuracy, consistency, timeliness, uniqueness).

- Registros ingeridos: **16.186.386**
- Reprovados em ≥1 regra: **850.704 (5,26%)** — isolados em quarentena
- Silver confiável: **15.335.554**
- Reconciliação: `16.186.386 − 850.704 − 128 duplicatas = 15.335.554` ✅
- Maior ofensor: `passenger_count` nulo — 428.665 registros (2,65%)

Achados notáveis: 144 mil corridas com `total_amount` ≤ 0 (estornos), 6 mil corridas com
desembarque anterior ao embarque (violação de consistência temporal) e 104 registros vazados
de fora da janela jan–mai/2023.

---

## 📊 Respostas

As duas perguntas são respondidas em duas versões — **ingênua** (dado cru) e **confiável**
(dado validado) — para evidenciar o impacto da qualidade na decisão.

### P1 — Média do `total_amount` por mês (yellow taxi)

| Mês     | Naive | Confiável | Δ%     |
| ------- | ----- | --------- | ------ |
| 2023-01 | 27,02 | 27,46     | +1,63% |
| 2023-02 | 26,90 | 27,37     | +1,75% |
| 2023-03 | 27,80 | 28,29     | +1,76% |
| 2023-04 | 28,27 | 28,78     | +1,80% |
| 2023-05 | 29,06 | 29,45     | +1,60% |

A média confiável fica ~1,7% acima da ingênua em todos os meses: os ~144 mil valores
negativos (estornos) puxavam a média para baixo. "Média do valor" foi interpretada como
`AVG` por corrida; a receita total (`SUM`) está disponível trocando a agregação.

### P2 — Média de `passenger_count` por hora (maio)

Em todas as 24 horas a média confiável supera a ingênua, pois `passenger_count` nulo/zero —
o campo mais problemático do dataset — reduzia o valor artificialmente. Padrão observado:
a ocupação média sobe ao longo do dia (~1,3 pela manhã, ~1,42 à noite).

---

## ▶️ Como executar

1. Crie uma conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Importe os notebooks de `src/` e `analysis/` (Workspace → Import).
3. Conecte a um compute serverless e rode na ordem:
   - `00_setup` → cria schema e volume
   - `01_ingestion_bronze` → baixa os dados e materializa a Bronze
   - `02_data_quality_silver` → aplica as regras, gera scorecard, quarentena e Silver
   - `03_gold_consumption` → cria a Gold e a view de consumo SQL
   - `04_analysis_questions` → responde P1 e P2 (naive vs confiável)

> Os dados são baixados automaticamente da fonte oficial da TLC. Caso a URL mude, baixe
> manualmente em <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> e suba no Volume.

---

## 📁 Estrutura

```
NYCtaxi-case/
├─ src/
│  ├─ 00_setup
│  ├─ 01_ingestion_bronze
│  ├─ 02_data_quality_silver
│  └─ 03_gold_consumption
├─ analysis/
│  └─ 04_analysis_questions
├─ README.md
└─ requirements.txt
```

---

## 🛠️ Stack

`Databricks` · `PySpark` · `Delta Lake` · `Unity Catalog` · `SQL` · `Arquitetura Medallion`
