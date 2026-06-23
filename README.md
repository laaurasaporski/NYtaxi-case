# Case Técnico — Pipeline de Táxis de NY com Data Quality em primeiro lugar

Ingestão, modelagem e análise dos dados de táxi amarelo de NYC (jan–mai/2023) em uma
arquitetura Medallion no Databricks, com uma camada de qualidade que garante que as
respostas analíticas sejam **confiáveis**, não apenas calculadas.

> O enunciado é um case de Data Architect; a entrega traz a lente de **Data Tester** —
> com dados notoriamente sujos, entregar o pipeline não basta: é preciso provar que o
> dado é confiável. A qualidade aqui não é enfeite, ela muda a resposta.

## Arquitetura (Medallion)

| Camada | Papel | Tabela |
|---|---|---|
| Landing | arquivos originais preservados | Volume `landing` |
| Bronze | dado cru + linhagem (`_source_file`, `_ingestion_ts`) | `bronze_yellow_taxi` |
| Silver | regras de qualidade, quarentena, dedup, colunas obrigatórias | `silver_yellow_taxi` (+ `_quarantine`) |
| Gold | camada de consumo modelada para SQL | `gold_yellow_taxi` + view `vw_yellow_taxi` |
| DQ | scorecard de qualidade com histórico | `dq_results` |

## Decisões técnicas

- **Databricks Free Edition** — o Community Edition foi descontinuado em jan/2026; adaptei para o Free Edition (serverless).
- **PySpark + Delta Lake** — requisito do case + transações ACID e evolução de schema.
- **Unity Catalog** — governança e linhagem nativas, coerente com o stack do iFood.
- **Cast explícito de tipos na ingestão** — a fonte da TLC publica colunas com tipos inconsistentes entre meses; padronizei na entrada.
- **Quarentena em vez de descarte** — registros reprovados são isolados e auditáveis.
- **DQ em PySpark puro** — transparente para o case; em produção, equivaleria a expectations declarativas.

## Qualidade de dados — resultados

- Registros ingeridos: **16.186.386**
- Reprovados (≥1 regra): **850.704 (5,26%)**
- Silver confiável: **15.335.554**
- Reconciliação: 16.186.386 − 850.704 − 128 duplicatas = 15.335.554 ✅
- Maior ofensor: `passenger_count` nulo — 428.665 (2,65%)

## Respostas

### P1 — Média do total_amount por mês (yellow taxi)
| Mês | Naive | Confiável | Δ% |
|---|---|---|---|
| 2023-01 | 27,02 | 27,46 | +1,63% |
| 2023-02 | 26,90 | 27,37 | +1,75% |
| 2023-03 | 27,80 | 28,29 | +1,76% |
| 2023-04 | 28,27 | 28,78 | +1,80% |
| 2023-05 | 29,06 | 29,45 | +1,60% |

A média confiável fica ~1,7% acima da ingênua em todos os meses: os ~144 mil valores
negativos (estornos) puxavam a média para baixo. Interpretei "média do valor" como AVG por
corrida; SUM (receita do mês) está disponível trocando a agregação.

### P2 — Média de passenger_count por hora (maio)
Em todas as 24 horas a média confiável supera a ingênua, pois passenger_count nulo/zero
(o campo mais problemático do dataset) reduzia o valor artificialmente. Padrão observado:
a ocupação média sobe ao longo do dia (~1,3 de manhã, ~1,42 à noite).

## Como executar

1. Conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Importe os notebooks de `src/` e `analysis/`.
3. Rode na ordem: `00 → 01 → 02 → 03 → 04`.

## Estrutura
```
ifood-case/
├─ src/ (00_setup, 01_ingestion_bronze, 02_data_quality_silver, 03_gold_consumption)
├─ analysis/ (04_analysis_questions)
├─ README.md
└─ requirements.txt
```
