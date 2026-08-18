# Scoring Logic

## Purpose and scope

EntityRisk Navigator provides two separate indicators:

- `risk_score`: an entity-level sanctions-screening escalation indicator.
- `macro_pressure_score`: a euro-area macroeconomic pressure indicator derived from selected ECB time series.

They answer different questions and must not be added together or interpreted as a single credit, AML, KYC, sanctions, or financial-risk rating. Both indicators are transparent, rule-based context for review.

## Entity Risk Score

### Business purpose

`marts.mart_entity_risk_score` summarizes sanctions-screening matches for every GLEIF-based legal entity. The score signals the strongest available screening outcome, not the number of matches and not a probability that an entity is sanctioned.

The source data is EU Financial Sanctions Files and OpenSanctions. The candidate entity data comes from GLEIF.

### Match creation and quality assessment

A potential match is created only when the normalized legal name of a GLEIF entity and a normalized sanctions name have the same exact match key. The current implementation does not use fuzzy matching.

Before assigning a match-quality tier, the model considers:

- whether the sanctions record contains the entity's LEI or source candidate ID (`identifier_overlap`);
- whether the entity country occurs in the sanctions-source countries (`country_overlap`);
- whether the sanctions name is a primary name or an alias;
- whether the match key is short (three characters or fewer) or on a maintained list of generic terms such as `bank`, `company`, `group`, or `holding`;
- whether country information is missing or conflicts.

The match-quality score and tier are assigned as follows:

| Condition, in priority order | `match_quality_score` | `match_quality_tier` |
| --- | ---: | --- |
| Identifier overlap | 100 | `high_confidence` |
| Primary-name match, country overlap, and neither short nor generic key | 95 | `high_confidence` |
| Primary-name match and neither short nor generic key | 90 | `medium_confidence` |
| Alias-name match, country overlap, and neither short nor generic key | 85 | `medium_confidence` |
| EU FSF alias-name match with unavailable sanctions-country context, and neither short nor generic key | 80 | `medium_confidence` |
| Short or generic match key | 50 | `review_required` |
| All other matches, including a country mismatch | 70 | `review_required` |

The EU FSF exception reflects that the current unified EU FSF subject model does not provide sanctions-country data. A clean exact EU FSF alias match is therefore not downgraded solely because this context is unavailable.

### Entity-level calculation

For each entity, the model aggregates all screening matches and evaluates the tiers in descending severity. The calculation is:

$$
\text{risk\_score} =
\begin{cases}
100 & \text{if at least one high-confidence match exists} \\
60 & \text{otherwise, if at least one medium-confidence match exists} \\
20 & \text{otherwise, if at least one review-required match exists} \\
0 & \text{otherwise}
\end{cases}
$$

| `risk_score` | `risk_tier` | Business interpretation |
| ---: | --- | --- |
| 100 | `high` | At least one high-confidence sanctions-screening match; immediate investigation is indicated. |
| 60 | `medium` | At least one medium-confidence match and no high-confidence match; review and corroboration are indicated. |
| 20 | `review` | Only matches requiring review are present; this is a possible name collision or weak signal. |
| 0 | `low_or_no_known_match` | No current screening match was produced; this is not evidence that the entity is free of sanctions risk. |

The mart also retains match counts by quality tier, the number of source systems and distinct sanctioned subjects, the best quality rank, source details, and `risk_reasons`. These fields are evidence for investigation, rather than additional inputs to the score.

### Interpretation limits

The score is deliberately a conservative escalation rule. Multiple medium-confidence matches do not exceed `60`, and multiple review-required matches do not exceed `20`. The score does not incorporate ownership, transaction activity, adverse media, customer risk, exposure, a sanctions-list legal determination, or an analyst decision. A screening result always requires validation against source records and the entity's identity attributes.

## ECB Macro Pressure Score

### Business purpose

`marts.mart_ecb_macro_pressure_score` provides an aggregate euro-area macroeconomic context indicator on a $0$ to $100$ scale. It measures the level and recent direction of selected interest-rate and borrowing-cost indicators relative to their own available histories. It is not an entity default-risk model and does not predict a financial outcome.

The score is calculated from the latest available observation for each selected ECB series in the relevant reference area. The model uses four indicators:

| Indicator | Code | Weight |
| --- | --- | ---: |
| Cost of borrowing for corporations | `cost_of_borrowing_corporations` | 35% |
| Euro short-term rate | `euro_short_term_rate` | 25% |
| AAA yield curve 10Y spot rate | `aaa_yield_curve_10y_spot_rate` | 25% |
| AAA yield curve 2Y spot rate | `aaa_yield_curve_2y_spot_rate` | 15% |

The weights total $100\%$. Any other available ECB series receive a zero weight and do not affect the aggregate score.

### Time-series inputs

For each series, the time-series mart calculates the previous observation, absolute and percentage change, and rolling averages over the most recent 3, 6, and 12 observations. The pressure-score model uses the full available history to calculate percentiles and uses up to the 12 most recent observations for the trend.

For a latest observation $x_t$ and its prior observation $x_{t-1}$:

$$
\Delta x_t = x_t - x_{t-1}
$$

The level and momentum inputs are percentile ranks from $0$ to $100$:

$$
L = 100 \cdot \operatorname{cume\_dist}(x_t)
$$

$$
M = 100 \cdot \operatorname{cume\_dist}(\Delta x_t)
$$

The ranks are calculated separately for each indicator and reference area. A high level score therefore means the latest value is high relative to that series' own retained history; a high momentum score means its latest absolute change is high relative to that change history.

The model estimates a linear regression slope across up to the latest 12 dated observations. It converts the slope to a directional score $T$:

$$
T =
\begin{cases}
75 & \text{positive slope (upward)} \\
25 & \text{negative slope (downward)} \\
50 & \text{zero or unavailable slope (stable or unknown)}
\end{cases}
$$

### Indicator and aggregate calculation

Each selected indicator receives an `indicator_pressure_score`:

$$
P_i = 0.50L_i + 0.30M_i + 0.20T_i
$$

The aggregate `macro_pressure_score` is the weighted average of available indicator scores. Let $w_i$ be an indicator weight:

$$
\text{macro\_pressure\_score} = \frac{\sum_i P_iw_i}{\sum_i w_i}
$$

The denominator only contains selected indicators with an available latest observation. This avoids treating unavailable data as a zero-pressure observation.

| Score range | `macro_pressure_level` | Business interpretation |
| ---: | --- | --- |
| $[75, 100]$ | `high` | Selected indicators are high relative to their histories and/or show sustained upward pressure. |
| $[50, 75)$ | `elevated` | Financing and rate pressure are increased relative to the historical range. |
| $[25, 50)$ | `moderate` | Some pressure is evident, but not at an elevated level. |
| $[0, 25)$ | `low` | Low pressure according to the selected indicators. |

The weighted trend signal is reported separately:

$$
\text{weighted\_trend\_signal} = \frac{\sum_i d_iw_i}{\sum_i w_i}, \qquad
d_i \in \{-1, 0, 1\}
$$

where $d_i$ is $1$ for an upward trend, $-1$ for a downward trend, and $0$ for stable or unknown. The reported aggregate trend is `upward` when the signal is above $0.15$, `downward` when below $-0.15$, and otherwise `stable`.

## Entity Macro Context

`marts.mart_entity_macro_context` applies the macro-pressure score as context to each entity; it does not change the sanctions `risk_score`.

| Entity jurisdiction | Applicability | Weight | `entity_macro_context_score` |
| --- | --- | ---: | --- |
| Euro-area country | `applicable` | 1.00 | Full `macro_pressure_score` |
| Outside the euro area | `reduced` | 0.35 | $0.35 \cdot \text{macro\_pressure\_score}$ |
| Country missing | `unknown` | 0.30 | $0.30 \cdot \text{macro\_pressure\_score}$ |

Euro-area applicability covers AT, BE, HR, CY, EE, FI, FR, DE, GR, IE, IT, LV, LT, LU, MT, NL, PT, SK, SI, and ES. For entities outside the euro area or without a mapped country, ECB information is shown as broader context with reduced applicability.

## Data lineage and refresh

The score marts are rebuilt during each dbt pipeline run. `source_load_date`, source object keys, raw identifiers, matching reasons, and load timestamps are retained in upstream marts to support tracing a displayed score back to the relevant source snapshot. The current model definition is the authoritative implementation in [mart_entity_risk_score.sql](../dbt/models/marts/mart_entity_risk_score.sql), [mart_ecb_macro_pressure_score.sql](../dbt/models/marts/mart_ecb_macro_pressure_score.sql), and [mart_entity_macro_context.sql](../dbt/models/marts/mart_entity_macro_context.sql).