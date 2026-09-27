<h1 align="center">fin-glassbox</h1>
<h2 align="center">An Explainable Multimodal Neural Framework for Financial Risk Management</h2>

A research-oriented financial AI system for building risk-aware, explainable market decision pipelines from multiple financial data modalities. The framework combines temporal market encoders, FinBERT-based text encoders, graph neural risk modelling, classical financial risk measures, trained analyst modules, interpretable position sizing, and hybrid fusion with explicit XAI traces.

The repository is designed around one central idea: financial decisions should not come from one opaque monolithic model. Instead, the system decomposes the financial decision problem into specialised modules, lets each module model one part of market risk or market context, and then fuses those outputs through a transparent, risk-constrained decision layer.

> **Research scope:** This repository is for academic research, experimentation, and explainable AI system design. It is not financial advice, trading advice, or an investment product.

---

## Table of contents

- [Project purpose](#project-purpose)
- [System philosophy](#system-philosophy)
- [High-level architecture](#high-level-architecture)
- [Repository structure](#repository-structure)
- [Documentation map](#documentation-map)
- [Data families](#data-families)
- [Major module families](#major-module-families)
- [Fusion and final decision design](#fusion-and-final-decision-design)
- [Explainability design](#explainability-design)
- [Setup](#setup)
- [Common command pattern](#common-command-pattern)
- [Outputs and artefacts](#outputs-and-artefacts)
- [Research and engineering safeguards](#research-and-engineering-safeguards)
- [Team](#team)
- [Licence](#licence)

---

## Project purpose

Financial risk management is a naturally multimodal problem. Market risk is visible in price movement and volatility, but also in financial text, macroeconomic state, liquidity, drawdown structure, cross-asset contagion, and regime shifts. A single model can learn some of these patterns, but it is difficult to audit, explain, and defend when the model’s internal reasoning is opaque.

This repository implements a modular neural framework that separates the problem into interpretable components:

- market sequence encoding,
- financial text encoding,
- technical analysis,
- sentiment and news analysis,
- volatility risk,
- drawdown risk,
- Value-at-Risk and Expected Shortfall,
- liquidity risk,
- graph-based contagion risk,
- graph/macro-based regime detection,
- position sizing,
- qualitative synthesis,
- quantitative synthesis,
- hybrid fusion,
- and explainable final decision reporting.

The framework is built to support research questions such as:

- Can specialised multimodal financial models improve transparency compared with one monolithic black-box predictor?
- Can module-level XAI traces be propagated into a full-system explanation?
- Can risk-aware fusion preserve both predictive information and explicit safety constraints?
- Can graph-based cross-asset structure support contagion and regime risk modelling?
- Can financial text embeddings improve qualitative context without weakening auditability?

---

## System philosophy

The framework follows a **specialisation + multimodality + explainability + risk-control** design.

### Specialisation

Each module is assigned a clear role. The volatility module estimates instability, the drawdown module estimates downside path risk, StemGNN estimates cross-asset contagion, MTGNN-style graph logic contributes to regime detection, FinBERT supports financial text understanding, and Fusion combines branch outputs into a final decision.

### Multimodality

The system uses structured market data, textual filing/news representations, macro/regime data, and cross-asset relation graphs. These modalities are not merged prematurely. They are processed by specialised pipelines first, then integrated at higher synthesis layers.

### Explainability

Every major module is expected to expose explanation artefacts, not only predictions. XAI is treated as a first-class system output rather than an afterthought.

### Risk-aware decision-making

The Risk Engine is central. Fusion is not allowed to ignore risk outputs. Position sizing and final rules constrain the final output so that predicted opportunity does not automatically become unrestricted exposure.

---

## High-level architecture

```text
INPUTS
├── Time-Series Market Data
├── Financial Text Data
├── Macro / Regime Data
└── Cross-Asset Relation Data

ENCODERS
├── Shared Temporal Attention Encoder
└── FinBERT Financial Text Encoder

ANALYST MODULES
├── Technical Analyst
├── Sentiment Analyst
├── News Analyst
├── Qualitative Analyst
└── Quantitative Analyst

RISK ENGINE
├── Volatility Risk Module
├── Drawdown Risk Module
├── Historical VaR Module
├── CVaR / Expected Shortfall Module
├── StemGNN Contagion Risk Module
├── Liquidity Risk Module
├── MTGNN Regime Detection Module
└── Position Sizing Engine

FUSION
├── Learned Fusion Layer
└── User Rule Barrier

OUTPUT
├── Buy / Hold / Sell
├── Confidence Score
├── Position Size
├── Risk Summary
└── XAI Explanation Trace
```

The active implementation excludes fundamentals. Older design documents may mention fundamentals as part of earlier architecture planning; the current repository-level documentation treats the active implementation as market + text + macro + graph + risk + fusion.

For the full system workflow, see [`WORKFLOW.md`](WORKFLOW.md). For the explainability design, see [`xAI.md`](xAI.md).

---

## Repository structure

```text
fin-glassbox/
├── README.md
├── setup/
├── WORKFLOW.md
├── xAI.md
├── LICENSE
├── requirements_linux_venv.txt
├── code/
│   ├── analysts/
│   ├── encoders/
│   ├── fusion/
│   ├── gnn/
│   ├── riskEngine/
│   └── yfinance_ib/
├── data/
│   ├── FRED_data/
│   ├── graphs/
│   ├── sec_edgar/
│   └── yFinance/
├── researchPapers/
└── outputs/
```

### `code/`

Contains the executable model and pipeline code. Most module files are designed to be usable both as importable modules and as CLI scripts for inspection, smoke testing, HPO, training, prediction, and validation.

### `data/`

Contains data engineering documentation and expected data-family locations. Large raw and processed datasets are usually external, ignored, or handled separately from normal Git tracking because of size.

### `outputs/`

Contains generated embeddings, model checkpoints, predictions, HPO artefacts, XAI summaries, and fused decision outputs. This directory is a runtime artefact location rather than source documentation.

### `researchPapers/`

Contains project-level research notes, workflow references, hyperparameter configuration notes, literature-related material, and older architectural references.

---

## Documentation map

The repository is intentionally documentation-heavy because the system is modular. Important information is distributed across module-level README files and specialised markdown files.

### Root-level documentation

| File | Purpose |
|---|---|
| [`README.md`](README.md) | Repository overview, architecture, documentation map, and project introduction. |
| [`SETUP.md`](setup/SETUP.md) | Environment setup, dependency installation, Git LFS, CUDA notes, and validation commands. |
| [`WORKFLOW.md`](WORKFLOW.md) | Full current workflow and module interaction design. |
| [`xAI.md`](xAI.md) | XAI integration strategy across encoders, analysts, risk modules, fusion, and final outputs. |
| [`LICENSE`](LICENSE) | Repository licence. |

### Analyst documentation

| File | Purpose |
|---|---|
| [`code/analysts/README.md`](code/analysts/README.md) | Overview of all analyst modules. |
| [`code/analysts/SentimentAnalyst.md`](code/analysts/SentimentAnalyst.md) | FinBERT-based sentiment analyst documentation. |
| [`code/analysts/NewsAnalyst.md`](code/analysts/NewsAnalyst.md) | News/event analyst documentation. |
| [`code/analysts/TechnicalAnalyst.md`](code/analysts/TechnicalAnalyst.md) | Technical analyst documentation using temporal embeddings. |
| [`code/analysts/QuantitativeAnalyst.md`](code/analysts/QuantitativeAnalyst.md) | Quantitative analyst documentation and attention-weighted risk pooling design. |
| [`code/analysts/QualitativeAnalyst.md`](code/analysts/QualitativeAnalyst.md) | Qualitative analyst documentation. |

### Encoder documentation

| File | Purpose |
|---|---|
| [`code/encoders/README.md`](code/encoders/README.md) | Encoder folder overview. |
| [`code/encoders/TemporalEncoder.md`](code/encoders/TemporalEncoder.md) | Shared Temporal Attention Encoder documentation. |
| [`code/encoders/FinBERT_Encoder.md`](code/encoders/FinBERT_Encoder.md) | FinBERT encoder, MLM adaptation, PCA projection, and embedding generation documentation. |
| [`code/encoders/TextEncoder.md`](code/encoders/TextEncoder.md) | Text encoding design notes and related text pipeline context. |

### GNN documentation

| File | Purpose |
|---|---|
| [`code/gnn/README.md`](code/gnn/README.md) | GNN folder overview. |
| [`code/gnn/CrossAssetRelationData.md`](code/gnn/CrossAssetRelationData.md) | Cross-asset graph data construction documentation. |
| [`code/gnn/StemGNN.md`](code/gnn/StemGNN.md) | StemGNN documentation. |
| [`code/gnn/StemGNN_Contagion.md`](code/gnn/StemGNN_Contagion.md) | StemGNN contagion risk module documentation. |
| [`code/gnn/MTGNN.md`](code/gnn/MTGNN.md) | MTGNN usage documentation for regime graph construction. |

### Risk Engine documentation

| File | Purpose |
|---|---|
| [`code/riskEngine/README.md`](code/riskEngine/README.md) | Risk Engine folder overview. |
| [`code/riskEngine/Volatility_Risk_Module.md`](code/riskEngine/Volatility_Risk_Module.md) | Volatility model documentation. |
| [`code/riskEngine/Drawdown_Risk_Module.md`](code/riskEngine/Drawdown_Risk_Module.md) | Drawdown risk model documentation. |
| [`code/riskEngine/Regime_Detection_Module.md`](code/riskEngine/Regime_Detection_Module.md) | Regime detection module documentation. |
| [`code/riskEngine/Position_Sizing_Engine.md`](code/riskEngine/Position_Sizing_Engine.md) | Position sizing engine documentation. |
| [`code/riskEngine/VaR_CVaR_Liquidity.md`](code/riskEngine/VaR_CVaR_Liquidity.md) | Historical VaR, CVaR, and liquidity documentation. |

### Fusion documentation

| File | Purpose |
|---|---|
| [`code/fusion/README.md`](code/fusion/README.md) | Hybrid learned-fusion and rule-barrier documentation. |

### Data documentation

| File | Purpose |
|---|---|
| [`data/README.md`](data/README.md) | Data folder overview. |
| [`data/FRED_data/README.md`](data/FRED_data/README.md) | FRED macro/regime data documentation. |
| [`data/graphs/README.md`](data/graphs/README.md) | Cross-asset graph data folder documentation. |
| [`data/sec_edgar/processed/DataProcessing.md`](data/sec_edgar/processed/DataProcessing.md) | SEC data processing methodology. |
| [`data/sec_edgar/processed/cleaned/README.md`](data/sec_edgar/processed/cleaned/README.md) | Cleaned SEC data documentation. |
| [`data/yFinance/yFinance.md`](data/yFinance/yFinance.md) | Market data acquisition and processing documentation. |

### Research and configuration notes

| File | Purpose |
|---|---|
| [`researchPapers/Hyperparameter_Config.md`](researchPapers/Hyperparameter_Config.md) | Hyperparameter and HPO design references. |
| [`researchPapers/XAI_Specifications.md`](researchPapers/XAI_Specifications.md) | Earlier XAI specification source. |
| [`researchPapers/MASTER_PROMPT.md`](researchPapers/MASTER_PROMPT.md) | Project context reference. |
| [`researchPapers/MASTER_PROMPT2.md`](researchPapers/MASTER_PROMPT2.md) | Updated project context reference. |
| [`researchPapers/WORKFLOW_v1.md`](researchPapers/WORKFLOW_v1.md) | Earlier workflow reference. |
| [`researchPapers/WORKFLOW_v2.md`](researchPapers/WORKFLOW_v2.md) | Updated workflow reference. |

---

## Data families

The framework is organised around four active data families.

### 1. Time-Series Market Data

Used by the Temporal Encoder, Technical Analyst, Volatility Risk Module, Drawdown Risk Module, VaR/CVaR calculations, Liquidity Risk Module, Position Sizing Engine, Quantitative Analyst, and Fusion.

Typical fields include:

- ticker,
- date,
- open, high, low, close,
- volume,
- returns,
- engineered technical features,
- rolling volatility,
- liquidity proxies.

Relevant documentation:

- [`data/yFinance/yFinance.md`](data/yFinance/yFinance.md)
- [`code/encoders/TemporalEncoder.md`](code/encoders/TemporalEncoder.md)
- [`code/analysts/TechnicalAnalyst.md`](code/analysts/TechnicalAnalyst.md)

### 2. Financial Text Data

Used by FinBERT, Sentiment Analyst, News Analyst, and Qualitative Analyst.

Typical fields include:

- filing or event date,
- ticker or CIK/entity mapping,
- form type,
- source section,
- document/chunk identifiers,
- text embeddings,
- sentiment and event-level predictions.

Relevant documentation:

- [`code/encoders/FinBERT_Encoder.md`](code/encoders/FinBERT_Encoder.md)
- [`code/analysts/SentimentAnalyst.md`](code/analysts/SentimentAnalyst.md)
- [`code/analysts/NewsAnalyst.md`](code/analysts/NewsAnalyst.md)
- [`code/analysts/QualitativeAnalyst.md`](code/analysts/QualitativeAnalyst.md)

### 3. Macro / Regime Data

Used to support regime characterisation and market-state modelling.

Typical fields include:

- interest rate series,
- yield curve spreads,
- credit spreads,
- currency and commodity indicators,
- macro stress proxies,
- recession or inversion-style indicators.

Relevant documentation:

- [`data/FRED_data/README.md`](data/FRED_data/README.md)
- [`code/riskEngine/Regime_Detection_Module.md`](code/riskEngine/Regime_Detection_Module.md)

### 4. Cross-Asset Relation Data

Used for graph-based systemic and contagion risk modelling.

Typical fields include:

- graph snapshots,
- rolling correlation edges,
- source and target tickers,
- edge weights,
- sector metadata,
- beta estimates,
- market-cap proxies,
- static and temporal graph features.

Relevant documentation:

- [`data/graphs/README.md`](data/graphs/README.md)
- [`code/gnn/CrossAssetRelationData.md`](code/gnn/CrossAssetRelationData.md)
- [`code/gnn/StemGNN_Contagion.md`](code/gnn/StemGNN_Contagion.md)

---

## Major module families

### Encoders

Encoders transform raw or engineered data into dense representations that downstream modules can consume.

- **Temporal Encoder:** attention-based encoder for market sequence embeddings.
- **FinBERT Encoder:** financial text encoder for SEC filing/news text, with domain-adaptive masked language modelling and dimensionality projection.
- **Text Encoder utilities:** supporting text-related embedding and manifest logic.

See [`code/encoders/README.md`](code/encoders/README.md).

### Analysts

Analyst modules convert encoder and model outputs into semantically meaningful intermediate decisions.

- **Technical Analyst:** produces trend, momentum, and timing interpretations from temporal embeddings.
- **Sentiment Analyst:** estimates financial sentiment from FinBERT-derived representations.
- **News Analyst:** estimates event impact, news importance, and risk relevance.
- **Qualitative Analyst:** synthesises text-side sentiment/news evidence into qualitative daily signals.
- **Quantitative Analyst:** attention-pools risk and technical outputs into a quantitative branch signal.

See [`code/analysts/README.md`](code/analysts/README.md).

### GNN modules

GNN modules provide graph-aware modelling for relation-driven financial risk.

- **StemGNN Contagion:** models cross-asset contagion risk across multiple horizons.
- **MTGNN-style Regime Graph Builder:** contributes graph-learning logic for regime detection.
- **Cross-Asset Relation Data Builder:** constructs graph snapshots and metadata for relation-aware modelling.

See [`code/gnn/README.md`](code/gnn/README.md).

### Risk Engine

The Risk Engine is the central risk-control block of the framework. It produces the risk summaries and constraints used by position sizing, quantitative analysis, fusion, and final decisions.

It includes:

- volatility risk,
- drawdown risk,
- VaR,
- CVaR,
- liquidity risk,
- contagion risk,
- regime risk,
- position sizing.

See [`code/riskEngine/README.md`](code/riskEngine/README.md).

### Fusion

Fusion integrates the qualitative and quantitative branches. It uses a learned fusion layer to estimate branch weights and decision signals, followed by a rule barrier that enforces user-defined risk constraints.

See [`code/fusion/README.md`](code/fusion/README.md).

---

## Fusion and final decision design

The Fusion Engine is intentionally hybrid.

```text
Quantitative Analyst output
        │
        ├── learned branch weighting
        │
Qualitative Analyst output
        │
        ▼
Learned Fusion Layer
        │
        ▼
User Rule Barrier
        │
        ▼
Final Decision + Explanation
```

The learned layer estimates:

- fused signal,
- fused risk,
- fused confidence,
- learned Buy/Hold/Sell probabilities,
- quantitative branch weight,
- qualitative branch weight,
- learned position multiplier.

The rule barrier applies explicit constraints such as:

- no trading when liquidity is inadequate,
- no trading when a ticker is marked non-tradable,
- exposure caps in crisis regimes,
- drawdown and contagion caps,
- position caps based on the Position Sizing Engine,
- final position never exceeding the risk-approved allocation.

This design keeps the final system transparent: the model can learn how to combine evidence, but it cannot silently bypass the risk policy.

---

## Explainability design

XAI is integrated across the system at three levels.

### Module-level XAI

Each major model exposes local explanations relevant to its own modelling task. Examples include attention weights, gradient importance, top risk drivers, graph properties, graph edges, counterfactual summaries, and rule-trigger explanations.

### Branch-level XAI

The Qualitative Analyst and Quantitative Analyst aggregate module outputs and preserve branch-level reasoning. The system records why a text signal or a risk driver dominates a branch-level output.

### System-level XAI

Fusion combines branch explanations and adds final rule-barrier explanations. Final outputs include both the learned fusion rationale and any user-rule override rationale.

For the full XAI design, see [`xAI.md`](xAI.md).

---

## Setup

Environment setup, Git LFS, Python versioning, dependency installation, and smoke-test validation are documented in [`SETUP.md`](setup/SETUP.md).

A typical setup flow is:

```bash
cd ~/fin-glassbox && python --version
```

```bash
cd ~/fin-glassbox && source venv3.12.7/bin/activate
```

```bash
cd ~/fin-glassbox && python -m py_compile code/encoders/temporal_encoder.py code/encoders/finbert_encoder.py
```

```bash
cd ~/fin-glassbox && python code/fusion/final_fusion.py smoke --repo-root . --device cuda
```

See [`SETUP.md`](setup/SETUP.md) for the complete setup and validation procedure.

---

## Common command pattern

Most major model files follow this pattern:

```bash
cd ~/fin-glassbox && python path/to/module.py inspect --repo-root .
```

```bash
cd ~/fin-glassbox && python path/to/module.py smoke --repo-root . --device cuda
```

```bash
cd ~/fin-glassbox && python path/to/module.py hpo --repo-root . --chunk 1 --trials 30 --device cuda --fresh
```

```bash
cd ~/fin-glassbox && python path/to/module.py train-best --repo-root . --chunk 1 --device cuda --fresh
```

```bash
cd ~/fin-glassbox && python path/to/module.py predict --repo-root . --chunk 1 --split test --device cuda
```

```bash
cd ~/fin-glassbox && python path/to/module.py validate --repo-root . --chunk 1 --split test
```

Exact commands differ by module. Use the module-specific markdown files for the correct CLI interface.

---

## Outputs and artefacts

Runtime outputs are organised under `outputs/`. Common output categories include:

```text
outputs/
├── embeddings/
├── models/
├── results/
├── codeResults/
└── cache/
```

Typical artefacts include:

- `.npy` embedding matrices,
- `.csv` manifests and prediction outputs,
- `.pt` PyTorch checkpoints,
- `.json` summaries and XAI traces,
- `.pkl` PCA or preprocessing objects where required,
- cached panels for large data operations.

Large outputs should generally not be committed to normal Git history. Use `.gitignore`, Git LFS, or external storage where appropriate.

---

## Research and engineering safeguards

The framework is designed around the following safeguards:

### Chronological discipline

Financial data must be split and evaluated chronologically. Random time mixing can introduce leakage and invalid backtesting conclusions.

### Point-in-time alignment

Ticker, date, filing date, macro date, and graph snapshot date must be aligned so that future information is not used for earlier predictions.

### Train-only fitting

Normalisers, PCA transformations, and fitted preprocessing objects should be trained on training splits only, then applied to validation and test splits.

### Schema validation

Downstream modules should fail loudly when expected columns are missing or stale schemas are detected.

### XAI preservation

Prediction files should preserve enough explanation metadata to support module-level and final-system audit.

### Risk-first fusion

The final Fusion Engine should not bypass position sizing, liquidity constraints, drawdown risk, contagion risk, or regime caps.

---

## Team

| Name | GitHub | Email |
|---|---|---|
| Ibrahim Hussain | [ib-hussain](https://github.com/ib-hussain) | <ibrahimbeaconarion@gmail.com> |
| Lubabah Moten | [lubabahmoten-dev](https://github.com/lubabahmoten-dev) | <lubabahmoten@gmail.com> |
| Sabeel Nadeem | [sabeelnadeem](https://github.com/sabeelnadeem) | <sabeelnadeem15@gmail.com> |

---

## Licence

This repository is licensed under the GNU General Public License v3.0. See [`LICENSE`](LICENSE).

---

## Disclaimer

This project is an academic and research implementation. It is not intended for live trading, portfolio management, investment advice, or automated financial decision-making without independent validation, risk review, regulatory review, and human oversight.
