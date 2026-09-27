<h1 align="center">fin-glassbox</h1>
<h2 align="center">An Explainable Multimodal Neural Framework for Financial Risk Management</h2>

A research-oriented financial AI system for building risk-aware, explainable market decision pipelines from multiple financial data modalities. The framework combines temporal market encoders, FinBERT-based text encoders, graph neural risk modelling, classical financial risk measures, trained analyst modules, interpretable position sizing, and hybrid fusion with explicit XAI traces.

The repository is designed around one central idea: financial decisions should not come from one opaque monolithic model. Instead, the system decomposes the financial decision problem into specialised modules, lets each module model one part of market risk or market context, and then fuses those outputs through a transparent, risk-constrained decision layer.

> **Research scope:** This repository is for academic research, experimentation, and explainable AI system design. It is not financial advice, trading advice, or an investment product.


The framework is organised around four active data families.

## Financial Text Data

Used by FinBERT, Sentiment Analyst, News Analyst, and Qualitative Analyst.

Typical fields include:

- filing or event date,
- ticker or CIK/entity mapping,
- form type,
- source section,
- document/chunk identifiers,
- text embeddings,
- sentiment and event-level predictions.



## Licence

This repository is licensed under the GNU General Public License v3.0. See [`LICENSE`](LICENSE).

---

## Disclaimer

This project is an academic and research implementation. It is not intended for live trading, portfolio management, investment advice, or automated financial decision-making without independent validation, risk review, regulatory review, and human oversight.
