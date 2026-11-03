---
language:
- fr
license: etalab-2.0
tags:
- energy
- electricity
- rte
---

# Consommation électrique en France — RTE

Pipeline Python : API publique éCO2mix → Excel brut + JSON immuable →
transformation → Parquet → SQLite → Excel relu depuis SQL → Hub Hugging Face.

Source : [RTE / ODRÉ, éCO2mix national temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/).
Les puissances sont exprimées en MW. Les données temps réel sont provisoires.
Réutilisation sous Licence Ouverte 2.0 avec attribution à RTE.

## Installation

Python 3.11 ou plus récent.

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

Les exports, la base locale et les secrets sont exclus de Git.
Le fichier `app.py` d'origine est conservé dans le premier commit du dépôt.
