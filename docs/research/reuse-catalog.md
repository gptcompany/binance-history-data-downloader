# F-649 reuse decisions (2026-11-10)

| Source | License / language | Coverage | Decision |
|---|---|---|---|
| Python standard library | PSF / Python | argv, paths, deterministic text | Reuse directly |
| python-jsonschema/jsonschema | MIT / Python | Draft 2020-12 closed inventory schema | Reuse maintained library; no custom schema engine |
| gptcompany/bybit_historical_data_download PR #7/#8 | license not exposed (GitHub license API 404) / Python | cold systemd ownership, digest, drift pattern | Design reference only; no source copied |

- Runner correction: B5 explicitly requested adaptation of the approved
  `gptcompany/bybit_historical_data_download` `scripts/cold_deploy.py` at
  `92d51df` (main): byte capture and phase/counts-status reporting at the
  container boundary. Binance keeps its strict JSON schema and planner,
  snapshots disable bytecode before imports, install rejects dry-run.
