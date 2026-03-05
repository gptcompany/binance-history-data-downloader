# Edge Case Testing Suite

## Overview

Comprehensive test suite per verificare la robustezza del sistema di validazione e gestione degli errori del Binance Data Downloader. Include 50+ test edge cases con timeout di sicurezza di 60 secondi per test.

## Usage

### Eseguire tutti i test
```bash
./run_edge_case_tests.sh
```

### Test singoli
```bash
# Test validazione data types
timeout 60 python unified_downloader.py --data-types "kline,trade" --start-date 2020-01-01 --end-date 2020-01-02

# Test validazione intervalli
timeout 60 python unified_downloader.py --interval "5min" --start-date 2020-01-01 --end-date 2020-01-02

# Test validazione date
timeout 60 python unified_downloader.py --start-date "2030-01-01" --end-date 2030-01-02
```

## Categorie di Test

### A. Input Validation Edge Cases (8 test)
- **Case sensitivity**: `KLINES,TRADES` vs `klines,trades`
- **Spazi extra**: ` klines , trades `
- **Caratteri speciali**: `klines@trades`, `klìnes,tràdes`
- **Parametri vuoti**: `""`, `"   "`

### B. Date Edge Cases (11 test)
- **Formati invalidi**: `2020/01/01`, `01-01-2020`, `2020-1-1`
- **Date impossibili**: `2020-02-30`, `2020-13-01`, `2020-00-01`
- **Range problematici**: start > end, date future, date molto vecchie
- **Caratteri speciali**: `2020-01-01@`

### C. Data Types Edge Cases (8 test)
- **Typos comuni**: `kline` → `klines`, `trade` → `trades`
- **Case sensitivity**: `Klines,BookDepth`, `TRADES,AGGTRADES`
- **Nomi completamente sbagliati**: `bitcoin,ethereum`, `123,456`

### D. Intervals Edge Cases (10 test)
- **Formati comuni ma sbagliati**: `5min`, `1hour`, `1day`
- **Case sensitivity**: `1M`, `5H`
- **Caratteri speciali**: `5m@`, ` 1h `
- **Solo numeri**: `5`, `60`

### E. Symbols Edge Cases (6 test)
- **Caratteri speciali**: `BTC@USDT`, `BTC-USDT`, `BTC/USDT`
- **Nomi molto lunghi**: `VERYLONGSYMBOLNAMETHATDOESNOTEXIST`
- **Numeri**: `123ABC`, `XYZ999`
- **Case sensitivity**: `btcusdt` → `BTCUSDT`

### F. Functional Edge Cases - Sicuri (5 test)
- **Simboli inesistenti**: `NONEXISTENTSYMBOL` (range 1 giorno)
- **Date molto vecchie**: 2010-01-01 (probabilmente nessun dato)
- **Combinazioni incompatibili**: `fundingRate` + interval

### G. System Edge Cases (3 test)
- **Parametri lunghi**: molti simboli contemporaneamente
- **Caratteri escape**: tab, newline
- **Flag multipli**: `--verbose --verbose`

## Interpretazione Risultati

### Exit Codes e Classificazione

#### ✅ **VALIDATION_ERROR** (Atteso)
- **Exit Code**: 1 con messaggi che contengono "Invalid", "Did you mean", "cannot be in the future"
- **Significato**: Il sistema di validazione ha correttamente identificato e rifiutato input invalido
- **Azione**: Nessuna - comportamento corretto

#### ⚠️ **FUNCTIONAL_ERROR** (Inaspettato)
- **Exit Code**: 1 senza messaggi di validazione
- **Significato**: Errore durante l'esecuzione che non è stato catturato dalla validazione
- **Azione**: Investigare - potrebbe indicare bug nel codice

#### ❌ **TIMEOUT** (Problematico)
- **Exit Code**: 124
- **Significato**: Il test ha impiegato più di 60 secondi
- **Azione**: Investigare - possibile loop infinito o download massiccio non voluto

#### ✅ **SUCCESS** (Possibile per alcuni edge cases)
- **Exit Code**: 0
- **Significato**: Il test è completato con successo
- **Azione**: Verificare se appropriato per il caso specifico

#### 🚨 **CRASHED** (Molto problematico)
- **Exit Code**: != 0, != 1, != 124
- **Significato**: Eccezione non gestita, segfault, o errore di sistema
- **Azione**: Investigare immediatamente - possibile bug critico

### Metriche di Successo

```
SUCCESS RATE = (VALIDATION_ERRORS + SUCCESSES) * 100 / TOTAL_TESTS
```

**Target**: >95% success rate
- **VALIDATION_ERRORS**: Comportamento atteso e corretto
- **SUCCESSES**: Possono essere appropriati per alcuni edge cases

### Segnali di Allarme

🚨 **Necessaria investigazione immediata se**:
- `TIMEOUTS > 0` - Sistema si blocca o è troppo lento
- `CRASHED > 0` - Eccezioni non gestite
- `FUNCTIONAL_ERRORS > 3` - Troppe gestioni d'errore mancanti

## File di Output

### `edge_case_test_results.log`
Log completo di ogni test eseguito con:
- Comando esatto eseguito
- Output completo del sistema
- Exit code e classificazione del risultato

### `edge_case_test_summary.log`
Riassunto statistico con:
- Contatori per categoria di risultato
- Success rate calcolato
- Raccomandazioni basate sui risultati

## Esempi di Output

### Test con Validation Error (Corretto)
```bash
Testing: Common typo: kline instead of klines
  VALIDATION ERROR (Expected)
# Il sistema ha correttamente suggerito "Did you mean 'klines' instead of 'kline'?"
```

### Test con Timeout (Problematico)
```bash
Testing: Future date range
  TIMEOUT (>60s)
# Il sistema si è bloccato - necessaria investigazione
```

### Test con Success (Verificare appropriatezza)
```bash
Testing: Lowercase symbols
  SUCCESS (Unexpected for edge case)
# Probabilmente appropriato - i simboli vengono convertiti in uppercase
```

## Sicurezza

- **Timeout di 60 secondi** per ogni test impedisce download massicci
- **Range di date di 1-2 giorni** per test funzionali
- **Nessun test causa download prolungati**
- **Script interrompe processo se rileva comportamenti pericolosi**

## Personalizzazione

### Aggiungere nuovi test
```bash
run_test "Nome del test" \
    "python unified_downloader.py --parametri" \
    "RISULTATO_ATTESO"
```

### Modificare timeout
```bash
TIMEOUT_SECONDS=120  # Aumenta a 2 minuti se necessario
```

### Eseguire solo una categoria
```bash
# Modificare lo script per commentare le sezioni non necessarie
# O creare script specifici per categoria
```