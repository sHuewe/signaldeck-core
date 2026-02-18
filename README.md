# SignalDeck Core

**SignalDeck Core** is the runtime component of the SignalDeck framework.

It provides:

- The Flask application runtime
- Configuration loading
- Processor orchestration
- Plugin discovery and blueprint registration
- CLI entrypoint (`signaldeck`)
- HTTP endpoints and page composition

It does **not** contain UI templates or processor implementations.

---

## Architecture Overview

SignalDeck consists of multiple layers:

Instance Config
↓
signaldeck-core
↓
signaldeck-sdk
↓
Plugins
↓
signaldeck-ui


- **signaldeck-core** → Runtime and orchestration  
- **signaldeck-sdk** → Processor contracts and base classes  
- **signaldeck-ui** → Layout, shared macros, static assets  
- **Plugins** → Processor implementations + component templates  
- **Instance repo** → Private configuration and deployment  

---

## Installation

```bash
pip install signaldeck-core
```

Entwicklung:
 ```
 py -m pip install -e . --config-settings editable_mode=compat
 ```


## Run application
```
signaldeck run --config config.json [--host 0.0.0.0] [--port 5000] [--debug] [--no-collect-data]
```

## Validate
```
signaldeck validate-config --config config/haus_demo.json
```


## List plugins
```
signaldeck list-plugins --config config/haus_demo.json

```