# SignalDeck SDK

The **SignalDeck SDK** defines the public plugin API for SignalDeck.

It provides:

- Base classes for processors
- The `ApplicationContext` contract
- The `ValueProvider` registry
- Helper utilities and shared logic

The SDK contains **no web runtime implementation**.  
Concrete integrations (Flask app, rendering, storage, authentication) are implemented in `signaldeck-core`.

---

## Purpose

The SDK exists to:

- Define a stable contract for plugin development
- Separate runtime wiring from plugin logic
- Provide reusable base implementations (e.g. `DisplayProcessor`)
- Enable independent versioning of plugins and the core runtime

---

## Installation

```bash
pip install signaldeck-sdk
```
