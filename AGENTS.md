# Data Product Framework - project context

## Context authority

This file owns runtime-neutral project context. Provider-specific files import it and contain mechanics only.

Read `README.md` before changing the framework. Keep project facts in their existing authoritative document.

## Project boundary

This public repository is an early Data Product Framework surface. It models sources, transformations, sinks, and pipelines from YAML configuration.

The active factory has evolved beyond this early surface. Preserve this repository as a coherent public version and do not present it as the current Ergasterion implementation.

## Change rules

- Preserve configuration as the source for runtime assembly.
- Avoid adding vendor-specific assumptions to the core abstractions.
- Keep examples public-safe and free of private paths or operational state.
- Update `README.md` when the public API or supported flow changes.
- For instruction-only changes, verify the import chain, historical scope, and public-safety boundary.

## Repository relationship

The private `data-product-framework` repository is the historical source associated with this public version. Current factory development lives in the Ergasterion source and public projection.
