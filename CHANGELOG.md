# Changelog

## [0.1.0-beta.0] - 2026-05-18

### Fixed
- `acquireTimeoutMs` pool option is no longer dead code — now used when no timeout argument passed
- NaN/Infinity in numeric options (`min`, `max`, `acquireTimeoutMs`, `idleTimeoutMs`) now throws
- `setInterval` idle timer now cleared on `drain()`; added `Symbol.dispose`/`Symbol.asyncDispose`
- O(n) idle connection scan replaced with O(1) `Set<PoolEntry>` lookup
- `release()` now updates `lastUsed` when passing connection directly to waiter
- `retries` parameter removed from public `acquire()` API (internalized)
- Idle check interval now adapts to `idleTimeoutMs` instead of hardcoded 30s
- `console.warn` replaced with `process.stderr.write()` (lint compliance)
- 11 new tests (28 total): NaN/Infinity, timeoutMs=0, double release, acquire after drain, dispose

## [0.1.0-alpha.0] - 2026-05-15
### Added
- Generic Pool<T> with create/destroy/validate factory
- Min/max connection limits
- Idle timeout with automatic cleanup
- Health check on acquire
- FIFO waiter queue with timeout
