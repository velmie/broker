// Package idempotency provides completion-marker middleware for Broker handlers
// backed by a caller-owned github.com/velmie/idempo engine and store.
//
// Concrete Handled results without an error commit an empty marker. Replays
// skip processing and request Handled. The native consumer owns settlement.
// Other results, errors and processing panics release the acquired lease with
// bounded cleanup. Commit failures keep the lease or marker by default.
//
// Business effects, marker persistence and settlement are separate operations.
// Lease expiry or a crash before persistence can repeat an effect. Broker
// keepalive does not extend the engine's lease. Logging adapters must preserve
// operations, fields and cause types while redacting application-specific data.
package idempotency
