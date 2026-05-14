// Package proof defines the versioned proof records carried by Peras metadata
// programs after holder admission.
//
// The package owns stable schema ids, rule ids, digest construction, and local
// verifiers for predicate and guard evidence. These records are holder/frontier
// evidence used by Peras admission, segment completion, and replay validation;
// they are not storage-engine cryptographic range proofs.
package proof
