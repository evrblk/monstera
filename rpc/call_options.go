package rpc

// ResponseMeta carries metadata about a completed RPC. It is populated by the
// generated stub after a successful round trip, into the destination
// supplied via WithResponseMeta.
type ResponseMeta struct {
	// Now is the request timestamp (Unix nanoseconds) the stub stamped on
	// this call (Request.now). It never travels over the wire as part of the
	// response — the stub already knows the value it generated, so it is
	// copied straight into ResponseMeta rather than round-tripped.
	Now int64
	// RaftLogIndex is the Raft log index the corresponding update committed
	// at, or 0 for reads. It is framework metadata that never passes through
	// the application core or the mrpc.Response envelope — the stub copies it
	// straight from monstera.Client's ClientResponse.
	RaftLogIndex uint64
}

// CallSettings collects the effect of the CallOptions applied to one
// ClientApi call. Generated stub code applies it directly; callers use the
// With* constructors below instead of touching it.
type CallSettings struct {
	IdempotencyToken string
	ResponseMeta     *ResponseMeta
}

// CallOption configures a single ClientApi call: supplying request-side
// values the stub would otherwise generate itself, or capturing response-side
// metadata into a caller-owned destination. It is additive — a call with no
// options behaves exactly as before.
type CallOption func(*CallSettings)

// WithIdempotencyToken supplies a token identifying this logical operation
// across retries. Unlike Now, which the stub is free to regenerate on every
// attempt, the token's whole purpose is to stay identical across retries of
// the same operation — so it must be chosen once by the caller, above any
// retry loop, never freshly generated per attempt inside the stub.
func WithIdempotencyToken(token string) CallOption {
	return func(s *CallSettings) {
		s.IdempotencyToken = token
	}
}

// WithResponseMeta captures server-observed metadata about the call into
// dst. dst is populated only after a successful round trip; it is left
// untouched if the call fails before a response is received.
func WithResponseMeta(dst *ResponseMeta) CallOption {
	return func(s *CallSettings) {
		s.ResponseMeta = dst
	}
}

// ApplyCallOptions folds opts into a fresh CallSettings. Exported for use by
// generated stub code.
func ApplyCallOptions(opts ...CallOption) *CallSettings {
	s := &CallSettings{}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
