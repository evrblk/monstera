package monstera

import (
	"context"
	"log/slog"
	"slices"
)

// CoreLogPolicy controls what a node's core diagnostic log keeps on the
// success path — see docs/core-implementation.md "Logging from a core" for
// the full design. It has no effect on the fatal path (an Update call that
// returned an error or panicked): that path always writes, unfiltered,
// because the exact dimensions this policy would otherwise cut noise on
// (which replica, live vs. replay) are precisely what an operator needs
// unfiltered when a node is about to crash. See appCoreAdapter.flushCoreLog.
type CoreLogPolicy struct {
	// MinLevel below which a line is dropped before it's queued. Set above
	// slog.LevelError to disable core-log emission for a core entirely on
	// the success path.
	MinLevel slog.Level

	// LeaderOnly, when true, keeps only lines produced while this replica's
	// raft state was Leader at emission time — collapsing the N-replica
	// duplication down to whichever one is currently leading, the closest
	// analogue to one line per logical operation.
	LeaderOnly bool

	// IncludeReplay, when false, drops lines tagged Replay=true — restart
	// log-tail replay and follower/rejoin catch-up. When true, replayed
	// lines are kept and clearly tagged, which is the point if you're
	// specifically debugging what a node does while catching up.
	IncludeReplay bool
}

// DefaultCoreLogPolicy is the recommended starting point for normal
// operation: one line per logical operation, only for genuinely new
// commits. Loosen either field for a specific debugging session.
var DefaultCoreLogPolicy = CoreLogPolicy{
	MinLevel:      slog.LevelInfo,
	LeaderOnly:    true,
	IncludeReplay: false,
}

// coreLogHandler is a slog.Handler that buffers records instead of writing
// them anywhere. Enabled always returns true: real filtering
// (CoreLogPolicy's MinLevel/LeaderOnly/IncludeReplay) happens once, in
// appCoreAdapter.flushCoreLog, after the owning Update call's outcome is
// known — not here, where that outcome isn't knowable yet (see the fatal
// path's bypass-everything behavior, which depends on seeing every line
// regardless of level).
type coreLogHandler struct {
	attrs  []slog.Attr
	groups []string
	lines  []slog.Record
}

func (h *coreLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *coreLogHandler) Handle(_ context.Context, r slog.Record) error {
	// Record.Clone() per slog's documented Handler contract: a Handler must
	// not retain a Record past the call to Handle without cloning it first.
	h.lines = append(h.lines, r.Clone())
	return nil
}

func (h *coreLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &coreLogHandler{attrs: append(slices.Clone(h.attrs), attrs...), groups: h.groups, lines: h.lines}
}

func (h *coreLogHandler) WithGroup(name string) slog.Handler {
	return &coreLogHandler{attrs: h.attrs, groups: append(slices.Clone(h.groups), name), lines: h.lines}
}

// newCoreLogger returns a *slog.Logger for one ApplicationCore.Update call,
// backed by a coreLogHandler that only buffers. The caller (appCoreAdapter)
// keeps the returned handler and reads h.lines back after Update returns —
// by ordinary Go reference semantics this works whether Update returned
// normally, returned an error, or panicked and was recovered, with no
// special-casing needed for any of the three (see callCoreUpdate).
func newCoreLogger() (*slog.Logger, *coreLogHandler) {
	h := &coreLogHandler{}
	return slog.New(h), h
}

// filterByLevel returns the records at or above min, preserving order.
func filterByLevel(records []slog.Record, min slog.Level) []slog.Record {
	kept := make([]slog.Record, 0, len(records))
	for _, r := range records {
		if r.Level >= min {
			kept = append(kept, r)
		}
	}
	return kept
}

// coreLogBatch is one flushCoreLog call's worth of records, plus the tags
// (node/application/shard/replica/leader/replay/index/fatal — see
// appCoreAdapter.flushCoreLog) they should be written with.
type coreLogBatch struct {
	tags    []slog.Attr
	records []slog.Record
}

// writeCoreLogBatch replays a batch's records into dest, tagged with the
// batch's attrs. Used both for the synchronous fatal-path write and by the
// success-path queue's drain goroutine below — the only difference between
// the two is scheduling, not this replay step.
func writeCoreLogBatch(dest slog.Handler, batch coreLogBatch) {
	if len(batch.records) == 0 {
		return
	}
	h := dest.WithAttrs(batch.tags)
	for _, r := range batch.records {
		_ = h.Handle(context.Background(), r)
	}
}

// coreLogQueue is a small, bounded, non-blocking per-node queue draining
// buffered core-log batches into a real destination slog.Handler on a
// background goroutine. Apply must never block on log I/O — the same
// requirement notes/monstera/events-design.md places on the Events bus — so
// a full queue drops the batch and counts it (coreLogDropped) rather than
// blocking the raft commit that produced it.
type coreLogQueue struct {
	dest    slog.Handler
	batches chan coreLogBatch
	done    chan struct{}
}

const defaultCoreLogQueueSize = 256

func newCoreLogQueue(dest slog.Handler, size int) *coreLogQueue {
	if dest == nil {
		dest = discardHandler{}
	}
	if size <= 0 {
		size = defaultCoreLogQueueSize
	}
	q := &coreLogQueue{
		dest:    dest,
		batches: make(chan coreLogBatch, size),
		done:    make(chan struct{}),
	}
	go q.run()
	return q
}

func (q *coreLogQueue) run() {
	defer close(q.done)
	for batch := range q.batches {
		writeCoreLogBatch(q.dest, batch)
	}
}

// enqueueNonBlocking never blocks: on a full queue it drops the batch and
// increments coreLogDropped instead. tags carries enough of the batch's own
// identity (node/application/shard/replica) for the metric label; the rest
// of the drop is otherwise silent by design (§ never blocks Apply).
func (q *coreLogQueue) enqueueNonBlocking(tags []slog.Attr, records []slog.Record) {
	if len(records) == 0 {
		return
	}
	select {
	case q.batches <- coreLogBatch{tags: tags, records: records}:
	default:
		coreLogDropped.WithLabelValues(attrString(tags, "node_id"), attrString(tags, "application_name")).Inc()
	}
}

// close stops accepting new batches and waits for the drain goroutine to
// finish writing whatever it already had queued.
func (q *coreLogQueue) close() {
	close(q.batches)
	<-q.done
}

func attrString(tags []slog.Attr, key string) string {
	for _, a := range tags {
		if a.Key == key {
			return a.Value.String()
		}
	}
	return ""
}

// discardHandler is the core-log destination when a node is configured
// without one (NodeConfig.CoreLogDestination == nil): the buffering
// mechanism above is always active internally, so a core author's
// log.Warn(...) call never panics on a nil logger, but nothing is written
// anywhere until an operator wires up a real destination handler.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (discardHandler) WithAttrs([]slog.Attr) slog.Handler        { return discardHandler{} }
func (discardHandler) WithGroup(string) slog.Handler             { return discardHandler{} }
