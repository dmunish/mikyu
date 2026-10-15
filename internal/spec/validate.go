package spec

import (
	"fmt"
	"net"
	"time"
)

// ValidateCreate checks a proposed spec.
func ValidateCreate(s Spec) error { return validate(s) }

// ValidateUpdate rejects a patch that sets a create-only field, naming it, then validates the fields the patch does set.
func ValidateUpdate(patch Spec) error {
	for _, p := range Params(LayerSpec) {
		if p.Mutability != MutCreateOnly {
			continue
		}
		if _, set := patch.Field(p); set {
			return errImmutable(p.Key)
		}
	}
	return validate(patch)
}

// ValidateSession checks the process-level parameters: the tick interval must be positive when metrics are enabled, the history depth is bounded away from zero, and the listen address, when set, is a host:port.
func ValidateSession(s Session) error {
	if s.MaxConnections < 1 {
		return errInvalid("max_connections", ruleAtLeastOne, s.MaxConnections)
	}
	if s.Metrics.Enabled && s.Metrics.Interval <= 0 {
		return errInvalid("metrics_interval", rulePositive, s.Metrics.Interval)
	}
	if s.Metrics.History < 1 {
		return errInvalid("metrics_history", ruleAtLeastOne, s.Metrics.History)
	}
	if s.Metrics.Listen != "" {
		if _, _, err := net.SplitHostPort(s.Metrics.Listen); err != nil {
			return fmt.Errorf("%w: metrics_listen must be host:port, got %q", ErrInvalidConfig, s.Metrics.Listen)
		}
	}
	switch s.Auth.Method {
	case "", "none", "none-insecure", "token", "mtls":
	default:
		return errInvalid("auth_method", "one of none|none-insecure|token|mtls", s.Auth.Method)
	}
	if s.Auth.Method == "token" && s.Auth.Token == "" && s.Auth.TokenFile == "" {
		return errInvalid("auth_token_file", "be set when auth_method=token", "")
	}
	if s.Auth.Token != "" && len(s.Auth.Token) < 16 {
		return errInvalid("auth_token", "be at least 16 bytes", "")
	}
	if s.Auth.Method == "mtls" && !s.TLS.Enabled {
		return errInvalid("tls_enabled", "be true when auth_method=mtls", s.TLS.Enabled)
	}
	return nil
}

func validate(s Spec) error {
	st := s.Storage
	if err := positiveBytes("max_segment", st.MaxSegment); err != nil {
		return err
	}
	if err := positiveDur("max_segment_age", st.MaxSegmentAge); err != nil {
		return err
	}
	if err := positiveBytes("max_payload", st.MaxPayload); err != nil {
		return err
	}
	if err := positiveBytes("max_cache", st.MaxCache); err != nil {
		return err
	}
	if st.Durability != nil && !st.Durability.Valid() {
		return errInvalid("durability", "one of sync|async", *st.Durability)
	}
	if st.Encryption != nil && !st.Encryption.Valid() {
		return errInvalid("encryption", "one of none|os|aes", *st.Encryption)
	}
	if st.Compaction != nil && !st.Compaction.Valid() {
		return errInvalid("compaction", "one of none|keys", *st.Compaction)
	}
	if err := positiveBytes("max_flush_bytes", s.Flush.MaxBytes); err != nil {
		return err
	}
	if s.Flush.MaxWait != nil {
		if *s.Flush.MaxWait <= 0 {
			return errInvalid("max_flush_wait", rulePositive, *s.Flush.MaxWait)
		}
		if *s.Flush.MaxWait < Duration(100*time.Microsecond) {
			return errInvalid("max_flush_wait", "at least 100µs", *s.Flush.MaxWait)
		}
	}
	if err := nonNegativeDur("max_retention_age", s.Retention.MaxAge); err != nil {
		return err
	}
	if err := nonNegativeBytes("max_retention_bytes", s.Retention.MaxBytes); err != nil {
		return err
	}
	if err := positiveDur("lease", s.Task.Lease); err != nil {
		return err
	}
	if err := positiveU32("max_attempts", s.Task.MaxAttempts); err != nil {
		return err
	}
	if err := positiveU32("max_in_flight", s.Task.MaxInFlight); err != nil {
		return err
	}
	if err := positiveU32("max_dead_letters", s.Task.MaxDeadLetters); err != nil {
		return err
	}
	return validatePairs(s)
}

// validatePairs rejects a conflict between two fields the user set explicitly; a derived field never conflicts with its source.
func validatePairs(s Spec) error {
	st := s.Storage
	if st.MaxPayload != nil && st.MaxSegment != nil && *st.MaxPayload > *st.MaxSegment-RecordOverhead {
		return fmt.Errorf("%w: max_payload (%d) exceeds max_segment (%d) minus the %d-byte record overhead",
			ErrInvalidConfig, *st.MaxPayload, *st.MaxSegment, RecordOverhead)
	}
	if st.MaxCache != nil && st.MaxSegment != nil {
		// The cache must at least hold the smaller of a full segment and the
		// built-in budget, so the derived default never conflicts.
		if floor := min(builtinMaxCache, *st.MaxSegment); *st.MaxCache < floor {
			return fmt.Errorf("%w: max_cache (%d) is smaller than max_segment (%d)",
				ErrInvalidConfig, *st.MaxCache, *st.MaxSegment)
		}
	}
	return nil
}

func positiveBytes(key string, p *int64) error {
	if p != nil && *p <= 0 {
		return errInvalid(key, rulePositive, *p)
	}
	return nil
}

func positiveDur(key string, p *Duration) error {
	if p != nil && *p <= 0 {
		return errInvalid(key, rulePositive, *p)
	}
	return nil
}

func positiveU32(key string, p *uint32) error {
	if p != nil && *p == 0 {
		return errInvalid(key, rulePositive, *p)
	}
	return nil
}

func nonNegativeDur(key string, p *Duration) error {
	if p != nil && *p < 0 {
		return errInvalid(key, ruleNonNegative, *p)
	}
	return nil
}

func nonNegativeBytes(key string, p *int64) error {
	if p != nil && *p < 0 {
		return errInvalid(key, ruleNonNegative, *p)
	}
	return nil
}
