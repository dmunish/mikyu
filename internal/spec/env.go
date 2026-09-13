package spec

import "fmt"

// ApplyEnv sets every queue-scoped parameter the lookup provides, outer layers winning; it reports the first value that fails to parse.
func ApplyEnv(s *Spec, lookup func(string) string) error {
	for _, p := range Params(LayerSpec) {
		val := lookup(p.EnvName)
		if val == "" {
			continue
		}
		v, err := p.Parse(val)
		if err != nil {
			return fmt.Errorf("%s: %w", p.EnvName, err)
		}
		p.Apply(s, v)
	}
	return nil
}

// ApplyEnvSession sets every session-layer parameter the lookup provides.
func ApplyEnvSession(sess *Session, lookup func(string) string) error {
	for _, p := range Params(LayerSession) {
		val := lookup(p.EnvName)
		if val == "" {
			continue
		}
		v, err := p.Parse(val)
		if err != nil {
			return fmt.Errorf("%s: %w", p.EnvName, err)
		}
		p.ApplySession(sess, v)
	}
	return nil
}
