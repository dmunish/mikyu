package mikyu

import "github.com/dmunish/mikyu/internal/client"

func Open(dir string, opts ...ClientOption) (Client, error) {
	c, err := client.NewEmbedded(dir, opts...)
	if err != nil {
		return nil, err
	}
	return newClient(c), nil
}

func Dial(target string, opts ...ClientOption) (Client, error) {
	c, err := client.NewRemote(target, opts...)
	if err != nil {
		return nil, err
	}
	return newClient(c), nil
}
