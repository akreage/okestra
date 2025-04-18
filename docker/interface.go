package docker

import "context"

// interface for docker client
type DockerClientInterface interface {
	BuildImage(ctx context.Context, imageName string, dockerfile string) error
	RunContainer(ctx context.Context, imageName string, command []string) (string, error)
	StopContainer(ctx context.Context, containerID string) error
}

// interface for docker observer
