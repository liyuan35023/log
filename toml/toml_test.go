package toml

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

var (
	CONFIG_PATH = "./example.toml"
)

func TestToml(t *testing.T) {

	configSet := NewConfigSet("example setting", ExitOnError)

	boolVal := configSet.Bool("datanode.flag", false)
	strVal := configSet.String("datanode.ip", "")
	globalVal := configSet.String("global", "")
	intVal := configSet.Int("datanode.port", 0)
	noVal := configSet.String("datanode.fake", "test")

	err := configSet.Parse(CONFIG_PATH)
	assert.NoError(t, err)
	assert.Equal(t, "global-val", *globalVal)
	assert.Equal(t, true, *boolVal)
	assert.Equal(t, "127.0.0.1", *strVal)
	assert.Equal(t, 2333, *intVal)
	assert.Equal(t, "test", *noVal)
}
