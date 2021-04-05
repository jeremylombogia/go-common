package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChildLookup(t *testing.T) {
	obj := map[string]interface{}{
		"result": map[string]interface{}{
			"name": "sahal",
		},
	}

	name, ok := lookup("result.name", obj)
	assert.True(t, ok)
	assert.Equal(t, "sahal", name)
}

func TestSimpleLookup(t *testing.T) {
	obj := map[string]interface{}{
		"result": "sahal",
	}

	name, ok := lookup("result", obj)
	assert.True(t, ok)
	assert.Equal(t, "sahal", name)
}

func TestSliceLookup(t *testing.T) {
	obj := map[string]interface{}{
		"result": map[string]interface{}{
			"name":  "sahal",
			"roles": []string{"user", "member", "admin"},
			"data": []interface{}{
				map[string]interface{}{
					"age": 37,
				},
				map[string]interface{}{
					"location": "Jogja",
				},
			},
		},
	}

	o, ok := lookup("result.roles.0", obj)
	assert.True(t, ok)
	assert.Equal(t, "user", o)

	o, ok = lookup("result.data.0.age", obj)
	assert.True(t, ok)
	assert.Equal(t, 37, o)
}

func TestMatch(t *testing.T) {
	obj := map[string]interface{}{
		"result": map[string]interface{}{
			"name":  "sahal",
			"roles": []string{"user", "member", "admin"},
			"data": []interface{}{
				map[string]interface{}{
					"age": 37,
				},
				map[string]interface{}{
					"location": "Jogja",
				},
			},
		},
	}

	assert.True(t, match("result.name", obj, "sahal"))
	assert.True(t, match("result.roles._", obj, "user"))
	assert.True(t, match("result.data._.location", obj, "Jogja"))
	assert.True(t, match("result.data._.location", obj, "Jog.*"))
	assert.True(t, match("result.data._.age", obj, 37))

}
