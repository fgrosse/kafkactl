package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfiguration(t *testing.T) {
	input := `
api_version: v1
current_context: staging
previous_context: prod
contexts:
  - name: staging
    brokers:
      - 10.255.4.206:9092
      - 10.255.5.193:9092
      - 10.255.6.4:9092
  - name: prod
    brokers:
      - my-kafka.a.example.com:9092
      - my-kafka.b.example.com:9092
      - my-kafka.c.example.com:9092
  - name: localhost
    brokers:
      - localhost:9092
`

	expected := Configuration{
		APIVersion:      "v1",
		CurrentContext:  "staging",
		PreviousContext: "prod",
		Contexts: []ContextConfiguration{
			{
				Name:    "staging",
				Brokers: []string{"10.255.4.206:9092", "10.255.5.193:9092", "10.255.6.4:9092"},
			},
			{
				Name:    "prod",
				Brokers: []string{"my-kafka.a.example.com:9092", "my-kafka.b.example.com:9092", "my-kafka.c.example.com:9092"},
			},
			{
				Name:    "localhost",
				Brokers: []string{"localhost:9092"},
			},
		},
	}

	r := strings.NewReader(input)
	actual, err := LoadConfiguration(r)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestLoadConfiguration_UnknownFields(t *testing.T) {
	input := `
foo: bar
bar: baz
`

	r := strings.NewReader(input)
	_, err := LoadConfiguration(r)
	assert.EqualError(t, err, "yaml: unmarshal errors:\n  line 2: field foo not found in type cmd.Configuration\n  line 3: field bar not found in type cmd.Configuration")
}

func TestSaveConfiguration(t *testing.T) {
	conf := Configuration{
		APIVersion:      "v1",
		CurrentContext:  "staging",
		PreviousContext: "prod",
		Contexts: []ContextConfiguration{
			{
				Name:    "staging",
				Brokers: []string{"10.255.4.206:9092", "10.255.5.193:9092", "10.255.6.4:9092"},
			},
			{
				Name:    "prod",
				Brokers: []string{"my-kafka.a.example.com:9092", "my-kafka.b.example.com:9092", "my-kafka.c.example.com:9092"},
			},
			{
				Name:    "localhost",
				Brokers: []string{"localhost:9092"},
			},
		},
	}

	w := new(bytes.Buffer)
	err := SaveConfiguration(w, conf)
	require.NoError(t, err)

	expected := `
api_version: v1
current_context: staging
previous_context: prod
contexts:
  - name: staging
    brokers:
      - 10.255.4.206:9092
      - 10.255.5.193:9092
      - 10.255.6.4:9092
  - name: prod
    brokers:
      - my-kafka.a.example.com:9092
      - my-kafka.b.example.com:9092
      - my-kafka.c.example.com:9092
  - name: localhost
    brokers:
      - localhost:9092
`

	assert.YAMLEq(t, expected, w.String())
}

func TestConfiguration_GetContext(t *testing.T) {
	c := NewConfiguration()

	actual, err := c.GetContext("test")
	assert.EqualError(t, err, `there is no context called "test"`)

	expected := ContextConfiguration{
		Name: "test",
		Brokers: []string{
			"broker1.example.com:9092",
			"broker2.example.com:9092",
			"broker3.example.com:9092",
		},
	}

	c.Contexts = append(c.Contexts, expected)

	actual, err = c.GetContext("test")
	require.NoError(t, err)

	assert.Equal(t, expected, actual)
}

func TestConfiguration_AddContext(t *testing.T) {
	c := NewConfiguration()
	err := c.AddContext("test", "broker1.example.com:9092", "broker2.example.com:9092", "broker3.example.com:9092")
	require.NoError(t, err)

	expected := ContextConfiguration{
		Name: "test",
		Brokers: []string{
			"broker1.example.com:9092",
			"broker2.example.com:9092",
			"broker3.example.com:9092",
		},
	}

	require.Len(t, c.Contexts, 1)
	assert.Equal(t, expected, c.Contexts[0])
}

func TestConfiguration_AddContext_Duplicates(t *testing.T) {
	c := NewConfiguration()
	err := c.AddContext("test", "broker1.example.com:9092", "broker2.example.com:9092", "broker3.example.com:9092")
	require.NoError(t, err)

	err = c.AddContext("test", "broker1.example.com:9092", "broker2.example.com:9092", "broker3.example.com:9092")
	assert.EqualError(t, err, `there is already a context named "test"`)
	assert.Len(t, c.Contexts, 1)
}

func TestConfiguration_AddContext_SetCurrentContextIfEmpty(t *testing.T) {
	c := NewConfiguration()
	require.Empty(t, c.CurrentContext)
	err := c.AddContext("test", "broker1.example.com:9092", "broker2.example.com:9092", "broker3.example.com:9092")
	require.NoError(t, err)

	assert.Equal(t, "test", c.CurrentContext)
}

func TestConfiguration_SetContext(t *testing.T) {
	conf := Configuration{
		APIVersion:      "v1",
		CurrentContext:  "localhost",
		PreviousContext: "staging",
		Contexts: []ContextConfiguration{
			{Name: "prod", Brokers: []string{"example.com"}},
			{Name: "staging", Brokers: []string{"staging.example.com"}},
			{Name: "localhost", Brokers: []string{"localhost:9092"}},
		},
	}

	cases := map[string]struct {
		contextName  string
		err          string
		expectedPrev string
		expectedCurr string
	}{
		"change": {
			contextName:  "prod",
			expectedPrev: "localhost",
			expectedCurr: "prod",
		},
		"no-change": {
			contextName:  "localhost",
			expectedPrev: "staging",
			expectedCurr: "localhost",
		},
		"unknown": {
			contextName:  "foo",
			err:          `there is no context named "foo"`,
			expectedPrev: "staging",
			expectedCurr: "localhost",
		},
		"dash": {
			contextName:  "-",
			expectedPrev: "localhost",
			expectedCurr: "staging",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			cpy := conf
			err := cpy.SetContext(c.contextName)
			if c.err == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, c.err)
			}

			assert.Equal(t, c.expectedPrev, cpy.PreviousContext)
			assert.Equal(t, c.expectedCurr, cpy.CurrentContext)
		})
	}
}

func TestConfiguration_SetContext_DashButPreviousIsEmpty(t *testing.T) {
	conf := Configuration{
		APIVersion:      "v1",
		CurrentContext:  "staging",
		PreviousContext: "",
		Contexts: []ContextConfiguration{
			{Name: "staging", Brokers: []string{"staging.example.com"}},
			{Name: "localhost", Brokers: []string{"localhost:9092"}},
		},
	}

	// Previous is empty but we only have two contexts. This will be the case
	// when both contexts have just been created and the user tries to switch
	// easily between the first created context and the new one.

	err := conf.SetContext("-")
	require.NoError(t, err)

	assert.Equal(t, "localhost", conf.CurrentContext)
	assert.Equal(t, "staging", conf.PreviousContext)
}

func TestConfiguration_SetContext_DashButPreviousIsEmpty2(t *testing.T) {
	conf := Configuration{
		APIVersion:      "v1",
		CurrentContext:  "staging",
		PreviousContext: "",
		Contexts: []ContextConfiguration{
			{Name: "staging", Brokers: []string{"staging.example.com"}},
			{Name: "prod", Brokers: []string{"staging.example.com"}},
			{Name: "localhost", Brokers: []string{"localhost:9092"}},
		},
	}

	// Previous is empty and we have more than two contexts. In this scenario it
	// is unclear what the user wants to do and thus we opt to return an error.

	err := conf.SetContext("-")
	require.EqualError(t, err, "cannot toggle context: no previous context was saved in configuration file")

	assert.Equal(t, "staging", conf.CurrentContext)
	assert.Equal(t, "", conf.PreviousContext)
}
