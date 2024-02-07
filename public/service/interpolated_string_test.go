package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterpolatedString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expr     string
		msg      *Message
		expected any
	}{
		{
			name:     "content interpolation",
			expr:     `foo ${! content() } bar`,
			msg:      NewMessage([]byte("hello world")),
			expected: `foo hello world bar`,
		},
		{
			name:     "no interpolation",
			expr:     `foo bar`,
			msg:      NewMessage([]byte("hello world")),
			expected: `foo bar`,
		},
		{
			name: "metadata interpolation",
			expr: `foo ${! meta("var1") } bar`,
			msg: func() *Message {
				m := NewMessage([]byte("hello world"))
				m.MetaSet("var1", "value1")
				return m
			}(),
			expected: `foo value1 bar`,
		},
		{
			name:     "interpolation with integer result",
			expr:     `${! json("value") }`,
			msg:      NewMessage([]byte(`{"value": 666}`)),
			expected: int64(666),
		},
		{
			name:     "interpolation with float result",
			expr:     `${! json("value") }`,
			msg:      NewMessage([]byte(`{"value": 0.666}`)),
			expected: .666,
		},
		{
			name:     "interpolation with bool result",
			expr:     `${! json("value") }`,
			msg:      NewMessage([]byte(`{"value": true}`)),
			expected: true,
		},
		{
			name:     "interpolation with bytes result",
			expr:     `${! content() }`,
			msg:      NewMessage([]byte(`foobar`)),
			expected: []byte{0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72},
		},
		{
			name:     "interpolation with timestamp result",
			expr:     `${! json("value").ts_parse("2006-01-02") }`,
			msg:      NewMessage([]byte(`{"value": "2022-06-06"}`)),
			expected: time.Date(2022, time.June, 6, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "interpolation with null result",
			expr:     `${! json("value") }`,
			msg:      NewMessage([]byte(`{}`)),
			expected: nil,
		},
		{
			name:     "interpolation with array result",
			expr:     `${! json("value") }`,
			msg:      NewMessage([]byte(`{"value": [666, "foo", true]}`)),
			expected: []any{json.Number("666"), "foo", true},
		},
		{
			name:     "interpolation with object result",
			expr:     `${! json() }`,
			msg:      NewMessage([]byte(`{"value": [666, "foo", true]}`)),
			expected: map[string]any{"value": []any{json.Number("666"), "foo", true}},
		},
		{
			name:     "interpolation with multiple mixed type resolvers and string result",
			expr:     `foo ${! json("foo") } data ${! json("data") } length ${! json("data").length() }`,
			msg:      NewMessage([]byte(`{"foo": "bar", "data": [1, 2, 3]}`)),
			expected: `foo bar data [1,2,3] length 3`,
		},
	}

	for _, test := range tests {
		test := test

		var expectedString string
		switch exp := test.expected.(type) {
		case string:
			expectedString = exp
		case []byte:
			expectedString = string(exp)
		case time.Time:
			expectedString = exp.Format(time.RFC3339)
		default:
			expectedBytes, err := json.Marshal(test.expected)
			require.NoError(t, err)
			expectedString = string(expectedBytes)
		}

		t.Run("deprecated api/"+test.name, func(t *testing.T) {
			t.Parallel()

			i, err := NewInterpolatedString(test.expr)
			require.NoError(t, err)
			assert.Equal(t, expectedString, i.String(test.msg))
			assert.Equal(t, expectedString, string(i.Bytes(test.msg)))
		})

		t.Run("recommended api/"+test.name, func(t *testing.T) {
			t.Parallel()

			i, err := NewInterpolatedString(test.expr)
			require.NoError(t, err)

			{
				got, err := i.TryString(test.msg)
				require.NoError(t, err)

				assert.Equal(t, expectedString, got)
			}

			{
				got, err := i.TryBytes(test.msg)
				require.NoError(t, err)

				assert.Equal(t, expectedString, string(got))
			}

			{
				got, err := i.TryAny(test.msg)
				require.NoError(t, err)

				assert.Equal(t, test.expected, got)
			}
		})
	}
}

func TestInterpolatedStringCtor(t *testing.T) {
	t.Parallel()

	i, err := NewInterpolatedString(`foo ${! meta("var1")  bar`)

	assert.EqualError(t, err, "required: expected end of expression, got: bar")
	assert.Nil(t, i)
}

func TestInterpolatedStringMethods(t *testing.T) {
	t.Parallel()

	i, err := NewInterpolatedString(`foo ${! meta("var1") + 1 } bar`)
	require.NoError(t, err)

	m := NewMessage([]byte("hello world"))
	m.MetaSet("var1", "value1")

	{
		got, err := i.TryString(m)
		require.EqualError(t, err, "cannot add types string (from meta field var1) and number (from number literal)")
		require.Empty(t, got)
	}

	{
		got, err := i.TryBytes(m)
		require.EqualError(t, err, "cannot add types string (from meta field var1) and number (from number literal)")
		require.Empty(t, got)
	}
}
