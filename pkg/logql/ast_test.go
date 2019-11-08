package logql

import (
	"strings"
	"testing"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/stretchr/testify/assert"

	"github.com/prometheus/prometheus/pkg/labels"
)

func Test_logSelectorExpr_String(t *testing.T) {
	t.Parallel()
	tests := []string{
		`{foo!~"bar"}`,
		`{foo="bar", bar!="baz"}`,
		`{foo="bar", bar!="baz"} != "bip" !~ ".+bop"`,
		`{foo="bar"} |= "baz" |~ "blip" != "flip" !~ "flap"`,
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt, func(t *testing.T) {
			t.Parallel()
			expr, err := ParseLogSelector(tt)
			if err != nil {
				t.Fatalf("failed to parse log selector: %s", err)
			}
			if expr.String() != strings.Replace(tt, " ", "", -1) {
				t.Fatalf("error expected: %s got: %s", tt, expr.String())
			}
		})
	}
}

func Test_logSelectorExpr_String2(t *testing.T) {
	t.Parallel()

	testLogSelectorExprHelper(t, `{ foo !~ "bar" } / foo ="bar" /`, `{foo!~"bar"}/foo="bar"/`)
	testLogSelectorExprHelper(t, `/foo="bar", bar="baz"/{foo!~"bar"}`, `{foo!~"bar"}/foo="bar",bar="baz"/`)
	testLogSelectorExprHelper(t, `/ foo="bar", bar="baz" /`, `/foo="bar",bar="baz"/`)
}

func testLogSelectorExprHelper(t *testing.T, query, want string) {
	t.Helper()

	expr, err := ParseLogSelector(query)
	if err != nil {
		t.Fatalf("failed to parse log selector: %s", err)
	}

	if got := expr.String(); got != want {
		t.Fatalf("error expected: %s got: %s", want, got)
	}
}

type linecheck struct {
	l string
	e bool
}

func Test_FilterMatcher(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		q                   string
		expectedTagMatchers []*chunk.TagMatcher
		expectedMatchers    []*labels.Matcher
		// test line against the resulting filter, if empty filter should also be nil
		lines []linecheck
	}{
		{
			`{app="foo",cluster=~".+bar"}`,
			nil,
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "app", "foo"),
				mustNewMatcher(labels.MatchRegexp, "cluster", ".+bar"),
			},
			nil,
		},
		{
			`{app!="foo",cluster=~".+bar",bar!~".?boo"}`,
			nil,
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchNotEqual, "app", "foo"),
				mustNewMatcher(labels.MatchRegexp, "cluster", ".+bar"),
				mustNewMatcher(labels.MatchNotRegexp, "bar", ".?boo"),
			},
			nil,
		},
		{
			`{app="foo"} |= "foo"`,
			nil,
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foobar", true}, {"bar", false}},
		},
		{
			`{app="foo"} |= "foo" != "bar"`,
			nil,
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foobuzz", true}, {"bar", false}},
		},
		{
			`{app="foo"} |= "foo" !~ "f.*b"`,
			nil,
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foo", true}, {"bar", false}, {"foobar", false}},
		},
		{
			`{app="foo"} |= "foo" |~ "f.*b"`,
			nil,
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foo", false}, {"bar", false}, {"foobar", true}},
		},
		{
			`{app="foo"}/foo="bar",bar="baz"/ |= "foo"`,
			[]*chunk.TagMatcher{
				mustNewTagMatcher("foo", "bar"),
				mustNewTagMatcher("bar", "baz"),
			},
			[]*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foo", false}, {"bar", false}, {"barbaz", false}, {"foobarbaz", true}},
		},
	} {
		tt := tt
		t.Run(tt.q, func(t *testing.T) {
			t.Parallel()
			expr, err := ParseLogSelector(tt.q)
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedMatchers, expr.Matchers())
			assert.Equal(t, tt.expectedTagMatchers, expr.Tags())

			f, err := expr.Filter()
			assert.Nil(t, err)

			if tt.lines == nil {
				assert.Nil(t, f)
			} else {
				for _, lc := range tt.lines {
					assert.Equal(t, lc.e, f([]byte(lc.l)))
				}
			}
		})
	}
}
