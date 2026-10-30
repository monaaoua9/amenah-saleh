package filter

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/monaaoua9/streamdb/internal/event"
)

// Grammar:
//
//	expr  = and { "||" and }
//	and   = not { "&&" not }
//	not   = "!" not | "(" expr ")" | cmp
//	cmp   = field op literal
//	field = key | value | time
//	op    = == != < <= > >=    (numbers)   == != ~ !~   (key: ~ is "contains")
type Filter func(event.Event) bool

func Parse(src string) (Filter, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	f, err := p.or()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.toks) {
		return nil, fmt.Errorf("filter: unexpected %q", p.toks[p.pos].s)
	}
	return f, nil
}

type tok struct {
	kind byte // 'i' ident, 'n' number, 's' string, 'o' operator/punct
	s    string
}

func lex(src string) ([]tok, error) {
	var out []tok
	rs := []rune(src)
	for i := 0; i < len(rs); {
		r := rs[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case unicode.IsLetter(r):
			j := i
			for j < len(rs) && (unicode.IsLetter(rs[j]) || unicode.IsDigit(rs[j])) {
				j++
			}
			out = append(out, tok{'i', string(rs[i:j])})
			i = j
		case unicode.IsDigit(r) || r == '-' || r == '.':
			j := i + 1
			for j < len(rs) && (unicode.IsDigit(rs[j]) || strings.ContainsRune(".eE+-", rs[j])) {
				j++
			}
			out = append(out, tok{'n', string(rs[i:j])})
			i = j
		case r == '"' || r == '\'':
			j := i + 1
			for j < len(rs) && rs[j] != r {
				j++
			}
			if j == len(rs) {
				return nil, fmt.Errorf("filter: unterminated string")
			}
			out = append(out, tok{'s', string(rs[i+1 : j])})
			i = j + 1
		default:
			two := ""
			if i+1 < len(rs) {
				two = string(rs[i : i+2])
			}
			switch two {
			case "&&", "||", "==", "!=", "<=", ">=", "!~":
				out = append(out, tok{'o', two})
				i += 2
				continue
			}
			if strings.ContainsRune("()!<>~", r) {
				out = append(out, tok{'o', string(r)})
				i++
				continue
			}
			return nil, fmt.Errorf("filter: bad character %q", r)
		}
	}
	return out, nil
}

type parser struct {
	toks []tok
	pos  int
}

func (p *parser) peek() tok {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return tok{}
}

func (p *parser) accept(op string) bool {
	if t := p.peek(); t.kind == 'o' && t.s == op {
		p.pos++
		return true
	}
	return false
}

func (p *parser) or() (Filter, error) {
	l, err := p.and()
	if err != nil {
		return nil, err
	}
	for p.accept("||") {
		r, err := p.and()
		if err != nil {
			return nil, err
		}
		a, b := l, r
		l = func(e event.Event) bool { return a(e) || b(e) }
	}
	return l, nil
}

func (p *parser) and() (Filter, error) {
	l, err := p.not()
	if err != nil {
		return nil, err
	}
	for p.accept("&&") {
		r, err := p.not()
		if err != nil {
			return nil, err
		}
		a, b := l, r
		l = func(e event.Event) bool { return a(e) && b(e) }
	}
	return l, nil
}

func (p *parser) not() (Filter, error) {
	if p.accept("!") {
		f, err := p.not()
		if err != nil {
			return nil, err
		}
		return func(e event.Event) bool { return !f(e) }, nil
	}
	if p.accept("(") {
		f, err := p.or()
		if err != nil {
			return nil, err
		}
		if !p.accept(")") {
			return nil, fmt.Errorf("filter: missing )")
		}
		return f, nil
	}
	return p.cmp()
}

func (p *parser) cmp() (Filter, error) {
	f := p.peek()
	if f.kind != 'i' {
		return nil, fmt.Errorf("filter: expected key, value or time, got %q", f.s)
	}
	p.pos++
	op := p.peek()
	if op.kind != 'o' {
		return nil, fmt.Errorf("filter: expected an operator after %s", f.s)
	}
	p.pos++
	lit := p.peek()
	p.pos++

	switch f.s {
	case "key":
		if lit.kind != 's' {
			return nil, fmt.Errorf("filter: key compares with a quoted string")
		}
		s := lit.s
		switch op.s {
		case "==":
			return func(e event.Event) bool { return e.Key == s }, nil
		case "!=":
			return func(e event.Event) bool { return e.Key != s }, nil
		case "~":
			return func(e event.Event) bool { return strings.Contains(e.Key, s) }, nil
		case "!~":
			return func(e event.Event) bool { return !strings.Contains(e.Key, s) }, nil
		}
	case "value", "time":
		if lit.kind != 'n' {
			return nil, fmt.Errorf("filter: %s compares with a number", f.s)
		}
		n, err := strconv.ParseFloat(lit.s, 64)
		if err != nil {
			return nil, fmt.Errorf("filter: bad number %q", lit.s)
		}
		get := func(e event.Event) float64 { return e.Value }
		if f.s == "time" {
			get = func(e event.Event) float64 { return float64(e.Time) }
		}
		switch op.s {
		case "==":
			return func(e event.Event) bool { return get(e) == n }, nil
		case "!=":
			return func(e event.Event) bool { return get(e) != n }, nil
		case "<":
			return func(e event.Event) bool { return get(e) < n }, nil
		case "<=":
			return func(e event.Event) bool { return get(e) <= n }, nil
		case ">":
			return func(e event.Event) bool { return get(e) > n }, nil
		case ">=":
			return func(e event.Event) bool { return get(e) >= n }, nil
		}
	default:
		return nil, fmt.Errorf("filter: unknown field %q", f.s)
	}
	return nil, fmt.Errorf("filter: %q does not apply to %s", op.s, f.s)
}
