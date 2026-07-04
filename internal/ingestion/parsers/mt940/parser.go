// Package mt940 parses SWIFT MT940 customer statement messages
// (plans/task/core/07) into an intermediate representation whose field
// names (field_61.*, field_86.*) match exactly what
// plans/docs/02-data-ingestion.md §3.2's mapping DSL example expects -
// see this task's Implementation Notes on why that exact naming is the
// most important cross-task contract in this pair of tasks. Field
// mapping/normalization/sign-application themselves are
// plans/task/core/08's job, not this package's.
package mt940

import (
	"fmt"
	"regexp"
	"strings"
)

// Balance mirrors a :60F:/:60M:/:62F:/:62M: field.
type Balance struct {
	DebitCreditMark string // "D" or "C"
	Date            string // YYMMDD, raw
	Currency        string
	Amount          string // raw digits with a comma decimal separator, as SWIFT encodes it
}

// Field61 mirrors one :61: statement line, field-named to match
// plans/docs/02-data-ingestion.md §3.2's mapping DSL example exactly
// (field_61.amount, field_61.debit_credit_mark, field_61.currency,
// field_61.value_date). Currency is not actually encoded in a real :61:
// line (SWIFT states it once per statement, in :60F:/:62F:) - it is
// carried over from the statement's opening balance so the exact
// cross-task field contract still holds for every transaction line.
type Field61 struct {
	ValueDate       string `json:"value_date"`        // YYMMDD
	EntryDate       string `json:"entry_date"`        // MMDD, optional
	DebitCreditMark string `json:"debit_credit_mark"` // "D", "C", "RD", "RC" - surfaced raw, sign application is task 08's job
	Amount          string `json:"amount"`            // raw digits with comma decimal separator
	Currency        string `json:"currency"`          // carried over from the statement's balance currency
	TransactionType string `json:"transaction_type"`  // e.g. "NMSC", "NTRF" - optional
	CustomerRef     string `json:"customer_ref"`
	BankRef         string `json:"bank_ref"` // after "//", optional
}

// Field86 mirrors one :86: information block.
type Field86 struct {
	Narrative string `json:"narrative"`
}

// TransactionLine pairs one :61: line with its following :86: narrative
// (SWIFT convention: :86: always immediately follows the :61: it
// describes).
type TransactionLine struct {
	Field61 Field61
	Field86 Field86
}

// Statement is the full parsed MT940 message.
type Statement struct {
	TransactionRefNo string // :20:
	AccountID        string // :25:
	StatementNumber  string // :28C:
	OpeningBalance   Balance
	ClosingBalance   Balance
	Lines            []TransactionLine
}

var field61Pattern = regexp.MustCompile(
	`^(\d{6})(\d{4})?(RD|RC|D|C)(\d+,\d*)([A-Z]{4})?([^/\n]*?)(?:/{2}(.*))?$`,
)

// Parse parses a full MT940 message using the named dialect (falls back
// to the generic default if name is unrecognized or empty - see
// dialect.go).
func Parse(data []byte, dialectName string) (Statement, error) {
	dialect := GetDialect(dialectName)

	var stmt Statement
	var pendingLine *TransactionLine
	var currency string

	for _, line := range splitLogicalLines(data) {
		tag, value := splitTag(line)
		switch tag {
		case "20":
			stmt.TransactionRefNo = value
		case "25":
			stmt.AccountID = value
		case "28C", "28":
			stmt.StatementNumber = value
		case "60F", "60M":
			bal, err := parseBalance(value)
			if err != nil {
				return stmt, fmt.Errorf("mt940: opening balance: %w", err)
			}
			stmt.OpeningBalance = bal
			currency = bal.Currency
		case "62F", "62M":
			bal, err := parseBalance(value)
			if err != nil {
				return stmt, fmt.Errorf("mt940: closing balance: %w", err)
			}
			stmt.ClosingBalance = bal
		case "61":
			f61, err := parseField61(value, currency)
			if err != nil {
				return stmt, fmt.Errorf("mt940: field 61 %q: %w", value, err)
			}
			if pendingLine != nil {
				// A :61: with no following :86: - some banks omit it.
				stmt.Lines = append(stmt.Lines, *pendingLine)
			}
			pendingLine = &TransactionLine{Field61: f61}
		case "86":
			narrative := dialect.ParseNarrative(value)
			if pendingLine == nil {
				// :86: with no preceding :61: shouldn't happen in valid
				// MT940, but don't lose data over it - attach to a
				// synthetic empty line rather than silently dropping.
				pendingLine = &TransactionLine{}
			}
			pendingLine.Field86 = Field86{Narrative: narrative}
			stmt.Lines = append(stmt.Lines, *pendingLine)
			pendingLine = nil
		}
	}
	if pendingLine != nil {
		stmt.Lines = append(stmt.Lines, *pendingLine)
	}

	return stmt, nil
}

func parseBalance(v string) (Balance, error) {
	if len(v) < 10 {
		return Balance{}, fmt.Errorf("balance field too short: %q", v)
	}
	return Balance{
		DebitCreditMark: v[0:1],
		Date:            v[1:7],
		Currency:        v[7:10],
		Amount:          v[10:],
	}, nil
}

func parseField61(v string, statementCurrency string) (Field61, error) {
	m := field61Pattern.FindStringSubmatch(v)
	if m == nil {
		return Field61{}, fmt.Errorf("does not match expected :61: structure")
	}
	return Field61{
		ValueDate:       m[1],
		EntryDate:       m[2],
		DebitCreditMark: m[3],
		Amount:          m[4],
		TransactionType: m[5],
		CustomerRef:     strings.TrimSpace(m[6]),
		BankRef:         m[7],
		Currency:        statementCurrency,
	}, nil
}

// splitLogicalLines joins continuation lines (lines not starting with
// ":") onto the preceding tagged line, and drops SWIFT's "-" message
// terminator and blank lines.
func splitLogicalLines(data []byte) []string {
	var logical []string
	for _, raw := range strings.Split(string(data), "\n") {
		l := strings.TrimRight(raw, "\r")
		if l == "" || l == "-" {
			continue
		}
		if strings.HasPrefix(l, ":") {
			logical = append(logical, l)
		} else if len(logical) > 0 {
			logical[len(logical)-1] += "\n" + l
		}
	}
	return logical
}

// splitTag splits ":20:REF123" into tag="20", value="REF123".
func splitTag(line string) (tag, value string) {
	line = strings.TrimPrefix(line, ":")
	idx := strings.Index(line, ":")
	if idx < 0 {
		return "", line
	}
	return line[:idx], line[idx+1:]
}
