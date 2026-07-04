package mt940_test

import (
	"os"
	"testing"

	"github.com/koriebruh/Jengine/internal/ingestion/parsers/mt940"
)

func TestParse_GenericDialect_BothDebitCreditMarks(t *testing.T) {
	data, err := os.ReadFile("testdata/generic_dialect.sta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	stmt, err := mt940.Parse(data, "generic")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if stmt.TransactionRefNo != "STMT0001" {
		t.Errorf("expected TransactionRefNo STMT0001, got %q", stmt.TransactionRefNo)
	}
	if stmt.AccountID != "1234567890" {
		t.Errorf("expected AccountID 1234567890, got %q", stmt.AccountID)
	}
	if stmt.OpeningBalance.Currency != "EUR" || stmt.OpeningBalance.Amount != "10000,00" {
		t.Errorf("unexpected opening balance: %+v", stmt.OpeningBalance)
	}
	if stmt.ClosingBalance.DebitCreditMark != "C" || stmt.ClosingBalance.Amount != "10250,00" {
		t.Errorf("unexpected closing balance: %+v", stmt.ClosingBalance)
	}

	if len(stmt.Lines) != 2 {
		t.Fatalf("expected 2 transaction lines, got %d", len(stmt.Lines))
	}

	// Line 1: the D (debit) case, with entry date and no bank ref.
	l1 := stmt.Lines[0].Field61
	if l1.ValueDate != "240102" {
		t.Errorf("line 1: expected value_date 240102, got %q", l1.ValueDate)
	}
	if l1.EntryDate != "0103" {
		t.Errorf("line 1: expected entry_date 0103, got %q", l1.EntryDate)
	}
	if l1.DebitCreditMark != "D" {
		t.Errorf("line 1: expected debit_credit_mark D, got %q", l1.DebitCreditMark)
	}
	if l1.Amount != "250,00" {
		t.Errorf("line 1: expected amount 250,00, got %q", l1.Amount)
	}
	if l1.Currency != "EUR" {
		t.Errorf("line 1: expected currency EUR (carried over from opening balance), got %q", l1.Currency)
	}
	if l1.TransactionType != "NTRF" {
		t.Errorf("line 1: expected transaction_type NTRF, got %q", l1.TransactionType)
	}
	if stmt.Lines[0].Field86.Narrative != "PAYMENT TO SUPPLIER ABC" {
		t.Errorf("line 1: unexpected narrative %q", stmt.Lines[0].Field86.Narrative)
	}

	// Line 2: the C (credit) case, with a bank reference after "//".
	l2 := stmt.Lines[1].Field61
	if l2.DebitCreditMark != "C" {
		t.Errorf("line 2: expected debit_credit_mark C, got %q", l2.DebitCreditMark)
	}
	if l2.Amount != "500,00" {
		t.Errorf("line 2: expected amount 500,00, got %q", l2.Amount)
	}
	if l2.CustomerRef != "REF456" {
		t.Errorf("line 2: expected customer ref REF456, got %q", l2.CustomerRef)
	}
	if l2.BankRef != "BANKREF01" {
		t.Errorf("line 2: expected bank ref BANKREF01, got %q", l2.BankRef)
	}
	if stmt.Lines[1].Field86.Narrative != "INCOMING PAYMENT FROM CUSTOMER XYZ" {
		t.Errorf("line 2: unexpected narrative %q", stmt.Lines[1].Field86.Narrative)
	}
}

func TestParse_StructuredDialect_StripsSubfieldCodes(t *testing.T) {
	data, err := os.ReadFile("testdata/structured_dialect.sta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	// Parsing with the generic dialect should leave the "?nn" codes in
	// place (proving the two dialects genuinely behave differently, not
	// just carrying different names).
	genericStmt, err := mt940.Parse(data, "generic")
	if err != nil {
		t.Fatalf("Parse (generic) failed: %v", err)
	}
	if len(genericStmt.Lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(genericStmt.Lines))
	}
	if genericStmt.Lines[0].Field86.Narrative != "?20BANKFEE-JAN?32MONTHLY SERVICE CHARGE" {
		t.Errorf("expected generic dialect to leave subfield codes untouched, got %q", genericStmt.Lines[0].Field86.Narrative)
	}

	stmt, err := mt940.Parse(data, "structured")
	if err != nil {
		t.Fatalf("Parse (structured) failed: %v", err)
	}
	if len(stmt.Lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(stmt.Lines))
	}
	if stmt.Lines[0].Field86.Narrative != "BANKFEE-JAN MONTHLY SERVICE CHARGE" {
		t.Errorf("expected structured dialect to strip subfield codes, got %q", stmt.Lines[0].Field86.Narrative)
	}
	if stmt.Lines[1].Field86.Narrative != "CUSTOMER-042 INVOICE PAYMENT RECEIVED" {
		t.Errorf("unexpected line 2 narrative: %q", stmt.Lines[1].Field86.Narrative)
	}

	// Field naming contract check: field_61/field_86 sub-fields must be
	// exactly as plans/docs/02-data-ingestion.md §3.2's mapping DSL
	// example expects, regardless of dialect.
	f61 := stmt.Lines[0].Field61
	if f61.Amount == "" || f61.DebitCreditMark == "" || f61.Currency == "" || f61.ValueDate == "" {
		t.Errorf("field_61 missing one of amount/debit_credit_mark/currency/value_date: %+v", f61)
	}
}

func TestGetDialect_UnknownFallsBackToGeneric(t *testing.T) {
	d := mt940.GetDialect("does-not-exist")
	if d.Name != mt940.DefaultDialect.Name {
		t.Errorf("expected fallback to generic dialect, got %q", d.Name)
	}
}
