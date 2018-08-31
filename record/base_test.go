package record

import (
	"testing"

	"mgotools/record"
)

func TestNewBase(tr *testing.T) {
	tr.Run("Base24", func(t *testing.T) {
		if b, err := record.NewBase("Tue Jan 16 15:00:40.105 [initandlisten] db version v2.4.14", 1); err != nil {
			t.Error("base (2.4) returned an error, should be successful")
		} else if b.RawDate == "" {
			t.Error("base.RawDate (2.4) is empty or returned with error (%s)", err)
		} else if b.RawDate != "Tue Jan 16 15:00:40.105" {
			t.Error("base.RawDate (2.4) is incorrect")
		} else if b.RawContext != "[initandlisten]" {
			t.Error("base.RawContext (2.4) is incorrect")
		} else if b.RawComponent != "" {
			t.Error("base.RawComponent (2.4) returned a component")
		} else if b.RawSeverity != record.SeverityNone {
			t.Error("base.RawSeverity (2.4) returned a severity")
		} else if b.RawMessage != "db version v2.4.14" {
			t.Error("base.RawMessage (2.4) is incorrect")
		}
	})
	tr.Run("Base26", func(t *testing.T) {
		if b, err := record.NewBase("2018-01-16T15:00:41.014-0800 [initandlisten] db version v2.6.12", 1); err != nil {
			t.Error("base (2.6) returned an error, should be successful")
		} else if b.RawDate == "" {
			t.Error("base.RawDate (2.6) is empty or returned with error (%s)", err)
		} else if b.RawDate != "2018-01-16T15:00:41.014-0800" {
			t.Error("base.RawDate (2.6) is incorrect")
		} else if b.RawContext != "[initandlisten]" {
			t.Error("base.RawContext (2.6) is incorrect")
		} else if b.RawComponent != "" {
			t.Error("base.RawComponent (2.6) returned a component")
		} else if b.RawSeverity != record.SeverityNone {
			t.Error("base.RawSeverity (2.6) returned a severity")
		} else if b.RawMessage != "db version v2.6.12" {
			t.Error("base.RawMessage (2.6) is incorrect")
		}
	})
	tr.Run("Base30", func(t *testing.T) {
		if b, err := record.NewBase("2018-01-16T15:00:41.759-0800 I CONTROL  [initandlisten] db version v3.0.15", 1); err != nil {
			t.Error("base (3.x) returned an error, should be successful")
		} else if b.RawDate == "" {
			t.Error("base.RawDate (3.x) is empty or returned with error (%s)", err)
		} else if b.RawDate != "2018-01-16T15:00:41.759-0800" {
			t.Error("base.RawDate (3.x) is incorrect")
		} else if b.RawContext != "[initandlisten]" {
			t.Error("base.RawContext (3.x) is incorrect")
		} else if b.RawComponent != "CONTROL" {
			t.Error("base.RawComponent (3.x) returned a component")
		} else if b.RawSeverity != 'I' {
			t.Error("base.RawSeverity (3.x) returned a severity")
		} else if b.RawMessage != "db version v3.0.15" {
			t.Error("base.RawMessage (3.x) is incorrect")
		}
	})
	tr.Run("InvalidPartial", func(t *testing.T) {
		if b, err := record.NewBase("line 1", 1); err == nil || b.RawDate != "" {
			t.Error("base.RawDate is not empty but should be")
		}
		if _, err := record.NewBase("Tue Jan 16 15:00:40.105", 1); err == nil {
			t.Error("base.RawDate (2.4) reported success, should be error")
		}
		if _, err := record.NewBase("2018-01-16 15:00:41.014-0800", 1); err == nil {
			t.Error("base.RawDate reported success, should be error")
		}
		if _, err := record.NewBase("2018-01-16T15:00:41.759-0800 I CONTROL  [initandlisten] ", 1); err != nil {
			t.Error("base.RawMessage can be blank, but returned an error")
		}
		if _, err := record.NewBase("2018-01-16T15:00:41.759-0800 I INVALID  [initandlisten] ", 1); err == nil {
			t.Error("base.RawComponent cannot be invalid, but returned without error")
		}
		if _, err := record.NewBase("2018-01-16T15:00:41.759-0800 ! CONTROL  [initandlisten] ", 1); err == nil {
			t.Error("base.RawSeverity cannot be invalid, but returned without error")
		}
		if _, err := record.NewBase("2018-01-16T15:00:41.759-0800 I CONTROL  ", 1); err == nil {
			t.Error("base.RawContext is empty, should be an error")
		}
	})
	tr.Run("Invalid24Date", func(t *testing.T) {
		if _, err := record.NewBase("Xyz Jan 16 15:00:40.105  [initandlisten]", 1); err != nil && err != record.ErrorParsingDate {
			t.Error("base.RawDate is incorrect, without incorrect error type")
		}
		if _, err := record.NewBase("Tue Xyz 16 15:00:40.105  [initandlisten]", 1); err != nil && err != record.ErrorParsingDate {
			t.Error("base.RawDate is incorrect, without incorrect error type")
		}
		if _, err := record.NewBase("Tue Jan AB 15:00:40.105  [initandlisten]", 1); err != nil && err != record.ErrorParsingDate {
			t.Error("base.RawDate is incorrect, without incorrect error type")
		}
		if _, err := record.NewBase("Tue Jan 16 XX:00:40.105  [initandlisten]", 1); err != nil && err != record.ErrorParsingDate {
			t.Error("base.RawDate is incorrect, without incorrect error type")
		}
		if _, err := record.NewBase("Tue Jan 16 15:00:40.XXX  [initandlisten]", 1); err != nil && err != record.ErrorParsingDate {
			t.Error("base.RawDate is incorrect, without incorrect error type")
		}
		if _, err := record.NewBase("Tue Jan 16 15:00:40  [initandlisten]", 1); err != nil {
			t.Error("base.RawDate is correct, but returned an error")
		}
	})
}
