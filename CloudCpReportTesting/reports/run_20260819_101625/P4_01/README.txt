P4-01 - Exactly one correct status per file
============================================================
Result: PASS

Steps performed:
  1. Build 100 source files with matching SUCCESS report rows (same size) -> expect OK
  2. Build 20 source files with no report row at all -> expect MISSING
  3. Build 20 source files with a FAILED report row -> expect FAILED
  4. Build 20 source files with a SUCCESS row but a different size -> expect MISMATCH
  5. Add 10 report rows with no matching source file -> expect EXTRA
  6. Add one OK file whose name embeds a comma, a quote, and a newline (CSV-quoting stress)
  7. Run verify() on the combined 171-file set, write final_report.csv, re-read it back
  8. Count rows per status in the re-parsed file and compare to expected counts
  9. Confirm the tricky-named file re-parses to exactly one OK row
  10. Confirm no file path appears more than once in the final parsed report

Details:
  expected: OK=101, MISSING=20, FAILED=20, MISMATCH=20, EXTRA=10
  actual: OK=101, MISSING=20, FAILED=20, MISMATCH=20, EXTRA=10
  csv quoting preserved: yes
  no duplicates: yes
  final report: C:\Cloud_cp Testing\cloudcp-phase-wise-testing\CloudCpReportTesting\reports\run_20260819_101625\P4_01\final_report.csv
