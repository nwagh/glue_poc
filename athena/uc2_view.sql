CREATE OR REPLACE VIEW uc2_view AS 
WITH
  tran_view AS (
   SELECT
     "acct_id"
   , "month"
   , "max"((CASE "tran_code" WHEN 'D' THEN "tran_date" ELSE null END)) "d_tran_date"
   , "max_by"("tran_amount", (CASE "tran_code" WHEN 'D' THEN "tran_date" ELSE null END)) "debit_amt"
   , "max"((CASE "tran_code" WHEN 'C' THEN "tran_date" ELSE null END)) "c_tran_date"
   , "max_by"("tran_amount", (CASE "tran_code" WHEN 'C' THEN "tran_date" ELSE null END)) "credit_amt"
   , "max"((CASE "tran_code" WHEN 'S' THEN "tran_date" ELSE null END)) "s_tran_date"
   , "max_by"("tran_amount", (CASE "tran_code" WHEN 'S' THEN "tran_date" ELSE null END)) "service_amt"
   FROM
     transactions
   GROUP BY "acct_id", "month"
) 
SELECT
  "concat"("c"."first_name", "concat"(' ', "c"."last_name")) "Customer Name"
, "a"."account_name" "Account Number"
, "t"."month" "Transaction Month"
, "t"."debit_amt" "Recent Debit Amount"
, "date_format"("t"."d_tran_date", '%Y-%m-%d') "Recent Debit Transaction Date"
, "t"."credit_amt" "Recent Credit Amount"
, "date_format"("t"."c_tran_date", '%Y-%m-%d') "Recent Credit Transaction Date"
, "t"."service_amt" "Recent Service Charges"
, "date_format"("t"."s_tran_date", '%Y-%m-%d') "Recent Service Charge Date"
FROM
  tran_view t
, accounts a
, customers c
WHERE (("t"."acct_id" = "a"."account_id") AND ("a"."customer_id" = "c"."customer_id"))

