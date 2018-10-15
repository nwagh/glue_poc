CREATE OR REPLACE VIEW uc1_view AS 
WITH
  tran_view AS (
   SELECT
     "acct_id"
   , "month"
   , CAST("sum"((CASE "tran_code" WHEN 'D' THEN "tran_amount" ELSE 0 END)) AS decimal(10,2)) "Sum of Debits"
   , CAST("sum"((CASE "tran_code" WHEN 'C' THEN "tran_amount" ELSE 0 END)) AS decimal(10,2)) "Sum of Credits"
   , CAST("sum"((CASE "tran_code" WHEN 'L' THEN "tran_amount" ELSE 0 END)) AS decimal(10,2)) "Sum of Late_fees"
   , CAST("sum"((CASE "tran_code" WHEN 'S' THEN "tran_amount" ELSE 0 END)) AS decimal(10,2)) "Sum of Service Charges"
   FROM
     banking.transactions
   GROUP BY "acct_id", "month"
) 
SELECT
  "concat"("c"."first_name", "concat"(' ', "c"."last_name")) "Customer Name"
, "a"."account_name" "Account Number"
, "t"."month" "Transaction Month"
, "t"."sum of debits"
, "t"."sum of credits"
, "t"."sum of late_fees"
, "t"."sum of service charges"
FROM
  tran_view t
, accounts a
, customers c
WHERE (("t"."acct_id" = "a"."account_id") AND ("a"."customer_id" = "c"."customer_id"))

