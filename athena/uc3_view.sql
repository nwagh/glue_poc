CREATE OR REPLACE VIEW uc3_view AS 
WITH
  tran_view AS (
   SELECT
     "acct_id"
   , "max"("tran_date") "max_tran_date"
   FROM
     transactions
   GROUP BY "acct_id"
) 
SELECT
  "concat"("c"."first_name", "concat"(' ', "c"."last_name")) "Customers"
, "t"."acct_id" "Account Name"
, "t"."max_tran_date" "Latest Balance Date"
, "a"."acct_balance" "Account Balance"
FROM
  customers c
, accounts a
, tran_view t
WHERE (("t"."acct_id" = "a"."account_id") AND ("a"."customer_id" = "c"."customer_id"))
