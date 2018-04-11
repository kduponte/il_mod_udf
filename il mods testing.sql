-- grant python permission to test the udf
grant USAGE on language python to kduponte;

-- UDF to create the full history versions of payments due for IL Mods
create or replace function f_il_mod_full_history (original varchar(1000), most_recent varchar(1000), il_mod_date timestamp)
    returns varchar(1000)

    stable as $$

        import time
        duedates = []
        original_keep = {}
        full_history = {}

        for key in original.keys():
            if time.mktime(time.strptime(key, "%a %b %d %H:%M:%S PDT %Y")) <= time.mktime(time.strptime(il_mod_date, "%Y-%m-%d %H:%M:%S.%f")):
                duedates.append(key)

        original_keep = {key: original[key] for key in duedates if key in original}
        full_history = {key: value for (key, value) in original_keep.items() + most_recent.items()}

        return full_history

    $$ language plpythonu;


-- Selecting max version of loan_audit, which would be used for most_recent
-- and also have details of the loan mod version
with mv as (
  select id, max(version) as max_version
  from loan_audit
  group by 1
),

-- Create 3 versions per loan:
-- 1) original - the loan details at origination
-- 2) most_recent - most recent loan details
-- 3) full - full history of loan payments (if hasPaymentPlan concat(original, most_recent) else most_recent)
-- This definition of "Full" will be incorrect if a loan mod is made within the original schedule.
-- One solution would be to parse dates from hstore of original_paymentsdue, cast to timestamp,
-- and then check if it is >= last_edited date of the first version where haspaymentplan is True
-- If it is >= to last_edited date, then we would drop this payment from the full version because
-- the loan mod was made prior to its duedate
t as (SELECT
        haspaymentplan,
        il_mod.il_mod_date,
        original.paymentsdue    AS original_paymentsdue,
        original.numpayments    AS original_numpayments,
        la.paymentsdue          AS most_recent_paymentsdue,
        la.numpayments          AS most_recent_numpayments,
        CASE WHEN la.haspaymentplan
          THEN f_il_mod_full_history(original.paymentsdue, la.paymentsdue, il_mod.il_mod_date)
        ELSE la.paymentsdue END AS full_paymentsdue
        --CASE WHEN la.haspaymentplan
        --  THEN (original.numpayments + la.numpayments)
        --ELSE la.numpayments END AS full_numpayments

  -- filtering on max/most_recent version of loan_audit and using this one sample user 40207
  -- Also left joining the original version details and loan_mod date
      FROM (SELECT
              loan_audit.id,
              loan_audit.user_id,
              loan_audit.paymentsdue,
              loan_audit.numpayments,
              loan_audit.haspaymentplan
            FROM loan_audit
              INNER JOIN mv ON mv.id = loan_audit.id AND mv.max_version = loan_audit.version
            WHERE loan_audit.user_id = 40207
                  AND loan_audit.dtype IN ('InstallmentLoan', 'PrimeLoan')) la
        LEFT JOIN (
                    SELECT
                      id,
                      paymentsdue,
                      numpayments
                    FROM loan_audit
                    WHERE version = 0
                  ) original ON original.id = la.id
        LEFT JOIN (
                    SELECT
                      id,
                      MIN(lastedited) as il_mod_date
                    FROM loan_audit
                    WHERE haspaymentplan
                    GROUP BY 1
                  ) il_mod ON il_mod.id = la.id
  )

  -- Comparing original, most_recent, and full
  select
    haspaymentplan,
    il_mod_date,
    original_paymentsdue,
    most_recent_paymentsdue,
    full_paymentsdue,
    original_numpayments,
    most_recent_numpayments,
    --full_numpayments,
    REGEXP_COUNT (original_paymentsdue, '"[[:alnum:][:space:]:]+": "[[:digit:]E.]+"') as paymentsduecount_original,
    REGEXP_COUNT (most_recent_paymentsdue, '"[[:alnum:][:space:]:]+": "[[:digit:]E.]+"') as paymentsduecount_most_recent,
    REGEXP_COUNT (full_paymentsdue, '"[[:alnum:][:space:]:]+": "[[:digit:]E.]+"') as paymentsduecount_full
from t


