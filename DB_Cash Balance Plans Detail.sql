WITH person_attributes as 
	(SELECT person.client_id AS Client_ID,
        person.client_id_string AS client_id_string,
        person.platform_person_internal_id AS Person_Internal_ID,
        employee.employee_id AS Employee_ID,
        person.last_name AS Last_Name,
        person.first_name AS First_Name,
        person.middle_name AS Middle_Name,
        person.gender AS Gender,
        person.birth_date AS Birth_Date,
        employee.hire_date AS Original_Hire_Date,
		person.platform_id as platform_id,
        CAST(COALESCE(employee.expected_annual_base_salary, 0) AS NUMERIC(19,4)) AS Salary,
        FLOOR(DATEDIFF(day, person.birth_date, <<$pEndDate>>) / 365.25) AS Age,
        CASE 
            WHEN person.person_reason_code = 'Employee' AND UPPER(rl.standard_value) = 'ACTIVE' 
                THEN ROUND(DATEDIFF(DAY, employee.hire_date, <<$pEndDate>>) / 365.25, 2)
            WHEN person.person_reason_code = 'Employee' AND UPPER(rl.standard_value) <> 'ACTIVE' 
                AND employee.termination_date IS NOT NULL 
                AND EXTRACT(YEAR FROM employee.termination_date) <> 2299 
                THEN ROUND(DATEDIFF(DAY, employee.hire_date, employee.termination_date) / 365.25, 2)
            ELSE NULL
        END AS Tenure,
        person.person_reason_code AS Person_Type,
        employee.standard_employment_status AS Employment_Status,
        employee.employment_status_begin_date AS Employment_Status_Begin_Date,
        employee.fulltime_parttime_status AS FullTime_PartTime,
        employee.hourly_salary_status AS Salary_Hourly,
        person.union_name AS Union_name,
		psf.row_level_security_filter_value,
        pcf.custom_filter_1,
        pcf.custom_filter_2,
        pcf.custom_filter_3,
        pcf.custom_filter_4,
        pcf.custom_filter_5
	FROM person_rpt.person person
    INNER JOIN person_rpt.person_employment employee ON 
        person.client_id = employee.client_id AND
        person.platform_id = employee.platform_id AND
        person.platform_person_internal_id = employee.platform_person_internal_id AND
        <<$pEndDate>> BETWEEN person.begin_date AND person.end_date AND
        <<$pEndDate>> BETWEEN employee.begin_date AND employee.end_date AND
        person.client_id = <<$pClientId>>
	LEFT JOIN adlfound_itg.standard_rollups_lookup rl
        ON employee.standard_employment_status_code = rl.standard_key
        AND rl.standard_group_key = 'EMPL-STAT-CD-ROLLUP1'
    LEFT JOIN person_itg.person_security_filter psf ON 
        person.platform_id = psf.platform_id AND
        person.platform_person_internal_id = psf.platform_person_internal_id AND
        person.client_id = psf.client_id AND
        psf.client_id = CAST(<<$pClientId>> AS INT)
    LEFT JOIN person_itg.person_custom_filter pcf ON 
        person.platform_id = pcf.platform_id AND
        person.platform_person_internal_id = pcf.platform_person_internal_id AND
        person.client_id = pcf.client_id AND
        <<$pEndDate>> BETWEEN pcf.begin_date AND pcf.end_date AND
        pcf.client_id = CAST(<<$pClientId>> AS INT) AND
        pcf.data_product = 'DB'
	),
person_plan_stat as(
	SELECT a.*
        FROM wealth_rpt.person_planstatus_dim a
        INNER JOIN wealth_rpt.dc_category_dim b ON 
            a.client_id = b.client_id AND
            a.category_id = b.category_id AND
            a.plan_id = b.plan_id
        WHERE 
            a.platform_indicator_code IN ('R3','R4', 'DBE') AND
            b.category_type_code = 'PS' AND
            b.category_definition_brand_code = 'DB-AVLB' AND
            <<$pEndDate>> BETWEEN a.begin_date AND a.end_date AND
            a.client_id = <<$pClientId>>
	),
db_person_plan as (
	SELECT 	distinct
		db_elig.client_id,
		db_elig.platform_id,
		db_elig.platform_person_internal_id,
		db_elig.plan_id,
		db_elig.vesting_service,
        db_elig.benefit_service,
        db_elig.eligibility_service,
        db_elig.participation_date,
        db_elig.early_retirement_date,
        db_elig.normal_retirement_date,
        db_elig.fully_vested_date AS First_vested_date,
        db_elig.fully_vested_date AS hundred_percent_vested_date,
        db_elig.service_as_of_date AS Service_and_eligibility_last_refresh_date
	FROM wealth_rpt.db_person_plan db_elig  
        WHERE <<$pEndDate>> BETWEEN db_elig.begin_date AND db_elig.end_date AND
        db_elig.client_id = <<$pClientId>>
	),
db_cash_balance1 as(
	SELECT
		fund.client_id,
		fund.plan_id,
		fund.platform_person_internal_id,
		fund.hard_balance_id,
		fund.platform_id,
		min(hbal_d.account_long_description) AS account_long_description,
		MIN(hbal_d.plan_long_description) AS plan_long_description,
		ROUND(SUM(op_bal.closing_units * bysl_open.daily_price), 2) AS opening_Balance,
		ROUND(SUM(fund.closing_units * bysl.daily_price), 2) AS closing_Balance
	FROM wealth_rpt.db_cash_balance fund
	INNER JOIN wealth_rpt.buysell_fund_prices bysl ON 
        bysl.client_id = fund.client_id AND
        bysl.platform_id = fund.platform_id AND
        bysl.fund_id = fund.fund_id AND
		bysl.subfund_id = fund.subfund_id AND
        bysl.effective_date = <<$pEndDate>> AND
		<<$pEndDate>> >= fund.begin_date 
		AND <<$pEndDate>> <= fund.end_date 
		AND fund.client_id = <<$pClientId>>
		AND bysl.client_id = <<$pClientId>>
	INNER JOIN wealth_rpt.hardbalance_dim hbal_d ON 
        hbal_d.client_id = fund.client_id AND
        fund.plan_id = hbal_d.plan_id AND
        fund.fund_id = hbal_d.fund_id AND
		fund.hard_balance_id = hbal_d.hardbalance_id AND
        fund.client_id = <<$pClientId>> AND
        hbal_d.db_attribute_type_code = 'CBAL'
	LEFT JOIN wealth_rpt.db_cash_balance op_bal 
		ON op_bal.client_id = fund.client_id AND
        fund.plan_id = op_bal.plan_id AND
        fund.fund_id = op_bal.fund_id AND
		fund.hard_balance_id = op_bal.hard_balance_id AND
		fund.platform_person_internal_id = op_bal.platform_person_internal_id AND
        op_bal.client_id = <<$pClientId>> AND
		<<$pBeginDate>> >= op_bal.begin_date 
		AND <<$pBeginDate>> <= op_bal.end_date 
	LEFT JOIN wealth_rpt.buysell_fund_prices bysl_open ON 
        bysl_open.client_id = fund.client_id AND
        bysl_open.platform_id = fund.platform_id AND
        bysl_open.fund_id = fund.fund_id AND
		bysl_open.subfund_id = fund.subfund_id AND
        bysl_open.effective_date = DATEADD(day, -1, <<$pBeginDate>>)
	GROUP BY
		fund.client_id,
		fund.plan_id,
		fund.platform_id,
		fund.platform_person_internal_id,
		fund.hard_balance_id
	),
db_cash_balance2 as(
	SELECT
		fund_trans.client_id,
		fund_trans.plan_id,
		fund_trans.platform_person_internal_id,
		fund_trans.hard_balance_id,
		fund_trans.platform_id,
		ROUND(SUM(fund_trans.pay_credit_amount), 2) AS Pay_credit,
        ROUND(SUM(fund_trans.interest_amount), 2) AS Interest_credit,
        ROUND(SUM(fund_trans.asset_in_amount), 2) AS Asset_transfer_In,
        ROUND(SUM(fund_trans.asset_out_amount), 2) AS Asset_transfer_out,
        ROUND(SUM(fund_trans.converted_in_amount), 2) AS Converted_in,
        ROUND(SUM(fund_trans.converted_out_amount), 2) AS Converted_out,
        ROUND(SUM(fund_trans.disbursement_payment_amount), 2) AS disbursement,
        ROUND(SUM(fund_trans.forfeiture_amount), 2) AS forfeiture,
        ROUND(SUM(fund_trans.restored_forfieted_amounts), 2) AS Restored_forfeiture,
        ROUND(SUM(fund_trans.interest_amount), 2) AS Interest_accounting_post_type_code,
        ROUND(SUM(fund_trans.transfer_in_amount), 2) AS Transfer_in,
        ROUND(SUM(fund_trans.transfer_out_amount), 2) AS Transfer_out,
        ROUND(SUM(fund_trans.roll_in_amount), 2) AS Balance_Roll_in,
        ROUND(SUM(fund_trans.roll_out_amount), 2) AS Balance_Roll_out,
        ROUND(SUM(fund_trans.overage_out_amount), 2) AS Overage_out
	FROM wealth_rpt.db_cash_balance fund_trans
	INNER JOIN wealth_rpt.hardbalance_dim hbal_d ON 
        hbal_d.client_id = fund_trans.client_id AND
        fund_trans.plan_id = hbal_d.plan_id AND
        fund_trans.fund_id = hbal_d.fund_id AND
		fund_trans.hard_balance_id = hbal_d.hardbalance_id AND
        fund_trans.client_id = <<$pClientId>> AND
        hbal_d.db_attribute_type_code = 'CBAL' AND
		<<$pBeginDate>> <= fund_trans.accounting_activity_effective_date AND <<$pEndDate>> >= fund_trans.accounting_activity_effective_date AND
        fund_trans.client_id = <<$pClientId>>
	GROUP BY
		fund_trans.client_id,
		fund_trans.plan_id,
		fund_trans.platform_person_internal_id,
		fund_trans.platform_id,
		fund_trans.hard_balance_id
	),
join_db_cash_balance AS(
	SELECT
		cash1.client_id AS client_id,
		cash1.plan_id AS plan_id,
		cash1.platform_id,
		cash1.platform_person_internal_id AS platform_person_internal_id,
		cash1.account_long_description as account_long_description,
		cash1.plan_long_description AS plan_long_description,
		SUM(opening_Balance) AS opening_Balance,
		SUM(closing_Balance) AS closing_Balance,
		SUM(Pay_credit) AS Pay_credit,
		SUM(Interest_credit) AS Interest_credit,
		SUM(Asset_transfer_In) AS Asset_transfer_In,
		SUM(Asset_transfer_out) AS Asset_transfer_out,
		SUM(Converted_in) AS Converted_in,
		SUM(Converted_out) AS Converted_out,
		SUM(disbursement) AS disbursement,
		SUM(forfeiture) AS forfeiture,
		SUM(Restored_forfeiture) AS Restored_forfeiture,
		SUM(Interest_accounting_post_type_code) AS Interest_accounting_post_type_code,
		SUM(Transfer_in) AS Transfer_in,
		SUM(Transfer_out) AS Transfer_out,
		SUM(Balance_Roll_in) AS Balance_Roll_in,
		SUM(Balance_Roll_out) AS Balance_Roll_out,
		SUM(Overage_out) AS Overage_out
	FROM db_cash_balance1 cash1
	LEFT JOIN db_cash_balance2 cash2
		ON cash2.client_id = cash1.client_id AND
        cash2.platform_id = cash1.platform_id AND
        cash2.platform_person_internal_id = cash1.platform_person_internal_id AND
		cash2.plan_id=cash1.plan_id AND
		cash2.hard_balance_id = cash1.hard_balance_id
	GROUP BY
		cash1.client_id,
		cash1.plan_id,
		cash1.platform_id,
		cash1.platform_person_internal_id,
		cash1.plan_long_description,
		cash1.account_long_description
	),
join_person_cashbal AS(
	SELECT	
		person.Client_ID AS Client_ID,
		person.client_id_string AS client_id_string,
		person.Person_Internal_ID AS Person_Internal_ID,
		person.platform_id as platform_id,
		person.Employee_ID AS Employee_ID,
		person.Last_Name AS Last_Name,
		person.First_Name AS First_Name,
		person.Middle_Name AS Middle_Name,
		person.Gender AS Gender,
		person.age,
		person.Birth_Date AS Birth_Date,
		person.Original_Hire_Date AS Original_Hire_Date,
		person.Tenure AS Tenure,
		person.salary,
		person.Person_Type AS Person_Type,
		person.Employment_Status AS Employment_Status,
		person.Employment_Status_Begin_Date AS Employment_Status_Begin_Date,
		person.FullTime_PartTime AS FullTime_PartTime,
		person.Salary_Hourly AS Salary_Hourly,
		person.Union_name AS union_name,
		ppstat.plan_status_description AS Plan_Status,
		person.row_level_security_filter_value,
        person.custom_filter_1,
        person.custom_filter_2,
        person.custom_filter_3,
        person.custom_filter_4,
        person.custom_filter_5,
		cash1.account_long_description AS account_long_description,
		cash1.plan_long_description AS plan_name,
		COALESCE(cash1.opening_Balance,0) AS opening_Balance,
		COALESCE(cash1.closing_Balance,0) AS closing_Balance,
		COALESCE(cash1.Pay_credit,0) AS Pay_credit,
		COALESCE(cash1.Interest_credit,0) AS Interest_credit,
		COALESCE(cash1.Asset_transfer_In,0) AS Asset_transfer_In,
		COALESCE(cash1.Asset_transfer_out,0) AS Asset_transfer_out,
		COALESCE(cash1.Converted_in,0) AS Converted_in,
		COALESCE(cash1.Converted_out,0) AS Converted_out,
		COALESCE(cash1.disbursement,0) AS disbursement,
		COALESCE(cash1.forfeiture,0) AS forfeiture,
		COALESCE(cash1.Restored_forfeiture,0) AS Restored_forfeiture,
		COALESCE(cash1.Interest_accounting_post_type_code,0) AS Interest_accounting_post_type_code,
		COALESCE(cash1.Transfer_in,0) AS Transfer_in,
		COALESCE(cash1.Transfer_out,0) AS Transfer_out,
		COALESCE(cash1.Balance_Roll_in,0) AS Balance_Roll_in,
		COALESCE(cash1.Balance_Roll_out,0) AS Balance_Roll_out,
		COALESCE(cash1.Overage_out,0) AS Overage_out,
		vesting_service,
        benefit_service,
        eligibility_service,
        participation_date,
        early_retirement_date,
        normal_retirement_date,
        First_vested_date,
        hundred_percent_vested_date,
        Service_and_eligibility_last_refresh_date
	FROM person_attributes person
	INNER JOIN join_db_cash_balance cash1 ON 
        person.client_id = cash1.client_id AND
        person.platform_id = cash1.platform_id AND
        person.Person_Internal_ID = cash1.platform_person_internal_id
	LEFT JOIN person_plan_stat ppstat 
		ON ppstat.client_id = cash1.client_id AND
        ppstat.platform_id = cash1.platform_id AND
        ppstat.plan_id = cash1.plan_id AND
        ppstat.platform_person_internal_id = cash1.platform_person_internal_id
	LEFT JOIN db_person_plan db_elig ON 
        cash1.client_id = db_elig.client_id AND
        cash1.platform_id = db_elig.platform_id AND
        cash1.platform_person_internal_id = db_elig.platform_person_internal_id AND
        cash1.plan_id = db_elig.plan_id
	)
, main as (
SELECT 
	Client_ID,
	client_id_string,
	Person_Internal_ID as platform_person_internal_id,
	Platform_id,
	Employee_ID,
	Last_Name,
	First_Name,
	Middle_Name,
	Gender,
	Birth_Date,
	Original_Hire_Date,
	salary,
	age,
	Tenure,
	Person_Type,
	Employment_Status,
	Employment_Status_Begin_Date,
	FullTime_PartTime,
	Salary_Hourly,
	union_name as Union,
	Plan_Status,
	AGE_RG.standard_display_name AS Age_Range,
    salary_rg.standard_display_name AS Salary_Range,
    tenure_rg.standard_display_name AS Employment_Range,
	row_level_security_filter_value,
    custom_filter_1,
    custom_filter_2,
    custom_filter_3,
    custom_filter_4,
    custom_filter_5,
	account_long_description as account,
	plan_name,
	vesting_service,
    benefit_service,
    eligibility_service,
    participation_date,
    early_retirement_date,
    normal_retirement_date,
    First_vested_date,
    hundred_percent_vested_date,
    Service_and_eligibility_last_refresh_date,
	opening_Balance,
	closing_Balance,
	Pay_credit,
	Interest_credit,
	Asset_transfer_In,
	Asset_transfer_out,
	Converted_in,
	Converted_out,
	disbursement,
	forfeiture,
	Restored_forfeiture,
	Interest_accounting_post_type_code,
	Transfer_in,
	Transfer_out,
	Balance_Roll_in,
	Balance_Roll_out,
	Overage_out
FROM join_person_cashbal final
LEFT JOIN (
    SELECT * FROM adlfound_itg.standard_ranges_lookup 
    WHERE standard_group_key = 'AGE_RANGE_DB'
) AGE_RG ON FLOOR(final.Age) BETWEEN AGE_RG.standard_min AND AGE_RG.standard_max
LEFT JOIN (
    SELECT * FROM adlfound_itg.standard_ranges_lookup 
    WHERE standard_group_key = 'TENURE'
) TENURE_RG ON FLOOR(final.Tenure) BETWEEN TENURE_RG.standard_min AND TENURE_RG.standard_max
LEFT JOIN (
    SELECT * FROM adlfound_rpt.standard_range_lookup 
    WHERE standard_group_key = 'SALARY_RANGE_WEALTH'
) salary_rg ON FLOOR(final.Salary) BETWEEN salary_rg.standard_min AND salary_rg.standard_max
WHERE Plan_Name IS NOT NULL AND (opening_Balance <> 0 OR closing_Balance<>0 OR Pay_credit<> 0 OR Interest_credit<> 0 OR Asset_transfer_In<> 0 OR Asset_transfer_out <> 0 OR Converted_in<> 0
OR Converted_out <> 0 OR disbursement <> 0 OR forfeiture <> 0 OR Restored_forfeiture <> 0 OR Interest_accounting_post_type_code <> 0 OR Transfer_in<> 0 OR Transfer_out <> 0 OR
Balance_Roll_in<> 0 OR Balance_Roll_out <> 0 OR Overage_out <> 0))

Select * from main
