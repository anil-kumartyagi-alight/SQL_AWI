WITH 
mv_person AS ( SELECT DISTINCT global_client_id,global_person_id,client_id,client_id_string,standard_gender AS gender, union_name, platform_id, platform_person_internal_id,person_reason_code,begin_date,end_date, row_level_security_filter_value,
CASE WHEN birth_date IN ('1800-01-01','1900-01-01','1901-01-01','1910-01-01','1930-01-01','0001-12-30 BC') THEN NULL ELSE birth_date END AS birth_date,last_name AS LAST_NM,first_name AS FRST_NM,middle_name AS MID_NM,is_bad_preferred_address  as BAD_ADDR_IND_CD
FROM person_rpt.person PRSN_DIMN7
WHERE begin_date <= last_day(<<$pAsOfDate>>)
AND end_date >= last_day(<<$pAsOfDate>>) AND client_id = cast(<<$pClientId>> as int)  
),

mv_person_employment as (SELECT DISTINCT global_client_id,global_person_id,client_id,platform_person_internal_id, platform_id, employment_status,standard_value as employment_status_roll_up_1,
fulltime_parttime_status,cast (coalesce (expected_annual_base_salary,0)  as numeric (19,4))  as expected_annual_base_salary,hourly_salary_status,employment_status_begin_date as effective_begin_date,hire_date,termination_date,employee_id
 FROM person_rpt.person_employment e 
left join (select distinct standard_key,standard_value from adlfound_itg.standard_rollups_lookup where 
standard_group_key in ('EMPL-STAT-CD-ROLLUP1') ) s
on trim(standard_employment_status_code)=(standard_key)
WHERE  begin_date <= last_day(<<$pAsOfDate>>)
 AND end_date >= last_day(<<$pAsOfDate>>) AND client_id = cast(<<$pClientId>> as int)
),

pcf AS (SELECT * FROM person_itg.person_custom_filter WHERE client_id = CAST(<<$pClientId>> AS INT) AND data_product = 'DB' AND begin_date <= last_day(<<$pAsOfDate>>) AND end_date >= last_day(<<$pAsOfDate>>)
),

person AS (
    SELECT p.global_client_id,p.platform_id, p.global_person_id, p.client_id, p.client_id_string, p.platform_person_internal_id, p.row_level_security_filter_value,
           e.expected_annual_base_salary AS salary, e.hourly_salary_status, e.fulltime_parttime_status, e.employment_status_roll_up_1 as employment_status,p.begin_date, p.end_date,
           p.person_reason_code, p.union_name, p.gender, p.FRST_NM, p.MID_NM, p.LAST_NM, p.birth_date,
           CASE WHEN p.person_reason_code = 'Employee' AND UPPER(e.employment_status_roll_up_1) <> 'ACTIVE' AND e.termination_date IS NOT NULL AND EXTRACT(YEAR FROM e.termination_date) <> 2299 THEN e.termination_date ELSE NULL END AS termination_date,
           e.hire_date, e.effective_begin_date, e.employee_id, e.employment_status_roll_up_1,
           CASE WHEN p.birth_date IN ('1800-01-01','1900-01-01','1901-01-01','1910-01-01','1930-01-01') THEN NULL ELSE ROUND(DATEDIFF(DAY, p.birth_date, LAST_DAY(<<$pAsOfDate>>)) / 365.25, 2) END AS age,
           CASE WHEN p.person_reason_code = 'Employee' AND UPPER(e.employment_status_roll_up_1) = 'ACTIVE' THEN ROUND(DATEDIFF(DAY, e.hire_date, LAST_DAY(<<$pAsOfDate>>)) / 365.25, 2)
                WHEN p.person_reason_code = 'Employee' AND UPPER(e.employment_status_roll_up_1) <> 'ACTIVE' AND e.termination_date IS NOT NULL AND EXTRACT(YEAR FROM e.termination_date) <> 2299 THEN ROUND(DATEDIFF(DAY, e.hire_date, e.termination_date) / 365.25, 2)
                ELSE NULL END AS tenure,
           pcf.custom_filter_1, pcf.custom_filter_2, pcf.custom_filter_3, pcf.custom_filter_4, pcf.custom_filter_5, BAD_ADDR_IND_CD
    FROM mv_person p
    LEFT JOIN mv_person_employment e ON p.client_id = e.client_id AND p.platform_person_internal_id = e.platform_person_internal_id 
	AND p.platform_id = e.platform_id
	-- AND p.global_client_id = e.global_client_id 
	AND p.client_id = CAST(<<$pClientId>> AS INT)
    LEFT JOIN pcf ON p.platform_id = pcf.platform_id AND p.platform_person_internal_id = pcf.platform_person_internal_id AND p.client_id = pcf.client_id
),
range_lookup AS (SELECT standard_group_key, standard_display_name AS standard_display_value, standard_min, standard_max, OWNER FROM adlfound_itg.standard_ranges_lookup
),

db_person_attributes AS (
    SELECT a.client_id,a.platform_id, a.client_id_string, a.platform_person_internal_id, a.salary,
           D.standard_display_value AS SALARY_RANGE, a.hourly_salary_status, a.fulltime_parttime_status, a.begin_date, a.end_date,
           a.employment_status, a.person_reason_code, a.union_name, a.gender, a.age,
           B.standard_display_value AS AGE_RANGE, a.tenure, C.standard_display_value AS TENURE_RANGE,
           a.row_level_security_filter_value, a.custom_filter_1, a.custom_filter_2, a.custom_filter_3,
           a.custom_filter_4, a.custom_filter_5, a.BAD_ADDR_IND_CD, a.FRST_NM, a.MID_NM, a.LAST_NM,
           a.birth_date, a.termination_date, a.hire_date, a.effective_begin_date, a.employee_id
    FROM person a
    LEFT JOIN (SELECT * FROM range_lookup WHERE standard_group_key = 'AGE_RANGE_DB') B 
        ON FLOOR(a.age) BETWEEN B.standard_min AND B.standard_max
    LEFT JOIN (SELECT * FROM range_lookup WHERE standard_group_key = 'TENURE') C 
        ON FLOOR(a.tenure) BETWEEN C.standard_min AND C.standard_max
    LEFT JOIN (SELECT * FROM range_lookup WHERE standard_group_key = 'SALARY_RANGE_WEALTH') D 
        ON FLOOR(a.salary) BETWEEN D.standard_min AND D.standard_max
),

db_plan_dim as ( select distinct client_id,plan_id,plan_ldsc_tx from wealth_rpt.db_plan_dim where client_id=<<$pClientId>>
),

PRSN_PLANSTAT_DIMN01 AS ( SELECT a.client_id,a.platform_id, a.platform_person_internal_id AS PRSN_INTN_ID, plan_status_description, a.plan_id
  ,c.plan_ldsc_tx as Plan_Description 
 FROM wealth_rpt.person_planstatus_dim a  INNER JOIN wealth_rpt.dc_category_dim b ON a.client_id = b.client_id
  AND a.category_id = b.category_id AND a.plan_id = b.plan_id  INNER JOIN wealth_rpt.db_plan_dim c on  a.client_id = c.client_id AND a.plan_id = c.plan_id WHERE a.platform_indicator_code IN ('R3','R4','DBE') AND b.category_type_code = 'PS'
  and a.plan_status_code='UNAV' AND last_day(<<$pAsOfDate>>) >= a.begin_date AND last_day(<<$pAsOfDate>>) <= a.end_date
  AND b.category_definition_brand_code = 'DB-AVLB' AND a.client_id =<<$pClientId>>
 ),
 
Exp_Calen_yza as ( SELECT DISTINCT last_day(calendar_date)::date AS cal_dt, calendar_year,
    dateadd(day, 1, last_day(dateadd(month, -1, calendar_date))) AS first_dt
FROM adlfound_itg.calendar Calen_yza
WHERE calendar_year = (date_part(year, current_date) - 2) OR calendar_year = (date_part(year, current_date) - 1)
   OR (calendar_year = date_part(year, current_date) AND date_part(month, current_date) >= date_part(month, calendar_date))
), 

Shortcut_to_Pension_Popu_Elig_Current_All AS ( SELECT distinct DB_ELIG_A_FACT11.client_id,platform_id, DB_ELIG_A_FACT11.platform_person_internal_id AS PRSN_INTN_ID, DB_ELIG_A_FACT11.plan_id AS PLAN_ID, DB_ELIG_A_FACT11.earliest_commencement_date_for_ccp AS earl_cmnc_dt_ccp, DB_ELIG_A_FACT11.normal_retirement_date AS normal_retire_dt,
case when is_deferred_vested then 'Y' else 'N' end as is_deferred_vested,
fully_vested_date  AS fully_vested_date,
  begin_date,end_date		
FROM  wealth_rpt.db_person_plan DB_ELIG_A_FACT11 WHERE DB_ELIG_A_FACT11.client_id=<<$pClientId>> and last_day(<<$pAsOfDate>>) between begin_date and end_date

),


query_db_payments As (select distinct client_id,platform_id,  platform_person_internal_id  ,payment_effective_date,
payment_adjustment_cd,payment_type,plan_id,plan,
payment_id,payment_destination,sum(total_payment_amount) as total_payment_amount,address_format_type_code
from wealth_rpt.db_payments  where 
client_id= cast(<<$pClientId>> as int) 
--and extract(year from payment_effective_date) >= 2023  and payment_effective_date <= <<$pAsOfDate>>
and payment_instruction_status_code ='A' 
and convert_timezone('CST', payment_effective_date) >= '1800-01-01'
/*AND (
    date_part(year, convert_timezone('CST', payment_effective_date)) = date_part(year, current_date) - 2
    OR date_part(year, convert_timezone('CST', payment_effective_date)) = date_part(year, current_date) - 1
    OR (
        date_part(year, convert_timezone('CST', payment_effective_date)) = date_part(year, current_date)
        AND date_part(month, convert_timezone('CST', payment_effective_date)) <= date_part(month, current_date)
    )
)*/
group by platform_person_internal_id  ,payment_effective_date,address_format_type_code,
payment_id,payment_destination,payment_adjustment_cd,client_id,payment_type,plan_id,plan,platform_id
),

payment_attribute_join as (SELECT person.client_id, query_db_payments.platform_person_internal_id, query_db_payments.payment_id,query_db_payments.plan_id,extract(year from query_db_payments.payment_effective_date) as payment_year,last_day(query_db_payments.payment_effective_date) as reporting_month_dt, query_db_payments.plan as plan_desc,person.person_reason_code,person.client_id_string,person.salary_range,person.age_range,person.tenure_range,person.gender,person.hourly_salary_status,person.fulltime_parttime_status,person.union_name,person.row_level_security_filter_value,person.custom_filter_1,person.custom_filter_2,person.custom_filter_3,person.custom_filter_4,person.custom_filter_5,Person.employment_status,cal.cal_dt
from query_db_payments inner join db_person_attributes person on
   query_db_payments.client_id = person.client_id
  AND query_db_payments.platform_person_internal_id = person.platform_person_internal_id
   AND query_db_payments.platform_id = person.platform_id
inner join Exp_Calen_yza cal on cal.cal_dt between Person.begin_date and Person.end_date  
),  

Pension_Participant_Data as (SELECT Person.platform_person_internal_id AS person_internal_id, Person.employment_status,
	Person.person_reason_code, Person.row_level_security_filter_value, fully_vested_date, cal.cal_dt,
Shortcut_to_Pension_Popu_Elig_Current_All.plan_id, DB_Plan_Dimension.plan_ldsc_tx as plan_desc, Shortcut_to_Pension_Popu_Elig_Current_All.client_id,Shortcut_to_Pension_Popu_Elig_Current_All.is_deferred_vested,Shortcut_to_Pension_Popu_Elig_Current_All.begin_date,person.client_id_string,person.salary_range,person.age_range,person.tenure_range,person.gender,person.hourly_salary_status,person.fulltime_parttime_status,person.union_name,person.custom_filter_1,person.custom_filter_2,person.custom_filter_3,person.custom_filter_4,person.custom_filter_5
FROM db_person_attributes Person LEFT OUTER JOIN Shortcut_to_Pension_Popu_Elig_Current_All
  ON Person.platform_person_internal_id = Shortcut_to_Pension_Popu_Elig_Current_All.PRSN_INTN_ID AND
Person.client_id = Shortcut_to_Pension_Popu_Elig_Current_All.client_id
and Person.platform_id = Shortcut_to_Pension_Popu_Elig_Current_All.platform_id
left JOIN db_plan_dim DB_Plan_Dimension on  Shortcut_to_Pension_Popu_Elig_Current_All.client_id=DB_Plan_Dimension.client_id
and Shortcut_to_Pension_Popu_Elig_Current_All.PLAN_ID=DB_Plan_Dimension.plan_id
left outer join PRSN_PLANSTAT_DIMN01 on PRSN_PLANSTAT_DIMN01.PLAN_ID=Shortcut_to_Pension_Popu_Elig_Current_All.PLAN_ID
and PRSN_PLANSTAT_DIMN01.PRSN_INTN_ID=Shortcut_to_Pension_Popu_Elig_Current_All.PRSN_INTN_ID
and PRSN_PLANSTAT_DIMN01.platform_id=Shortcut_to_Pension_Popu_Elig_Current_All.platform_id
inner join Exp_Calen_yza cal on cal.cal_dt between Person.begin_date and Person.end_date
where PRSN_PLANSTAT_DIMN01.PRSN_INTN_ID is null ),

Exp_db_ppt_1_elig_smry_yza as (SELECT client_id,plan_id,plan_desc,
person_internal_id, fully_vested_date,last_day(fully_vested_date) AS reporting_month_dt, extract(year from fully_vested_date) as reporting_year, employment_status AS empl_stat, person_reason_code AS prsn_rsn_cd, cal_dt as rpt_fltr_1_desc,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,
cast('Participant_Data' as VARCHAR) AS smry_dimn_type, cast('Vested_NonVested' as VARCHAR) AS smry_dimn_value
FROM Pension_Participant_Data WHERE upper(employment_status) ='ACTIVE' and fully_vested_date >= '1900-01-01'
),

Exp_db_ppt_2_elig_smry_yza as (SELECT client_id,
plan_id, plan_desc, person_internal_id, last_day(cal_dt) AS reporting_month_dt, 
employment_status AS empl_stat, 
person_reason_code AS prsn_rsn_cd,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,cal_dt as rpt_fltr_1_desc,
cast('Participant_Data' as VARCHAR) AS smry_dimn_type,
cast('Deferred_Vested' as VARCHAR) AS smry_dimn_value
FROM Pension_Participant_Data
WHERE is_deferred_vested ='Y' and begin_date >= '1800-01-01'
),

Exp_db_ppt_3_elig_smry_yza as (SELECT client_id,platform_person_internal_id,
payment_id,plan_id,payment_year,reporting_month_dt,plan_desc, person_reason_code AS prsn_rsn_cd,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,employment_status AS empl_stat, cal_dt as rpt_fltr_1_desc,
cast('Participant_Data' as VARCHAR) AS smry_dimn_type,
cast('In_Payment' as VARCHAR) AS smry_dimn_value
FROM payment_attribute_join
),

Qry_Participant_Data_Count_1 AS (select plan_desc AS Plan, reporting_month_dt AS Reporting_Month, client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5, plan_id AS Plan_ID, empl_stat,
'Active - Vested' AS Category,
last_day(<<$pAsOfDate>>) AS To_Date,
DATE_TRUNC('month',date(<<$pAsOfDate>>)) AS From_Date,
COUNT(DISTINCT CONCAT(person_internal_id, plan_id)) as Count0
    
FROM Exp_db_ppt_1_elig_smry_yza
WHERE reporting_month_dt <= (last_day(<<$pAsOfDate>>))::date AND
    smry_dimn_value = 'Vested_NonVested' 
    AND smry_dimn_type = 'Participant_Data'
	AND prsn_rsn_cd <> 'N'
	AND rpt_fltr_1_desc = last_day(<<$pAsOfDate>>)
	AND upper(empl_stat) IN ('ACTIVE')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),

Qry_Participant_Data_Count_2 AS (SELECT plan_desc AS Plan, reporting_month_dt AS Reporting_Month,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5, plan_id AS Plan_ID,empl_stat,
'Active - Not Vested' AS Category, 
    last_day(<<$pAsOfDate>>) AS To_Date,
    DATE_TRUNC('month',date(<<$pAsOfDate>>)) AS From_Date, 
COUNT(DISTINCT CONCAT(person_internal_id, plan_id)) as Count0
FROM Exp_db_ppt_1_elig_smry_yza
WHERE reporting_month_dt > (last_day(<<$pAsOfDate>>))::date AND 
smry_dimn_value = 'Vested_NonVested' AND
smry_dimn_type = 'Participant_Data' AND
prsn_rsn_cd <> 'N' 
AND rpt_fltr_1_desc = last_day(<<$pAsOfDate>>)
AND upper(empl_stat) IN ('ACTIVE')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),

Qry_Participant_Data_Count_3 AS (SELECT plan_desc AS Plan, reporting_month_dt AS Reporting_Month, client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5, plan_id AS Plan_ID,empl_stat,
'In Payment' AS Category, 
    last_day(<<$pAsOfDate>>) AS To_Date, 
    DATE_TRUNC('month',date(<<$pAsOfDate>>)) AS From_Date, 
--COUNT(DISTINCT(CONCAT(CONCAT(platform_person_internal_id,plan_id),payment_year)) ) as Count0
  COUNT(DISTINCT(CONCAT(platform_person_internal_id,plan_id)) ) as Count0
FROM Exp_db_ppt_3_elig_smry_yza
WHERE reporting_month_dt = (last_day(<<$pAsOfDate>>))::date
 AND rpt_fltr_1_desc = last_day(<<$pAsOfDate>>)
 --AND upper(empl_stat) IN ('INACTIVE', 'NON-EMPLOYEE', 'ACTIVE')
AND smry_dimn_value = 'In_Payment' AND smry_dimn_type = 'Participant_Data' AND prsn_rsn_cd <> 'N' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),

Qry_Participant_Data_Count_4 AS (SELECT plan_desc AS Plan, reporting_month_dt AS Reporting_Month, client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5, plan_id AS Plan_ID,empl_stat,
'Deferred Vested' AS Category, 
    last_day(<<$pAsOfDate>>) AS To_Date, 
    DATE_TRUNC('month',date(<<$pAsOfDate>>)) AS From_Date, 
COUNT(DISTINCT CONCAT(person_internal_id, plan_id)) as Count0
FROM Exp_db_ppt_2_elig_smry_yza
WHERE reporting_month_dt = (last_day(<<$pAsOfDate>>))::date 
AND rpt_fltr_1_desc = last_day(<<$pAsOfDate>>)
 --AND upper(empl_stat) IN ('INACTIVE', 'NON-EMPLOYEE', 'ACTIVE')
AND smry_dimn_value = 'Deferred_Vested' AND smry_dimn_type = 'Participant_Data' AND prsn_rsn_cd <> 'N' AND 'Deferred Vested' <> '' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),

Query1 AS 
    (
    SELECT
        Union1.Plan AS Plan, 
        Union1.Reporting_Month AS Reporting_Month, Union1.client_id_string,Union1.salary_range,Union1.age_range,Union1.tenure_range,
        Union1.gender,Union1.hourly_salary_status,Union1.fulltime_parttime_status,Union1.union_name,Union1.row_level_security_filter_value,
        Union1.custom_filter_1,Union1.custom_filter_2,Union1.custom_filter_3,Union1.custom_filter_4,Union1.custom_filter_5,Union1.Plan_ID AS Plan_ID,
		Union1.empl_stat,
        Union1.Category AS Category, 
        Union1.To_Date AS To_Date, 
        Union1.From_Date AS From_Date, 
		Union1.Count0 AS Count0
    FROM
        (
        SELECT
            *
        FROM
            Qry_Participant_Data_Count_1
        
        UNION
        
        SELECT
            *
        FROM
            Qry_Participant_Data_Count_2
        ) Union1(Plan,Reporting_Month,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,Plan_ID,empl_stat,Category,To_Date,From_Date,Count0) 
    GROUP BY 
        Union1.Plan, 
        Union1.Count0, 
        Union1.Reporting_Month, 
        Union1.Category, 
        Union1.To_Date, 
        Union1.From_Date, 
        Union1.Plan_ID,Union1.client_id_string,Union1.salary_range,Union1.age_range,Union1.tenure_range,Union1.gender,Union1.hourly_salary_status,Union1.fulltime_parttime_status,Union1.union_name,Union1.row_level_security_filter_value,Union1.custom_filter_1,Union1.custom_filter_2,Union1.custom_filter_3,Union1.custom_filter_4,Union1.custom_filter_5,empl_stat
    ), 
Query2 AS 
    (
    SELECT
        Union2.Plan AS Plan, 
        Union2.Reporting_Month AS Reporting_Month, Union2.client_id_string,Union2.salary_range,Union2.age_range,Union2.tenure_range,
        Union2.gender,Union2.hourly_salary_status,Union2.fulltime_parttime_status,Union2.union_name,Union2.row_level_security_filter_value,
        Union2.custom_filter_1,Union2.custom_filter_2,Union2.custom_filter_3,Union2.custom_filter_4,Union2.custom_filter_5,Union2.Plan_ID AS Plan_ID,
        Union2.empl_stat,
		Union2.Category AS Category, 
        Union2.To_Date AS To_Date, 
        Union2.From_Date AS From_Date, 
		Union2.Count0 AS Count0
    FROM
        (
        SELECT
            *
        FROM
            Qry_Participant_Data_Count_3
        
        UNION
        
        SELECT
            *
        FROM
            Qry_Participant_Data_Count_4
        ) Union2(Plan,Reporting_Month,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,Plan_ID,empl_stat,Category,To_Date,From_Date,Count0) 
    GROUP BY 
        Union2.Plan, 
        Union2.Count0, 
        Union2.Reporting_Month, 
        Union2.Category, 
        Union2.To_Date, 
        Union2.From_Date, Union2.Plan_ID,Union2.client_id_string,Union2.salary_range,Union2.age_range,Union2.tenure_range,Union2.gender,Union2.hourly_salary_status,Union2.fulltime_parttime_status,Union2.union_name,Union2.row_level_security_filter_value,Union2.custom_filter_1,Union2.custom_filter_2,Union2.custom_filter_3,Union2.custom_filter_4,Union2.custom_filter_5,empl_stat
		
    ),
Union3 AS (
    SELECT * 
    FROM Query1
    UNION
    SELECT * 
    FROM Query2
),
Final AS (
    SELECT
        Plan,
        Count0,
        Reporting_Month,
        CASE 
            WHEN '00994' IN (1881, 5987) AND Category = 'Active - Not Vested' THEN 'Members in Waiting Period'
            WHEN NOT ('00994' IN (1881, 5987)) AND Category = 'Active - Not Vested' THEN Category
            ELSE Category
        END AS Final_Category,
        To_Date,
        From_Date,
        Plan_ID,
		client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,empl_stat
    FROM Union3
)

SELECT
    Plan,
    SUM(Count0) AS Count0,
    Final_Category AS Category,
    To_Date,
    From_Date,
    Plan_ID,
	client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,empl_stat,
    'Y' AS In_Payment
FROM Final
GROUP BY 
    Plan,
    Final_Category,
    To_Date,
    From_Date,
    Plan_ID,client_id_string,salary_range,age_range,tenure_range,gender,hourly_salary_status,fulltime_parttime_status,union_name,row_level_security_filter_value,custom_filter_1,custom_filter_2,custom_filter_3,custom_filter_4,custom_filter_5,empl_stat
