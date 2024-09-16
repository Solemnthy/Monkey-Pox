-- ------------------------------
-- data cleaning and preparation
-- ------------------------------

-- update the date column to a proper date format
update monkeypox
set `date` = str_to_date(`date`, '%Y-%m-%d');

-- alter the column type for date consistency
alter table monkeypox
modify column `date` date;

-- ------------------------------
-- data exploration and analysis
-- ------------------------------

-- 1. check for rows with potential duplicates
with duplicate_finder as (
    select *,
		row_number() over(partition by location, `date`,
        total_cases, total_deaths,
		new_cases, new_deaths, new_cases_smoothed,
		new_deaths_smoothed, new_cases_per_million,
		total_cases_per_million, new_cases_smoothed_per_million,
		new_deaths_per_million, total_deaths_per_million,
		new_deaths_smoothed_per_million
		order by location, `date`) as row_num
    from monkeypox
)
select *,
       row_num
from duplicate_finder
where row_num > 1;

-- 2. summary of total deaths by location
select location,
       sum(total_deaths) as total_deaths
from monkeypox
group by location;

-- 3. check for missing values in key columns
select 
    count(*) as total_rows,
    sum(case when total_cases is null then 1 else 0 end) as null_total_cases,
    sum(case when total_deaths is null then 1 else 0 end) as null_total_deaths,
    sum(case when new_cases is null then 1 else 0 end) as null_new_cases,
    sum(case when new_deaths is null then 1 else 0 end) as null_new_deaths
from monkeypox;

-- 4. global summary of cases and deaths
select 
    sum(total_cases) as global_total_cases, 
    sum(total_deaths) as global_total_deaths
from monkeypox;

-- 5. new cases and deaths by location and date
select 
    location, 
    date, 
    sum(new_cases) as total_new_cases, 
    sum(new_deaths) as total_new_deaths
from monkeypox
group by location, date
order by date asc;

-- 6. top 10 locations by maximum cases
select 
    location, 
    max(total_cases) as total_cases, 
    max(total_deaths) as total_deaths
from monkeypox
group by location
order by total_cases desc
limit 10;

-- 7. average new cases per million by location
select 
    location, 
    avg(new_cases_per_million) as avg_new_cases_per_million
from monkeypox
group by location
order by avg_new_cases_per_million desc;

-- 8. weekly new cases trend for each location
select 
    location,
    date,
    sum(new_cases) over (partition by location order by date asc rows between 6 preceding and current row) as weekly_new_cases
from monkeypox;

-- 9. monthly averages of new cases and deaths
with monthly_averages as (
    select
        date_format(date, '%Y-%m') as month,
        avg(new_cases) as avg_new_cases,
        avg(new_deaths) as avg_new_deaths
    from monkeypox
    group by month
)
select 
    month,
    avg_new_cases,
    avg_new_deaths
from monthly_averages
order by month desc;

-- 10. correlation between new cases and new deaths
select 
    corr(new_cases, new_deaths) as correlation_new_cases_deaths
from monkeypox;

-- 11. locations with the highest new cases per million
select 
    location,
    max(new_cases_per_million) as max_new_cases_per_million
from monkeypox
group by location
order by max_new_cases_per_million desc
limit 10;

-- 12. global trend of new cases and deaths over time
select 
    date_format(date, '%Y-%m') as month,
    sum(new_cases) as total_new_cases,
    sum(new_deaths) as total_new_deaths
from monkeypox
group by month
order by month desc;

-- 13. significant outbreaks with sharp increases in new cases
with significant_outbreaks as (
    select 
        location, 
        date,
        new_cases,
        lag(new_cases, 1) over (partition by location order by date) as previous_day_cases,
        new_cases - lag(new_cases, 1) over (partition by location order by date) as change_in_cases
    from monkeypox
)
select 
    location, 
    date,
    new_cases,
    previous_day_cases,
    change_in_cases
from significant_outbreaks
where change_in_cases > 1000  -- example threshold for significant increase
order by date desc;

-- 14. analyze the most deadly regions
select
    location,
    avg(total_deaths) as avg_total_deaths
from monkeypox
group by location
order by avg_total_deaths desc
limit 10;

-- 15. summary report of recent significant outbreaks
select
    location,
    date_format(date, '%Y-%m-%d') as date,
    total_cases,
    total_deaths,
    new_cases,
    new_deaths
from monkeypox
where date >= curdate() - interval 60 day
order by new_cases desc;
