-- Sequences

SET SEARCH_PATH TO parlgov;
drop table if exists q6 cascade;

-- You must not change this table definition.

CREATE TABLE q6(
        countryName VARCHAR(50),
        cabinetId INT, 
        startDate DATE,
        endDate DATE,
        pmParty VARCHAR(100)
);

-- You may find it convenient to do this for each of the views
-- that define your intermediate steps.  (But give them better names!)
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS cabinets_over_time_one CASCADE;
DROP VIEW IF EXISTS cabinets_over_time_two CASCADE;
DROP VIEW IF EXISTS cabinets_over_time_three CASCADE;
DROP VIEW IF EXISTS cabinets_over_time CASCADE;
DROP VIEW IF EXISTS cabinets_parties CASCADE;
DROP VIEW IF EXISTS temp_one CASCADE;
DROP VIEW IF EXISTS temp_subtract CASCADE;
DROP VIEW IF EXISTS temp_two CASCADE;
DROP VIEW IF EXISTS temp_final CASCADE;
DROP VIEW IF EXISTS with_country CASCADE;
DROP VIEW IF EXISTS with_pm CASCADE;
DROP VIEW IF EXISTS without_pm CASCADE;
DROP VIEW IF EXISTS q6_result CASCADE;


-- Define views for your intermediate steps here.
SET SEARCH_PATH TO parlgov;


-- Find cabinets formed over time (not most recent)

CREATE VIEW cabinets_over_time_one AS
  (SELECT T1.id,
          T1.country_id,
          T1.start_date,
          T2.start_date AS end_date
   FROM cabinet T1,
        cabinet T2
   WHERE T1.country_id = T2.country_id
     AND T1.id = T2.previous_cabinet_id
 );


-- Find most recent cabinets

CREATE VIEW cabinets_over_time_two AS
  (SELECT id,
          country_id,
          start_date
   FROM cabinet
   EXCEPT SELECT id,
                 country_id,
                 start_date
   FROM cabinets_over_time_one);


-- Set NULL as end_date for most recent cabinets

CREATE VIEW cabinets_over_time_three AS
  (SELECT id,
          country_id,
          start_date,
          NULL AS end_date
   FROM cabinets_over_time_two);


-- Join most recent and not most recent cabinet sets

CREATE VIEW cabinets_over_time AS
  (SELECT *
   FROM cabinets_over_time_three
   UNION SELECT *
   FROM cabinets_over_time_one);


-- Join with each cabinet with party

CREATE VIEW cabinets_parties AS
SELECT cabinets_over_time.id,
       country_id,
       start_date,
       end_date,
       party_id,
       pm
FROM cabinets_over_time,
     cabinet_party
WHERE cabinets_over_time.id = cabinet_party.cabinet_id;


-- Find party that does fill PM

CREATE VIEW fills_pm AS
SELECT cabinets_parties.id,
       cabinets_parties.country_id,
       start_date,
       end_date,
       party_id,
       name
FROM cabinets_parties,
     party
WHERE pm='t'
  AND party_id = party.id;


-- Find cabinets without a party filling pm position

CREATE VIEW temp_subtract AS
SELECT T1.id,
       T1.country_id,
       T1.start_date,
       T1.end_date,
       T1.party_id,
       T1.pm
FROM cabinets_parties T1
WHERE pm='t';


CREATE VIEW does_not_fill_pm AS
SELECT id,
       country_id,
       start_date,
       end_date,
       party_id,
       NULL AS name
FROM
  (SELECT *
   FROM cabinets_parties
   EXCEPT SELECT *
   FROM temp_subtract) TEMP
WHERE pm = 'f';


-- Join with pm and without pm

CREATE VIEW temp_final AS
  (SELECT *
   FROM fills_pm
   UNION SELECT *
   FROM does_not_fill_pm);


-- Join for country name

CREATE VIEW with_country AS
SELECT country.name AS countryName,
       temp_final.id AS cabinetId,
       start_date AS startDate,
       end_date AS endDate,
       temp_final.name AS pmParty
FROM temp_final,
     country
WHERE temp_final.country_id = country.id;


-- Separate the with and without pm cases

CREATE VIEW with_pm AS
SELECT *
FROM with_country
WHERE pmParty IS NOT NULL;


CREATE VIEW without_pm AS
SELECT *
FROM with_country
WHERE with_country.cabinetId NOT IN
    (SELECT T1.cabinetId
     FROM with_pm T1);


-- Final result with cabinets with and without pm, with ordering

CREATE VIEW q6_result AS
SELECT *
FROM
  (SELECT *
   FROM with_pm
   UNION SELECT *
   FROM without_pm) TEMP
ORDER BY countryName DESC,
         startDate ASC;


-- the answer to the query

INSERT INTO q6
SELECT *
FROM q6_result;
