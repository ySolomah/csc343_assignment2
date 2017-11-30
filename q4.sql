-- Left-right

SET SEARCH_PATH TO parlgov;
drop table if exists q4 cascade;

-- You must not change this table definition.


CREATE TABLE q4(
        countryName VARCHAR(50),
        r0_2 INT,
        r2_4 INT,
        r4_6 INT,
        r6_8 INT,
        r8_10 INT
);

-- You may find it convenient to do this for each of the views
-- that define your intermediate steps.  (But give them better names!)
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS party_position_two CASCADE;
DROP VIEW IF EXISTS party_values CASCADE;
DROP VIEW IF EXISTS party_and_country CASCADE;
DROP VIEW IF EXISTS countries_not_in CASCADE;
DROP VIEW IF EXISTS group_parties CASCADE;
DROP VIEW IF EXISTS total_left_right CASCADE;
DROP VIEW IF EXISTS q4_result CASCADE;


-- Define views for your intermediate steps here.
SET SEARCH_PATH TO parlgov;


-- Set null values to artibrary value not in range 0 to 10

CREATE VIEW party_position_two AS
  (SELECT party_id,
          11 AS left_right
   FROM party_position
   WHERE left_right IS NULL
   UNION SELECT party_id,
                left_right
   FROM party_position
   WHERE left_right IS NOT NULL);


-- Set values for party position

CREATE VIEW party_values as
  ( SELECT 1 AS r0_2, 0 AS r2_4, 0 AS r4_6, 0 AS r6_8, 0 AS r8_10, party_id
   FROM party_position_two
   WHERE left_right >= 0
     AND left_right < 2
   UNION SELECT 0 AS r0_2, 1 AS r2_4, 0 AS r4_6, 0 AS r6_8, 0 AS r8_10, party_id
   FROM party_position_two
   WHERE left_right >= 2
     AND left_right < 4
   UNION SELECT 0 AS r0_2, 0 AS r2_4, 1 AS r4_6, 0 AS r6_8, 0 AS r8_10, party_id
   FROM party_position_two
   WHERE left_right >= 4
     AND left_right < 6
   UNION SELECT 0 AS r0_2, 0 AS r2_4, 0 AS r4_6, 1 AS r6_8, 0 AS r8_10, party_id
   FROM party_position_two
   WHERE left_right >= 6
     AND left_right < 8
   UNION SELECT 0 AS r0_2, 0 AS r2_4, 0 AS r4_6, 0 AS r6_8, 1 AS r8_10, party_id
   FROM party_position_two
   WHERE left_right >= 8
     AND left_right <= 10 );


-- Merge party, country together

CREATE VIEW party_and_country AS
SELECT party_id,
       country_id,
       r0_2,
       r2_4,
       r4_6,
       r6_8,
       r8_10
FROM party_values,
     party
WHERE party_id = party.id;


-- Find countries without a party position

CREATE VIEW countries_not_in AS
SELECT country.id AS country_id,
       0 AS r0_2,
       0 AS r2_4,
       0 AS r4_6,
       0 AS r6_8,
       0 AS r8_10
FROM country
WHERE country.id NOT IN
    (SELECT country_id
     FROM party_and_country);


-- Group by for the countries with party positions

CREATE VIEW group_parties AS
SELECT country_id,
       sum(r0_2) AS r0_2,
       sum(r2_4) AS r2_4,
       sum(r4_6) AS r4_6,
       sum(r6_8) AS r6_8,
       sum(r8_10) AS r8_10
FROM party_and_country
GROUP BY country_id;


-- All countries (with party positions and without party positions)

CREATE VIEW total_left_right AS
  (SELECT *
   FROM group_parties
   UNION SELECT *
   FROM countries_not_in);


-- Final Result, formatting and ordering

CREATE VIEW q4_result AS
SELECT name AS countryName,
       r0_2,
       r2_4,
       r4_6,
       r6_8,
       r8_10
FROM country,
     total_left_right
WHERE country.id = country_id
ORDER BY countryName;


-- the answer to the query

INSERT INTO q4
SELECT *
FROM q4_result;

