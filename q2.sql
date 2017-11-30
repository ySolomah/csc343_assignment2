-- Winners

SET SEARCH_PATH TO parlgov;
drop table if exists q2 cascade;

-- You must not change this table definition.

create table q2(
countryName VARCHaR(100),
partyName VARCHaR(100),
partyFamily VARCHaR(100),
wonElections INT,
mostRecentlyWonElectionId INT,
mostRecentlyWonElectionYear INT
);


-- You may find it convenient to do this for each of the views
-- that define your intermediate steps.  (But give them better names!)
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS election_result_two CASCADE;
DROP VIEW IF EXISTS num_votes CASCADE;
DROP VIEW IF EXISTS num_votes_total CASCADE;
DROP VIEW IF EXISTS winners_votes CASCADE;
DROP VIEW IF EXISTS winners CASCADE;
DROP VIEW IF EXISTS win_group CASCADE;
DROP VIEW IF EXISTS winner_sum CASCADE;
DROP VIEW IF EXISTS total_partipcations_per_country CASCADE;
DROP VIEW IF EXISTS total_wins_per_country CASCADE;
DROP VIEW IF EXISTS avg_per_country CASCADE;
DROP VIEW IF EXISTS total_win_party CASCADE;
DROP VIEW IF EXISTS total_win_party_country CASCADE;
DROP VIEW IF EXISTS total_elections_per_country CASCADE;
DROP VIEW IF EXISTS avg_per_party CASCADE;
DROP VIEW IF EXISTS triple_winners CASCADE;
DROP VIEW IF EXISTS participated_in CASCADE;
DROP VIEW IF EXISTS number_won CASCADE;
DROP VIEW IF EXISTS total_wins CASCADE;
DROP VIEW IF EXISTS final_part_one CASCADE;
DROP VIEW IF EXISTS final_part_two CASCADE;
DROP VIEW IF EXISTS total_wins_in_country CASCADE;
DROP VIEW IF EXISTS total_parties_in_country CASCADE;
DROP VIEW IF EXISTS q2_result CASCADE;
DROP VIEW IF EXISTS total_win_party_without_country CASCADE;	



-- Define views for your intermediate steps here.

-- Repalce null votes with 0 votes

CREATE VIEW election_result_two AS
  (SELECT election_result.id,
          election_id,
          party_id,
          alliance_id,
          seats,
          0 AS votes,
          e_date
   FROM election,
        election_result
   WHERE election.id = election_result.election_id
     AND votes IS NULL
   UNION SELECT election_result.id,
                election_id,
                party_id,
                alliance_id,
                seats,
                votes,
                e_date
   FROM election,
        election_result
   WHERE election.id = election_result.election_id
     AND votes IS NOT NULL);


-- Find number of votes for each alliance

CREATE VIEW num_votes AS
SELECT sum(votes) AS add_to_total,
       alliance_id AS add_id
FROM election_result_two
WHERE alliance_id IS NOT NULL
GROUP BY alliance_id;


-- Find all possible combinations of total votes

CREATE VIEW num_votes_total AS
  (SELECT votes + add_to_total AS total_votes,
          id,
          election_id
   FROM election_result_two,
        num_votes
   WHERE id = add_id
   UNION ALL SELECT votes AS total_votes,
                    id,
                    election_id
   FROM election_result_two);


-- Find highest votes for each election

CREATE VIEW winners_votes AS
SELECT max(total_votes) AS winner_count,
       election_id
FROM num_votes_total
GROUP BY election_id;


-- Find id which matched to the highest vote count

CREATE VIEW winners AS
SELECT id
FROM winners_votes,
     num_votes_total
WHERE total_votes = winner_count
  AND winners_votes.election_id = num_votes_total.election_id;


-- Find groups which matched id (alliance or personal)

CREATE VIEW win_group AS
SELECT distinct(election_result_two.id)
FROM winners,
     election_result_two
WHERE election_result_two.id = winners.id
  OR election_result_two.alliance_id = winners.id;


-- Group results with election information

CREATE VIEW winner_sum AS
SELECT election_result_two.id,
       election_id,
       party_id
FROM election_result_two,
     win_group
WHERE election_result_two.id = win_group.id;


-- Find average for each country (ASSUMPTION ON PIAZZA POST -- AVG_PER_COUNTRY = TOTAL_PARTIES_WON_SUM / TOTAL_PARTY_PARTICIPATION)

CREATE VIEW total_wins_in_country AS
SELECT country_id,
       count(*) AS num_total_wins
FROM winner_sum,
     party
WHERE party_id=party.id
GROUP BY country_id;


CREATE VIEW total_parties_in_country AS
SELECT country_id,
       count(*) AS total_parties
FROM party
GROUP BY country_id;


CREATE VIEW avg_per_country AS
SELECT CAST(num_total_wins AS float) / CAST (total_parties AS float) AS win_rate,
                                            country_id
FROM total_parties_in_country
NATURAL JOIN total_wins_in_country;


-- Find number of wins per party

CREATE VIEW total_win_party_without_country AS
SELECT party_id,
       count(*) AS total_party_win
FROM win_group
NATURAL JOIN election_result_two
GROUP BY party_id;


CREATE VIEW total_win_party AS
SELECT party_id,
       total_party_win,
       country_id
FROM total_win_party_without_country,
     party
WHERE total_win_party_without_country.party_id=party.id;


 -- Find groups with triple the national average and join with party for attributes

CREATE VIEW number_won AS
SELECT party_id,
       total_party_win
FROM total_win_party,
     avg_per_country
WHERE total_win_party.country_id = avg_per_country.country_id
  AND total_party_win > 3*win_rate;


-- Find latest victory

CREATE VIEW total_wins AS
SELECT number_won.party_id,
       total_party_win,
       election_result_two.election_id::int AS mostRecentlyWonElectionId,
       date_part('year', e_date) AS mostRecentlyWonElectionYear
FROM number_won,
     win_group,
     election_result_two
WHERE win_group.id = election_result_two.id
  AND election_result_two.party_id = number_won.party_id
  AND election_result_two.e_date >= ALL
    ( SELECT e_date
     FROM number_won T0,
          win_group T1,
          election_result_two T2
     WHERE T1.id = T2.id
       AND T0.party_id = number_won.party_id
       AND T2.party_id = T0.party_id) ;


-- Join with party (null and non-null cases)

CREATE VIEW final_part_one AS
SELECT country.name AS countryName,
       party.name AS partyName,
       family AS partyFamily,
       total_party_win AS wonElections,
       mostRecentlyWonElectionId,
       mostRecentlyWonElectionYear::int
FROM country,
     party,
     total_wins,
     party_family
WHERE country.id = party.country_id
  AND party.id = total_wins.party_id
  AND party.id = party_family.party_id
ORDER BY countryName ASC,
         wonElections ASC,
         partyName DESC;


CREATE VIEW final_part_two AS
SELECT country.name AS countryName,
       party.name AS partyName,
       NULL::text AS partyFamily,
       total_party_win AS wonElections,
       mostRecentlyWonElectionId,
       mostRecentlyWonElectionYear::int
FROM country,
     party,
     total_wins
WHERE country.id = party.country_id
  AND party.id = total_wins.party_id
  AND party.id NOT IN
    (SELECT party_id
     FROM party_family)
ORDER BY countryName ASC,
         wonElections ASC,
         partyName DESC;


-- Final statement and ordering by

CREATE VIEW q2_result AS
SELECT *
FROM
  (SELECT *
   FROM final_part_one
   UNION SELECT *
   FROM final_part_two) temp_result
ORDER BY countryName ASC,
         wonElections ASC,
         partyName DESC;


-- the answer to the query

INSERT INTO q2
SELECT *
FROM q2_result;

