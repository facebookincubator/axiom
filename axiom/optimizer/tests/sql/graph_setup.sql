-- People and who knows whom. Alice and Bob know each other, Bob knows Carol,
-- Carol knows Dave, and Dave knows himself. Alice and Carol live in Paris,
-- Bob lives in Rome, and Dave lives in New York, which is not a listed city.
-- Alice and Carol work at Acme, and Dave works at Globex. Acme has offices in
-- Paris and Rome: Alice and Carol work in Paris, and Bob in Rome. Among
-- employees, Ann manages Bob, who manages Cid and Dee, and Eve and Fay manage
-- each other.
CREATE TABLE person AS
SELECT * FROM (
  VALUES
    (1, 'Alice', 'PAR'),
    (2, 'Bob', 'ROM'),
    (3, 'Carol', 'PAR'),
    (4, 'Dave', 'NYC')
) AS t(id, name, city)
----
CREATE TABLE knows AS
SELECT * FROM (
  VALUES
    (1, 2, 2010),
    (2, 1, 2010),
    (2, 3, 2015),
    (3, 4, 2020),
    (4, 4, 2000)
) AS t(src, dst, since)
----
CREATE TABLE city AS
SELECT * FROM (
  VALUES
    ('PAR', 'Paris'),
    ('ROM', 'Rome')
) AS t(code, name)
----
CREATE TABLE company AS
SELECT * FROM (
  VALUES
    (1, 'Acme'),
    (2, 'Globex')
) AS t(id, name)
----
CREATE TABLE works_at AS
SELECT * FROM (
  VALUES
    (1, 1),
    (3, 1),
    (4, 2)
) AS t(person, company)
----
CREATE TABLE office AS
SELECT * FROM (
  VALUES
    (1, 'PAR'),
    (1, 'ROM')
) AS t(company, city)
----
CREATE TABLE works_in AS
SELECT * FROM (
  VALUES
    (1, 1, 'PAR'),
    (2, 1, 'ROM'),
    (3, 1, 'PAR')
) AS t(person, company, city)
----
CREATE TABLE employee AS
SELECT * FROM (
  VALUES
    (1, CAST(NULL AS INTEGER), 'Ann'),
    (2, 1, 'Bob'),
    (3, 2, 'Cid'),
    (4, 2, 'Dee'),
    (5, 6, 'Eve'),
    (6, 5, 'Fay')
) AS t(id, manager, name)
