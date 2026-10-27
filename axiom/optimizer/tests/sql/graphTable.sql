-- setup_file: graph_setup.sql

-- define graph
GRAPH g AS (
  VERTEX TABLES (
    person KEY (id) LABEL Person PROPERTIES (id, name),
    city KEY (code) LABEL City PROPERTIES (code, name, upper(name) AS upper_name),
    company KEY (id) LABEL Company PROPERTIES (id, name),
    office KEY (company, city) LABEL Office PROPERTIES (city))
  EDGE TABLES (
    knows KEY (src, dst)
      SOURCE KEY (src) REFERENCES person (id)
      DESTINATION KEY (dst) REFERENCES person (id)
      LABEL Knows,
    person AS lives_in KEY (id)
      SOURCE KEY (id) REFERENCES person (id)
      DESTINATION KEY (city) REFERENCES city (code)
      LABEL LivesIn PROPERTIES (city),
    works_at KEY (person)
      SOURCE KEY (person) REFERENCES person (id)
      DESTINATION KEY (company) REFERENCES company (id)
      LABEL WorksAt,
    works_in KEY (person)
      SOURCE KEY (person) REFERENCES person (id)
      DESTINATION KEY (company, city) REFERENCES office (company, city)
      LABEL WorksIn))
-- end_define

-- Who knows whom, and since when.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 2010),
--     ('Bob', 'Alice', 2010),
--     ('Bob', 'Carol', 2015),
--     ('Carol', 'Dave', 2020),
--     ('Dave', 'Dave', 2000)
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[k IS Knows]-> (b IS Person)
  COLUMNS (a.name AS a, b.name AS b, k.since))
----
-- Who is known by whom: a left-pointing edge leaves the vertex on its right.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob'),
--     ('Bob', 'Alice'),
--     ('Bob', 'Carol'),
--     ('Carol', 'Dave'),
--     ('Dave', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (b IS Person) <-[IS Knows]- (a IS Person)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Whom people other than Alice have known since after 2010, among people
-- with a larger ID: a vertex predicate, an edge predicate, and a condition
-- across elements.
-- duckdb: VALUES ('Bob', 'Carol'), ('Carol', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person WHERE a.name <> 'Alice')
    -[k IS Knows WHERE k.since > 2010]-> (b IS Person)
  WHERE a.id < b.id
  COLUMNS (a.name AS a, b.name AS b))
----
-- Who knows someone who knows them back: a name that appears twice binds one
-- vertex. Dave's path follows his one edge twice.
-- duckdb: VALUES ('Alice', 'Bob', 2), ('Bob', 'Alice', 2), ('Dave', 'Dave', 2)
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH p = (a IS Person) -[IS Knows]-> (b IS Person) -[IS Knows]-> (a)
  COLUMNS (a.name AS a, b.name AS b, path_length(p) AS n))
----
-- Who knows whom through someone: paths that share a name join on it.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Alice'),
--     ('Alice', 'Bob', 'Carol'),
--     ('Bob', 'Alice', 'Bob'),
--     ('Bob', 'Carol', 'Dave'),
--     ('Carol', 'Dave', 'Dave'),
--     ('Dave', 'Dave', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS Knows]-> (b IS Person), (b) -[IS Knows]-> (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Who is a person and not a person: a label expression no table matches
-- matches nothing.
-- count 0
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH p = (a IS Person & !Person)
  COLUMNS (path_length(p) AS n))
----
-- Which cities there are: a path of one vertex. A property can be an
-- expression over its table's columns.
-- duckdb: VALUES ('PAR', 'PARIS'), ('ROM', 'ROME')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (c IS City)
  COLUMNS (c.code AS code, c.upper_name AS name))
----
-- Who lives where: an edge between vertices of different tables, backed by
-- the table of its source vertices. Dave's edge reaches no vertex, so it
-- matches nothing.
-- duckdb: VALUES ('Alice', 'Paris'), ('Bob', 'Rome'), ('Carol', 'Paris')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS LivesIn]-> (c IS City)
  COLUMNS (a.name AS person, c.name AS city))
----
-- Where people live, read from the city: a left-pointing edge reaches the
-- vertex on its left.
-- duckdb: VALUES ('Alice', 'Paris'), ('Bob', 'Rome'), ('Carol', 'Paris')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (c IS City) <-[IS LivesIn]- (a IS Person)
  COLUMNS (a.name AS person, c.name AS city))
----
-- Who lives in a person: an edge reaches only the vertex table it references.
-- count 0
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH p = (a IS Person) -[IS LivesIn]-> (b IS Person)
  COLUMNS (path_length(p) AS n))
----
-- Which city lives in a city: an edge matches only between the vertex tables
-- it connects.
-- count 0
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH p = (a IS City) -[IS LivesIn]-> (b IS City)
  COLUMNS (path_length(p) AS n))
----
-- Alice paired with every city: paths that share no name.
-- duckdb: VALUES ('Alice', 'Paris'), ('Alice', 'Rome')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person WHERE a.name = 'Alice'), (c IS City)
  COLUMNS (a.name AS person, c.name AS city))
----
-- Who knows the same person as whom: two paths that meet at one vertex.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Alice'),
--     ('Bob', 'Alice', 'Bob'),
--     ('Bob', 'Carol', 'Bob'),
--     ('Carol', 'Dave', 'Carol'),
--     ('Carol', 'Dave', 'Dave'),
--     ('Dave', 'Dave', 'Carol'),
--     ('Dave', 'Dave', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS Knows]-> (b IS Person), (c IS Person) -[IS Knows]-> (b)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Which person who lives somewhere is also a city: a name that appears twice
-- matches only its first table.
-- count 0
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH p = (a IS Person) -[IS LivesIn]-> (c IS City), (a IS City)
  COLUMNS (path_length(p) AS n))
----
-- Every person and city by name: a label disjunction matches the tables of
-- both labels. A property that only one of them defines is NULL for the
-- other.
-- duckdb:
--   VALUES
--     ('Alice', NULL),
--     ('Bob', NULL),
--     ('Carol', NULL),
--     ('Dave', NULL),
--     ('Paris', 'PAR'),
--     ('Rome', 'ROM')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (x IS Person | City)
  COLUMNS (x.name AS name, x.code AS code))
----
-- Every vertex by name: a pattern without a label matches every vertex table.
-- duckdb:
--   VALUES
--     ('Alice'), ('Bob'), ('Carol'), ('Dave'),
--     ('Paris'), ('Rome'),
--     ('Acme'), ('Globex'),
--     (NULL), (NULL)
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (x)
  COLUMNS (x.name AS name))
----
-- Whom people know and where they live: an edge label disjunction reaches
-- vertices of either table.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob'),
--     ('Bob', 'Alice'),
--     ('Bob', 'Carol'),
--     ('Carol', 'Dave'),
--     ('Dave', 'Dave'),
--     ('Alice', 'Paris'),
--     ('Bob', 'Rome'),
--     ('Carol', 'Paris')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS Knows | LivesIn]-> (b)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Who lives where, with neither end labeled: the edge's tables decide what
-- each end can match.
-- duckdb: VALUES ('Alice', 'Paris'), ('Bob', 'Rome'), ('Carol', 'Paris')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a) -[IS LivesIn]-> (b)
  COLUMNS (a.name AS person, b.name AS city))
----
-- Whom Bob knows or is known by: an undirected edge matches in either
-- direction. Bob and Alice know each other through two edges.
-- duckdb: VALUES ('Bob', 'Alice'), ('Bob', 'Alice'), ('Bob', 'Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person WHERE a.name = 'Bob') -[IS Knows]- (b IS Person)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Whom Dave knows or is known by: his edge to himself matches once.
-- duckdb: VALUES ('Dave', 'Carol'), ('Dave', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person WHERE a.name = 'Dave') -[IS Knows]- (b)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Who lives in each city, read undirected from the city.
-- duckdb: VALUES ('Paris', 'Alice'), ('Rome', 'Bob'), ('Paris', 'Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (c IS City) -[IS LivesIn]- (a)
  COLUMNS (c.name AS city, a.name AS person))
----
-- Who points at the same person or company as whom. Acme and Alice have the
-- same ID and are different vertices.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Alice'),
--     ('Bob', 'Alice', 'Bob'),
--     ('Bob', 'Carol', 'Bob'),
--     ('Carol', 'Dave', 'Carol'),
--     ('Carol', 'Dave', 'Dave'),
--     ('Dave', 'Dave', 'Carol'),
--     ('Dave', 'Dave', 'Dave'),
--     ('Alice', 'Acme', 'Alice'),
--     ('Alice', 'Acme', 'Carol'),
--     ('Carol', 'Acme', 'Alice'),
--     ('Carol', 'Acme', 'Carol'),
--     ('Dave', 'Globex', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS Knows | WorksAt]-> (b)
    <-[IS Knows | WorksAt]- (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Whom Bob knows or is known by, one path per person: ANY keeps one match per
-- pair of endpoints, though Bob and Alice know each other through two edges.
-- duckdb: VALUES ('Bob', 'Alice'), ('Bob', 'Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ANY (a IS Person WHERE a.name = 'Bob') -[IS Knows]- (b IS Person)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Whom Bob knows or is known by, up to two paths per person: SHORTEST 2 keeps
-- both of Bob's paths to Alice.
-- duckdb: VALUES ('Bob', 'Alice'), ('Bob', 'Alice'), ('Bob', 'Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH SHORTEST 2 (a IS Person WHERE a.name = 'Bob') -[IS Knows]- (b IS Person)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Whom Alice knows or is known by, once per person, and whom that person
-- knows: ANY keeps one of the two paths between Alice and Bob, and Bob keeps
-- both people he knows.
-- duckdb: VALUES ('Alice', 'Bob', 'Alice'), ('Alice', 'Bob', 'Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ANY (a IS Person WHERE a.name = 'Alice') -[IS Knows]- (b IS Person),
    (b) -[IS Knows]-> (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Who connects Alice to Alice through one other person, once per person on
-- each side: both paths have a selector and share the person in the middle.
-- duckdb: VALUES ('Alice', 'Bob', 'Alice')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ANY (a IS Person WHERE a.name = 'Alice') -[IS Knows]- (b IS Person),
    ANY (b) -[IS Knows]- (c IS Person WHERE c.name = 'Alice')
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Every Knows edge, read by two paths with a selector that share it.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Alice', 'Bob'),
--     ('Bob', 'Alice', 'Bob', 'Alice'),
--     ('Bob', 'Carol', 'Bob', 'Carol'),
--     ('Carol', 'Dave', 'Carol', 'Dave'),
--     ('Dave', 'Dave', 'Dave', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ANY (a IS Person) -[e IS Knows]-> (b IS Person),
    ANY (c IS Person) -[e]-> (d IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c, d.name AS d))
----
-- Who works in the same office as whom.
-- duckdb:
--   VALUES
--     ('Alice', 'PAR', 'Alice'),
--     ('Alice', 'PAR', 'Carol'),
--     ('Carol', 'PAR', 'Alice'),
--     ('Carol', 'PAR', 'Carol'),
--     ('Bob', 'ROM', 'Bob')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS WorksIn]-> (o IS Office)
    <-[IS WorksIn]- (b IS Person)
  COLUMNS (a.name AS a, o.city, b.name AS b))
----
-- Where Alice lives: a second path names her vertex again with a condition.
-- duckdb: VALUES ('Alice', 'Paris')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS LivesIn]-> (c IS City), (a WHERE a.name = 'Alice')
  COLUMNS (a.name AS a, c.name AS city))
----
-- Adjacent vertex patterns match one vertex: x is the person a stands for.
-- duckdb: VALUES ('Alice', 'Bob')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) (x WHERE x.name = 'Alice') -[IS Knows]-> (b)
  COLUMNS (a.name AS a, b.name AS b))
----
-- Adjacent vertex patterns match only tables both labels match: companies.
-- duckdb: VALUES ('Acme', 'Alice'), ('Acme', 'Carol'), ('Globex', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (v IS Person | Company) (w IS Company) <-[IS WorksAt]- (p)
  COLUMNS (w.name AS company, p.name AS p))
----
-- A subpath's condition applies to its match: whom Bob knows other than Alice.
-- duckdb: VALUES ('Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person WHERE a.name = 'Bob')
    ((x) -[IS Knows]-> (y) WHERE y.name <> 'Alice') (b)
  COLUMNS (b.name AS b))
----
-- The length of a subpath counts its edges.
-- duckdb: VALUES ('Bob', 1, 2)
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH p = (a IS Person WHERE a.name = 'Alice') (q = -[IS Knows]-> (b IS Person))
    -[IS LivesIn]-> (c IS City)
  COLUMNS (b.name AS b, path_length(q) AS q, path_length(p) AS p))
----
-- Who lives in a listed city, paired with every person.
-- duckdb:
--   SELECT a.name AS a, p.name AS p
--   FROM person a JOIN city c ON a.city = c.code CROSS JOIN person p
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[IS LivesIn]-> (c IS City), (p IS Person)
  COLUMNS (a.name AS a, p.name AS p))
----
-- Who reaches whom in two steps over two different edges: a trail follows
-- Dave's edge to himself once.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Alice'),
--     ('Alice', 'Bob', 'Carol'),
--     ('Bob', 'Alice', 'Bob'),
--     ('Bob', 'Carol', 'Dave'),
--     ('Carol', 'Dave', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH TRAIL (a IS Person) -[IS Knows]-> (b IS Person)
    -[IS Knows]-> (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Who follows one edge twice in a trail: a trail matches no path that binds
-- one edge pattern twice.
-- count 0
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH TRAIL (a IS Person) -[k IS Knows]-> (b IS Person) -[k]-> (c IS Person)
  COLUMNS (a.name AS a))
----
-- Who reaches whom in two steps through three different people.
-- duckdb: VALUES ('Alice', 'Bob', 'Carol'), ('Bob', 'Carol', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ACYCLIC (a IS Person) -[IS Knows]-> (b IS Person)
    -[IS Knows]-> (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Who knows someone living in a listed city, without meeting a vertex twice:
-- a person and a city are never the same vertex.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Rome'),
--     ('Bob', 'Alice', 'Paris'),
--     ('Bob', 'Carol', 'Paris')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ACYCLIC (a IS Person) -[IS Knows]-> (b IS Person)
    -[IS LivesIn]-> (c IS City)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Whom or where each person knows or works at, but for themselves: Alice and
-- Acme share the key 1 and are different vertices.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob'),
--     ('Bob', 'Alice'),
--     ('Bob', 'Carol'),
--     ('Carol', 'Dave'),
--     ('Alice', 'Acme'),
--     ('Carol', 'Acme'),
--     ('Dave', 'Globex')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ACYCLIC (a IS Person) -[IS Knows | WorksAt]-> (x IS Person | Company)
  COLUMNS (a.name AS a, x.name AS x))
----
-- Who reaches whom in two steps without meeting a person twice, but for
-- returning to the start: a simple path may end where it starts.
-- duckdb:
--   VALUES
--     ('Alice', 'Bob', 'Alice'),
--     ('Alice', 'Bob', 'Carol'),
--     ('Bob', 'Alice', 'Bob'),
--     ('Bob', 'Carol', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH SIMPLE (a IS Person) -[IS Knows]-> (b IS Person)
    -[IS Knows]-> (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Who knows the same person as whom over a different edge: a trail compares
-- edges whichever way the path follows them.
-- duckdb: VALUES ('Carol', 'Dave', 'Dave'), ('Dave', 'Dave', 'Carol')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH TRAIL (a IS Person) -[IS Knows]-> (b IS Person)
    <-[IS Knows]- (c IS Person)
  COLUMNS (a.name AS a, b.name AS b, c.name AS c))
----
-- Where each person lives, and whom they reach in two steps through three
-- different people: the second path compares its own copy of the shared 'a'.
-- duckdb: VALUES ('Alice', 'Paris', 'Bob', 'Carol'), ('Bob', 'Rome', 'Carol', 'Dave')
WITH ${graph}
SELECT * FROM GRAPH_TABLE (g
  MATCH ANY (a IS Person) -[IS LivesIn]-> (x IS City),
    ANY ACYCLIC (a) -[IS Knows]-> (b IS Person) -[IS Knows]-> (c IS Person)
  COLUMNS (a.name AS a, x.name AS x, b.name AS b, c.name AS c))
