/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <array>
#include <set>
#include <span>
#include <variant>

#include <folly/CppAttributes.h>

#include "axiom/optimizer/v2/Translator.h"

namespace facebook::axiom::optimizer::v2 {

/// Lowers one graph match into a plan of joins over the element tables.
///
/// Runs in stages, each reading what the earlier ones settled:
///
/// 1. Bind: binds each pattern name to an element, and records the paths, the
///    edge occurrences between their vertices, and the repeated patterns. A
///    vertex pattern right after another binds the same element.
/// 2. Narrow: drops the tables an element cannot match given its neighbors.
///    An element is a union of the remaining legs; an undirected edge has a
///    leg per orientation.
/// 3. Copy: an element that several paths with a selector bind gets a copy
///    for each of them but the first, since each such path is limited on its
///    own before paths join. A copy matches its original on identity.
/// 4. Fold: a vertex at a single edge end joins inside the legs of that edge,
///    and reads its identity from that end; so does a vertex between two
///    edges inside an edge whose rows are its rows. A vertex table that reads
///    the edge table's own rows is projected over the edge's scan. Other
///    vertices keep a relation of their own.
/// 5. Build: builds each element's relation.
/// 6. Connect: connects each edge to the vertices next to it, an unfolded
///    vertex's relation or the edge another vertex folds into, and each copy
///    to its original, on their identities.
/// 7. Join: builds each repeated pattern's hops. Joins each path with a
///    selector on its own, filters it with its term predicates, and limits it
///    to the selector's matches per pair of endpoints. Then joins the rest of
///    the connected elements and filters them with the other paths' term
///    predicates. Unconnected groups join with a cross product, and the match
///    predicate filters the result.
///
///   Stage    Reads             Produces
///   -------  ----------------  ----------------------------
///   Bind     the logical plan  Element::name
///                              Element::labelTables
///                              Element::properties
///                              Element::paths
///                              EdgeState
///                              'elementByName_'
///                              'occurrences_'
///                              'repetitions_'
///                              'paths_'
///                              'pathLengths_'
///
///   Narrow   'occurrences_'    Element::legs
///            'repetitions_'
///            Element::labelTables
///            EdgeState
///
///   Copy     Element::paths    'copies_'
///            'paths_'          'elements_', with the copies
///            'occurrences_'    'occurrences_', repointed
///                              'paths_', repointed
///                              Element::needsIdentity
///
///   Fold     'paths_'          VertexState
///            'occurrences_'    Element::needsIdentity
///                              folded EdgeEnds
///
///   Build    Element::legs     Element::node
///            VertexState       Element::scope
///            EdgeState         Element::row
///            folded EdgeEnds
///
///   Connect  'occurrences_'    'connections_'
///            'copies_'
///            Element::scope
///
///   Join     'connections_'    the plan
///            'repetitions_'    Repetition::hop
///            Element::node     Element::scope, substituted
///            'paths_'          Element::row, substituted
///            Element::row      'connections_', substituted
///            'copies_'
class Translator::GraphMatchLowering {
 public:
  GraphMatchLowering(
      Translator& translator,
      const logical_plan::GraphMatchNode& match)
      : translator_{translator}, match_{match}, graph_{*match.graph()} {}

  /// Returns the lowered match with its outputs in 'required', or nullopt when
  /// the match is empty because an element has no table left after narrowing.
  std::optional<Translated> lower(const LpNameSet& required);

 private:
  using ElementTable = logical_plan::PropertyGraph::ElementTable;
  using EdgeTable = logical_plan::PropertyGraph::EdgeTable;

  // A table an element can match. For an edge, 'reversed' reads its
  // destination as the end the pattern leaves from.
  struct Leg {
    const ElementTable* table{nullptr};
    bool reversed{false};

    // The table ID and the key columns of an identity of the leg's rows. With
    // 'end' unset, the leg's own table and key. For an edge leg with 'end'
    // set, the vertex table at its source (true) or destination (false) end,
    // and the edge columns that hold that vertex's key.
    int32_t tableId(std::optional<bool> end) const;
    const std::vector<std::string>& keyColumns(std::optional<bool> end) const;
  };

  // State only a vertex has.
  struct VertexState {
    // Number of edge ends it sits at.
    int32_t numEdgeEnds{0};

    // True for a vertex that joins inside the legs of an edge: 'foldedEdge',
    // at its source end if 'foldedAtSource'.
    bool folded{false};
    size_t foldedEdge{0};
    bool foldedAtSource{false};
  };

  // State only an edge has.
  struct EdgeState {
    // True if an undirected pattern binds it.
    bool undirected{false};

    // True if a repeated pattern binds it. Its relation is the hop of the
    // walks, which no connection joins.
    bool repeated{false};
  };

  // A vertex or edge that patterns bind.
  struct Element {
    // Pattern name, empty for an unnamed pattern.
    std::string name;

    // Tables its label expressions match.
    std::vector<const ElementTable*> labelTables;

    // Properties of 'labelTables', which make the ROW its pattern name stands
    // for in predicates.
    velox::RowTypePtr properties;

    // Tables it can match: 'labelTables' less those that cannot connect to
    // the elements next to it. Each is one leg of its relation; an undirected
    // edge has a leg per orientation that connects.
    std::vector<Leg> legs;

    // Indices of the paths that bind the element. A path with a selector that
    // binds a copy of it is the copy's.
    std::set<size_t> paths;

    // The element's rows. Null for a vertex that joins inside an edge's legs.
    NodeCP node{nullptr};

    // Expressions over 'node' by name: the element's properties, and the
    // '$'-prefixed columns that joins and selectors read. For a vertex that
    // joins inside an edge's legs, its properties as expressions over the
    // edge's rows.
    Scope scope;

    // The ROW of its properties that its pattern name stands for in
    // predicates. Null for an unnamed element or one without properties.
    ExprCP row{nullptr};

    // True if it is inside a repeated pattern: its rows make the hops of a
    // walk, which the walk joins.
    bool inRepetition{false};

    // True if it is inside a repetition that walks zero hops: it has no rows,
    // and its properties are NULL.
    bool inZeroHops{false};

    // True if its identity is read: a selector partitions on it, or it
    // matches a copy of it. A vertex that joins inside an edge's legs reads
    // its identity from that edge's end.
    bool needsIdentity{false};

    // Vertex or edge, with the state of that kind.
    std::variant<VertexState, EdgeState> state;

    bool isVertex() const {
      return std::holds_alternative<VertexState>(state);
    }

    bool isEdge() const {
      return std::holds_alternative<EdgeState>(state);
    }

    // True for a vertex that joins inside the legs of an edge (see fold()).
    // Any other vertex has a relation of its own that the edges join on its
    // identity.
    bool folds() const {
      return isVertex() && std::get<VertexState>(state).folded;
    }

    // Adds the IDs of the tables of its legs to 'ids'.
    void collectTableIds(folly::F14FastSet<int32_t>& ids) const;

    // Drops the legs whose table is not in 'tableIds'. Returns true if it
    // dropped any.
    bool keepLegsOf(const folly::F14FastSet<int32_t>& tableIds);

    // True if the identity 'end' selects (see Leg::tableId) can come from more
    // than one table, so it includes a table ID.
    bool hasSeveralTables(std::optional<bool> end) const;

    // Returns the columns that hold the identity 'end' selects: the table ID
    // when it has several tables, then the key.
    ExprVector identity(std::optional<bool> end) const;
  };

  // A vertex at one end of an edge occurrence.
  struct EdgeEnd {
    size_t vertex{0};
    // True for the source end of the edge's relation.
    bool source{false};
  };

  // An edge pattern and the vertex patterns before and after it.
  struct EdgeOccurrence {
    size_t edge{0};
    size_t from{0};
    size_t to{0};
    // True if the edge leaves 'from' at the source end of its relation.
    bool pointsRight{false};
    // Index of the path the edge pattern is in.
    size_t path{0};

    // Returns 'from' and 'to' with the end of the edge's relation each sits
    // at.
    std::array<EdgeEnd, 2> ends() const {
      return {{{from, pointsRight}, {to, !pointsRight}}};
    }
  };

  // A repeated edge pattern or subpath: walks of between 'minHops' and
  // 'maxHops' hops from vertex 'from' to vertex 'to'.
  struct Repetition {
    size_t from{0};
    size_t to{0};
    // For a repeated edge pattern, the edge whose relation holds the hops, and
    // true if each hop leaves at the source end of that relation.
    std::optional<size_t> edge;
    bool pointsRight{false};
    // For a repeated subpath, the vertices a hop leaves from and reaches, and
    // the elements inside it.
    size_t first{0};
    size_t last{0};
    std::vector<size_t> elements;
    int32_t minHops{0};
    int32_t maxHops{0};
    // Index of the path the pattern is in.
    size_t path{0};
    // The conditions each hop satisfies: the edge pattern's own, or those of
    // the subpath and the patterns inside it.
    std::vector<const logical_plan::Expr*> predicates;
    // The hops: rows of 'edge', or of 'elements' joined.
    NodeCP hop{nullptr};
    // True for an optional subpath, '( ... )?': its elements are read outside
    // it, and are NULL in a walk of zero hops.
    bool optional{false};
    // Number of edges in one hop: 1 for an edge pattern, the edges inside for
    // a subpath.
    int64_t edgesPerHop{1};
    // Names of the paths and subpaths the repetition is in, whose lengths
    // count its hops.
    std::vector<std::string> lengthNames;

    // Returns the elements one hop binds: the edge, or the elements inside.
    std::vector<size_t> hopElements() const {
      std::vector<size_t> result = elements;
      if (edge.has_value()) {
        result.push_back(*edge);
      }
      return result;
    }
  };

  // Where binding a sequence of path terms stands.
  struct TermCursor {
    // Index of the path the terms are in.
    size_t path{0};
    // Where the terms' predicates go.
    std::vector<const logical_plan::Expr*>* predicates{nullptr};
    // The first vertex bound, and the vertex the next edge leaves from.
    std::optional<size_t> first;
    std::optional<size_t> vertex;
    // The edge or repetition waiting for the vertex it reaches.
    std::optional<EdgeOccurrence> pendingEdge;
    std::optional<Repetition> pendingRepetition;
    // Number of edges bound, outside repetitions.
    int64_t numEdges{0};
    // Names of the paths and subpaths the terms are in.
    std::vector<std::string> lengthNames;

    // True if the last term bound is a vertex pattern: the next edge or
    // repetition leaves from it.
    bool atVertex() const {
      return vertex.has_value() && !pendingEdge.has_value() &&
          !pendingRepetition.has_value();
    }
  };

  // A path pattern: its selector's limit on matches per pair of endpoints,
  // its endpoint vertices, its term predicates, and its mode with the
  // vertices and edges it binds, in path order.
  struct Path {
    std::optional<int32_t> limit;
    size_t first{0};
    size_t last{0};
    std::vector<const logical_plan::Expr*> predicates;
    logical_plan::GraphPathMode mode{logical_plan::GraphPathMode::kWalk};
    std::vector<size_t> vertices;
    std::vector<size_t> edges;

    // Returns the elements the mode compares: the edges of a trail, or the
    // vertices of an acyclic or simple path. See modePredicates for which
    // pairs of them must differ.
    std::span<const size_t> comparedElements() const;
  };

  // The length of a named path or subpath: its edges outside repetitions,
  // and the repetitions in it, whose hops add to it.
  struct PathLength {
    int64_t edges{0};
    std::vector<size_t> repetitions;
  };

  // A vertex table joined inside an edge leg, and its columns by name.
  struct FoldedVertex {
    const ElementTable* table{nullptr};
    Scope scope;
  };

  // Elements 'leftElement' and 'rightElement' match where 'leftKeys' equal
  // 'rightKeys'.
  struct Connection {
    size_t leftElement;
    ExprVector leftKeys;
    size_t rightElement;
    ExprVector rightKeys;
  };

  // Returns the state of the vertex or edge at 'index' in 'elements_'.
  VertexState& vertexAt(size_t index) {
    return std::get<VertexState>(elements_[index].state);
  }

  EdgeState& edgeAt(size_t index) {
    return std::get<EdgeState>(elements_[index].state);
  }

  // Returns the element 'pattern' binds.
  size_t bind(const logical_plan::GraphElementPattern& pattern);

  // Binds a vertex pattern right after the vertex at 'index' to that vertex.
  void bindAdjacent(
      const logical_plan::GraphVertexPattern& pattern,
      size_t index);

  // Binds the patterns of 'path', the path at 'pathIndex', and records its
  // edge occurrences.
  void addPath(const logical_plan::GraphPathPattern& path, size_t pathIndex);

  // Binds 'terms' from where 'cursor' stands, and advances it. A vertex
  // pattern right after a vertex pattern matches the same vertex.
  void addTerms(
      const std::vector<logical_plan::GraphPatternTermPtr>& terms,
      TermCursor& cursor);

  // Binds a vertex pattern where 'cursor' stands: after an edge or a
  // repetition, which it ends, or right after a vertex, which it binds to.
  void addVertex(
      const logical_plan::GraphVertexPattern& pattern,
      TermCursor& cursor);

  // Binds an edge pattern after the vertex 'cursor' stands at, as the pending
  // edge, or as the pending repetition when 'repetition' is set.
  void addEdge(
      const logical_plan::GraphEdgePattern& edge,
      const logical_plan::GraphRepetition* repetition,
      TermCursor& cursor);

  // Binds a subpath: its patterns join the path where 'cursor' stands, or,
  // when 'repetition' is set, they make the hops of the pending repetition.
  void addSubpath(
      const logical_plan::GraphSubpathPattern& subpath,
      const logical_plan::GraphRepetition* repetition,
      TermCursor& cursor);

  // Binds a repeated or optional subpath after the vertex 'cursor' stands at,
  // as the pending repetition.
  void addRepeatedSubpath(
      const logical_plan::GraphSubpathPattern& subpath,
      const logical_plan::GraphRepetition& repetition,
      TermCursor& cursor);

  // Fails if a predicate reads a name inside a repetition outside it: there it
  // stands for one element per hop.
  void checkGroupReads() const;

  // Records the properties the outputs in 'required' read, by pattern name.
  void recordPropertyReads(const LpNameSet& required);

  // Adds each repetition to the lengths of the named paths and subpaths it is
  // in.
  void recordPathLengths();

  // Drops the legs that cannot connect to the elements next to them, until
  // no more can be dropped. After it, each vertex table an edge leg
  // references at an end is a leg of the vertex there. Returns false if an
  // element has no legs left.
  bool narrow();

  // Drops the legs of the edge and of the vertices at 'occurrence' that
  // cannot connect to each other. Returns true if it dropped any.
  bool narrowAtEdge(const EdgeOccurrence& occurrence);

  // Drops the legs of the vertices at the ends of 'repetition' that a walk
  // cannot leave from or reach. Returns true if it dropped any.
  bool narrowAtRepetition(const Repetition& repetition);

  // Gives each path with a selector its own copy of each element that an
  // earlier path with a selector binds.
  void copySharedElements();

  // Sets each vertex's VertexState. A vertex at a single edge end joins
  // inside that edge's legs. A vertex at the ends of two edges joins inside
  // one whose rows are its rows (see isEdgeRow), and the other edge joins it
  // there on its identity. Returns, for each edge, the ends whose vertices
  // join inside its legs.
  std::vector<std::vector<EdgeEnd>> fold();

  // True if each row of edge 'edge' is the row of the vertex at 'end' that it
  // references: each matches one table, and the two scan one table whose key
  // the edge's end columns are.
  bool isEdgeRow(size_t edge, const EdgeEnd& end) const;

  // Translates the plan of 'table'.
  Translated translateTable(const ElementTable& table);

  // Builds the relation of a vertex that does not join inside an edge.
  void buildVertex(Element& vertex);

  // Builds the relation of the edge at 'edgeIndex', joining each leg with the
  // vertices at 'foldedEnds'.
  void buildEdge(size_t edgeIndex, const std::vector<EdgeEnd>& foldedEnds);

  // Drops the self-loops of 'table' from 'translated', a reversed leg's rows.
  Translated dropSelfLoops(Translated translated, const EdgeTable& table);

  // The rows of one leg of an edge: the edge table's rows, and the vertex
  // table at each folded end.
  struct LegRows {
    NodeCP node{nullptr};

    // The edge table's columns by name.
    Scope edgeScope;

    // The vertex table at each folded end, and its columns by name.
    std::vector<FoldedVertex> foldedVertices;
  };

  // Returns the rows of 'leg' with the vertex tables at 'foldedEnds'. A vertex
  // table whose row is the edge's row is read from the same scan; any other
  // joins on its key.
  LegRows translateLeg(const Leg& leg, const std::vector<EdgeEnd>& foldedEnds);

  // Translates 'tables', whose plans are Projects over scans of one table,
  // over one scan that reads every column they read. Returns each table's
  // translation, in order.
  std::vector<Translated> translateOverOneScan(
      const std::vector<const ElementTable*>& tables);

  // Joins 'vertexTable' to 'rows' where its key equals the edge's
  // 'endColumns'. Returns the vertex table's columns by name, and applies the
  // join's substitutions to 'rows'.
  Scope joinVertexTable(
      LegRows& rows,
      const std::vector<std::string>& endColumns,
      const ElementTable& vertexTable);

  // Sets the properties of the elements inside a walk of zero hops: NULL for
  // an optional subpath, empty arrays otherwise.
  void bindZeroHopNames();

  // Binds each named element's pattern name to a ROW of its properties.
  void bindPatternNames();

  // Appends to 'exprs' each of 'properties' as 'table' computes it over
  // 'scope', or a typed NULL where 'table' does not define it.
  void appendProperties(
      const velox::RowType& properties,
      const ElementTable& table,
      const Scope& scope,
      ExprVector& exprs);

  // Sets the relation of 'element' to the UNION ALL of its legs, or to its
  // one leg. 'legExprs[i]' holds leg i's expressions for 'names'.
  void unionLegs(
      Element& element,
      std::vector<NodeCP> legNodes,
      const std::vector<ExprVector>& legExprs,
      const std::vector<std::string>& names,
      const std::vector<velox::TypePtr>& types);

  // Connects each edge occurrence to the vertices next to it.
  void connect();

  // Returns the element whose rows hold 'element': the edge a folded vertex
  // joins inside, or the element itself.
  size_t holderOf(size_t element) const;

  // Returns the groups of elements that connections join, each in the order
  // patterns bind them. A vertex joined inside an edge's legs is in none.
  std::vector<std::vector<size_t>> components() const;

  // Returns the columns that identify 'element'. A vertex that folds reads
  // them from its edge's end. With 'withTableId', the table ID comes first
  // even where it does not vary.
  ExprVector identityOf(size_t element, bool withTableId) const;

  // Returns the identity 'end' selects of 'element' (see
  // Element::identity), with the table ID first even where it does not vary
  // if 'withTableId'.
  ExprVector identity(
      const Element& element,
      std::optional<bool> end,
      bool withTableId) const;

  // Adds to 'sources' the IDs of the tables a hop of 'repetition' can leave
  // from, and to 'destinations' those it can reach.
  void collectHopTableIds(
      const Repetition& repetition,
      folly::F14FastSet<int32_t>& sources,
      folly::F14FastSet<int32_t>& destinations) const;

  // Builds each repetition's hops: the edge's relation, or the subpath's
  // elements joined, filtered with the repetition's predicates.
  void buildHops();

  // The identities a walk compares, which line up by position: of the vertex
  // it leaves from, of each hop's ends, and of the vertex it reaches.
  struct WalkKeys {
    ExprVector start;
    ExprVector end;
    ExprVector hopSource;
    ExprVector hopDestination;
  };

  // Returns the identities the walks of 'repetition' compare, without hop
  // identities for a walk of zero hops. Fails if their types differ.
  WalkKeys walkKeys(const Repetition& repetition) const;

  // Returns 'length' as an expression: the edges outside repetitions plus the
  // edges on each walk's hops.
  ExprCP pathLength(const PathLength& length);

  // Returns the walks of the repetition at 'index' between the rows of 'start',
  // which holds its 'from' vertex, and 'end', which holds its 'to' vertex. A
  // null 'end' means 'start' holds both. The walks also output their number of
  // hops and the properties of the elements inside, which the elements then
  // read. For an optional subpath, a property is its value on the one hop, NULL
  // for zero hops. Otherwise it is an array of its values on the hops. Walks of
  // zero hops are 'start' joined with 'end' on the vertex's identity.
  NodeCP walk(size_t index, NodeCP start, NodeCP end);

  // Returns the walks of zero hops between 'start' and 'end': 'start' joined
  // with 'end' on 'keys', or 'start' filtered when 'end' is null.
  NodeCP walkZeroHops(WalkKeys keys, NodeCP start, NodeCP end);

  // Adds to the walks' outputs each column of the optional subpath's hop: its
  // value on the one hop, NULL for zero hops. The elements then read these.
  void addHopValues(
      const Repetition& repetition,
      ColumnVector& sourceColumns,
      ColumnVector& outputColumns);

  // Adds to the walks' outputs an array of each property the outputs read of
  // an element inside the repetition: its values on the hops. The element then
  // reads these.
  void addHopLists(
      const Repetition& repetition,
      ColumnVector& sourceColumns,
      ColumnVector& outputColumns);

  // Returns 'node', the rows of the groups 'joined' marks, joined with the
  // walks of a repetition that reaches a group not joined yet, and that group.
  // Returns nullopt when no repetition does. 'groupOf' maps an element to its
  // group, and 'nodes' holds each group's rows.
  std::optional<std::pair<NodeCP, size_t>> walkToUnjoined(
      NodeCP node,
      const std::vector<std::optional<size_t>>& groupOf,
      const std::vector<bool>& joined,
      const std::vector<NodeCP>& nodes);

  // Returns 'node' joined with the walks whose ends are both in the groups
  // 'joined' marks. 'groupOf' maps an element to its group.
  NodeCP walkWithin(
      NodeCP node,
      const std::vector<std::optional<size_t>>& groupOf,
      const std::vector<bool>& joined);

  // Returns the paths that bind the elements of 'component', or the vertices
  // that join inside its edges.
  std::set<size_t> componentPaths(const std::vector<size_t>& component) const;

  // Returns 'node' with at most the selector's limit of matches of 'path'
  // per pair of endpoints.
  NodeCP limitMatches(NodeCP node, const Path& path);

  // Returns the matches of the paths in 'component': each path with a
  // selector joined, filtered and limited on its own, then joined with the
  // rest of the elements and filtered with the other paths' predicates.
  NodeCP lowerComponent(const std::vector<size_t>& component);

  // Joins 'groups', each a set of elements whose rows 'nodes' holds, on the
  // connections between groups. Joins next a group connected to those joined
  // before it, so paths that share names join without a cross product.
  NodeCP join(
      const std::vector<std::vector<size_t>>& groups,
      std::vector<NodeCP> nodes);

  // Returns 'node' filtered by 'conjuncts'.
  NodeCP filter(NodeCP node, ExprVector conjuncts);

  // Applies node simplification facts to every expression the lowering
  // holds: element columns and rows, and connection keys.
  void substitute(const PlanSubstitutions& substitutions);

  // Returns the conditions the mode of 'path' puts on its elements: each two
  // compared elements differ, except a simple path's first and last vertices.
  ExprVector modePredicates(const Path& path) const;

  // Returns a condition that elements 'lhs' and 'rhs' differ, or nullptr when
  // their tables cannot hold one element. Elements that each match one table
  // match the same one and differ by key; others differ by table ID or key.
  ExprCP FOLLY_NULLABLE makeDistinct(size_t lhs, size_t rhs) const;

  // Translates 'predicates' against the pattern names. In the predicates of
  // 'path', a name stands for the copy of its element that path binds.
  ExprVector translatePredicates(
      const std::vector<const logical_plan::Expr*>& predicates,
      std::optional<size_t> path) const;

  // Returns the outputs in 'required' by output name.
  Scope outputs(const LpNameSet& required);

  Translator& translator_;
  const logical_plan::GraphMatchNode& match_;
  const logical_plan::PropertyGraph& graph_;

  // Bound elements, in the order patterns first name them.
  std::vector<Element> elements_;

  // Index in 'elements_' of the element each pattern name binds.
  folly::F14FastMap<std::string, size_t> elementByName_;

  // Edge patterns, in the order paths list them. A repeated edge pattern is
  // in 'repetitions_' instead.
  std::vector<EdgeOccurrence> occurrences_;

  // Repeated edge patterns and subpaths, in the order paths list them.
  std::vector<Repetition> repetitions_;

  // True for each repetition whose walks are joined.
  std::vector<bool> walked_;

  // Pairs of adjacent elements and the columns they match on.
  std::vector<Connection> connections_;

  // An element, its copy, and the path with a selector the copy is for.
  struct ElementCopy {
    size_t original{0};
    size_t copy{0};
    size_t path{0};
  };

  // Copies of elements that several paths with a selector bind.
  std::vector<ElementCopy> copies_;

  // Length of each named path and subpath.
  folly::F14FastMap<std::string, PathLength> pathLengths_;

  // Number of hops of each walk, by repetition index, once it is walked.
  std::vector<ExprCP> hopCounts_;

  // The properties the outputs read, by pattern name.
  folly::F14FastMap<std::string, folly::F14FastSet<std::string>> propertyReads_;

  // Path patterns, in the order the node lists them.
  std::vector<Path> paths_;
};

} // namespace facebook::axiom::optimizer::v2
