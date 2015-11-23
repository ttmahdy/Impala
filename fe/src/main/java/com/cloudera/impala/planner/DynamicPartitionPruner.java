// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.planner.PlanNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.base.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a filter propagation graph. A filter propagation graph is constructed
 * from a query plan and stores information about runtime filters that can be applied to
 * a specific query. Runtime filters are populated using runtime data (i.e., tuples
 * produced by an operator) and are propagated to destination operators (scan nodes)
 * which use these filters to prune tuples or partitions (for the case of DPP).
 *
 * Every edge (u, v) in the graph represents a 'can-filter' relationship between two
 * operators, where 'v' is the operator materializing the rows that are used to populate
 * a filter at runtime, and 'u' is the operator applying the filter. The runtime filter
 * is constructed at the lowest join operator that "materializes" the tuples of both 'u'
 * and 'v'. An edge may contain multiple runtime filters. If an edge does not
 * contain any runtime filters, it is not included in the filter propagation graph.
 *
 * The process of computing the runtime filters for a query works as follows:
 * 1. Compute the filter propagation graph by traversing the query single-node plan
 *    in a top-down fashion to identify candidate runtime filters. Candidate filters
 *    are generated from equi-join predicates.
 * 2. Compute the final list of runtime filters by pruning filters from the
 *    filter propagation graph that do not satisfy a set of criteria specified by the
 *    users of this class. For instance, if the filter propagation graph is used for
 *    dynamic partition pruning, only the runtime filters that are applied at partitioned
 *    tables may be returned.
 */
final class FilterPropagationGraph {
  private final static Logger LOG =
      LoggerFactory.getLogger(FilterPropagationGraph.class);
  /**
   * Internal representation of a runtime filter.
   */
  public static class RuntimeFilter {
    // Identifier of the filter (unique within a query)
    private final DynamicFilterId filterId_;
    // Join operator that constructs the filter
    private final JoinNode filterConstructingOp_;
    // Scan operator that applies the filter
    private final ScanNode filterApplyingOp_;
    // Operator that materializes the rows that are used to populate the filter
    private final PlanNode materializingInputRowsOp_;
    // Expr on which the filter is built
    private final Expr buildFilterExpr_;
    // SlotRef on which the filter is applied
    private final SlotRef applyFilterSlot_;

    public RuntimeFilter(DynamicFilterId id, JoinNode filterConstructingOp,
        ScanNode filterApplyingOp, PlanNode materializingInputRowsOp,
        Expr buildFilterExpr, SlotRef applyFilterSlot) {
      Preconditions.checkNotNull(filterConstructingOp);
      Preconditions.checkNotNull(filterApplyingOp);
      Preconditions.checkNotNull(buildFilterExpr);
      Preconditions.checkNotNull(applyFilterSlot);
      Preconditions.checkState(filterApplyingOp.getTupleIds().contains(
          applyFilterSlot.getDesc().getParent().getId()));
      filterId_ = id;
      filterConstructingOp_ = filterConstructingOp;
      filterApplyingOp_ = filterApplyingOp;
      materializingInputRowsOp_ = materializingInputRowsOp;
      buildFilterExpr_ = buildFilterExpr;
      applyFilterSlot_ = applyFilterSlot;
    }

    public DynamicFilterId getFilterId() { return filterId_; }
    public ScanNode getFilterApplyingOp() { return filterApplyingOp_; }
    public PlanNode getMaterializingInputRowsOp() { return materializingInputRowsOp_; }
    public Expr getBuildFilterExpr() { return buildFilterExpr_; }
    public SlotRef getApplyFilterSlot() { return applyFilterSlot_; }

    /**
     * Registers the runtime filter to the appropriate operators.
     */
    public void registerRuntimeFilter() {
      filterConstructingOp_.addFilter(filterId_, buildFilterExpr_);
      filterApplyingOp_.addFilter(filterId_, applyFilterSlot_);
    }

    @Override
    public String toString() {
      StringBuilder output = new StringBuilder();
      output.append("FilterID: " + filterId_ + " ");
      output.append("Operator constructing the filter: " +
          filterConstructingOp_.getId() + " ");
      output.append("Operator applying the filter: " + filterApplyingOp_.getId() + " ");
      output.append("BuildFilterExpr: " + getBuildFilterExpr().debugString() +  " ");
      output.append("ApplyFilterSlot: " + getApplyFilterSlot().debugString());
      return output.toString();
    }
  }

  // Internal representation of the filter propagation graph
  private final Map<PlanNodeId, Map<PlanNodeId, List<RuntimeFilter>>> edges_ =
      Maps.newHashMap();

  // Maximum filter propagation path allowed. A path of length N in the filter
  // propagation graph creates a dependency chain of size N. Because long dependency
  // chains may increase the response time of a query, when we compute the final runtime
  // filters, we prune the paths from the graph that originate from specific destination
  // nodes (operators applying runtime filters) and are longer than
  // MAX_FILTER_PROPAGATION_PATH_LEN.
  private final static int MAX_FILTER_PROPAGATION_PATH_LEN = 2;

  // Scan operators that apply at least one candidate runtime filter
  private final Map<PlanNodeId, ScanNode> filterApplyingOperators_ = Maps.newHashMap();

  // Generator for filter ids
  private final IdGenerator<DynamicFilterId> filterIdGenerator =
      DynamicFilterId.createGenerator();

  private FilterPropagationGraph() {};

  /**
   * Static function to build a filter propagation graph from a query's single node plan.
   */
  public static FilterPropagationGraph buildFilterPropagationGraph(
      Analyzer analyzer, PlanNode plan) {
    FilterPropagationGraph graph = new FilterPropagationGraph();
    graph.buildGraph(analyzer, plan);
    return graph;
  }

  /**
   * Builds a filter propagation graph by recursively traversing the single node
   * plan in a top-down fashion. The graph is populated with candidate runtime filters
   * that are generated from join predicates.
   */
  private void buildGraph(Analyzer analyzer, PlanNode root) {
    if (root instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) root;
      List<BinaryPredicate> joinPredicates = Lists.newArrayList();
      if (!joinNode.getJoinOp().isLeftOuterJoin()
          && !joinNode.getJoinOp().isFullOuterJoin()
          && !joinNode.getJoinOp().isAntiJoin()) {
        // It's not correct to push runtime filters to the left side of a left outer,
        // full outer or anti join if the filter corresponds to an equi-join predicate
        // from the ON clause.
        joinPredicates.addAll(joinNode.getEqJoinConjuncts());
      }

      // Consider all equality binary predicates assigned at this join node as
      // candidates for generating runtime filters.
      for (Expr otherConjunct: joinNode.getConjuncts()) {
        if (otherConjunct instanceof BinaryPredicate
            && ((BinaryPredicate) otherConjunct).getOp() == BinaryPredicate.Operator.EQ) {
          joinPredicates.add((BinaryPredicate) otherConjunct);
        }
      }

      // Try to construct a runtime filter from every equality binary predicate
      for (BinaryPredicate joinPredicate: joinPredicates) {
        RuntimeFilter filter = constructCandidateFilter(filterIdGenerator.getNextId(),
            analyzer, joinPredicate, joinNode);
        if (filter == null) continue;
        LOG.trace("DPP candidate filter: " + filter);
        ScanNode dstOperator = filter.getFilterApplyingOp();
        filterApplyingOperators_.put(dstOperator.getId(), dstOperator);
        // Add the candidate runtime filter to the filter propagation graph
        addOrUpdateEdge(filter);
      }
    }
    for (PlanNode childNode: root.getChildren()) buildGraph(analyzer, childNode);
  }

  /**
   * Constructs a candidate runtime filer from the equi-join predicate 'joinPredicate'
   * of join node 'filterConstructingOp'. Returns the runtime filter or null
   * if it is not possible to construct a runtime filter from the specified predicate.
   */
  private static RuntimeFilter constructCandidateFilter(DynamicFilterId id,
      Analyzer analyzer, Expr joinPredicate, JoinNode filterConstructingOp) {
    // Find the expr which is used to populate the runtime filter and the operator
    // in the plan tree that materializes the tuple ids of that expr.
    Expr buildFilterExpr = joinPredicate.getChild(1);
    List<TupleId> tids = Lists.newArrayList();
    buildFilterExpr.getIds(tids, null);
    if (tids.isEmpty()) return null;
    PlanNode operatorMaterializingBuildExprTids =
        PlanNode.findOperatorMaterializingTids(filterConstructingOp.getChild(1), tids);
    if (operatorMaterializingBuildExprTids == null) return null;

    // Find the operator on which the runtime filter will be applied and the
    // associated slot
    SlotRef applyFilterSlot = joinPredicate.getChild(0).unwrapSlotRef(false);
    if (applyFilterSlot == null) return null;
    TupleId applyFilterTupleId = null;
    TupleDescriptor desc = applyFilterSlot.getDesc().getParent();
    if (desc.getTable() != null) {
      applyFilterTupleId = desc.getId();
    } else {
      // The slot on the lhs of the join predicate is not materialized by a scan
      // operator. Since runtime filters are only applied at scan operator, we check if
      // there is an equivalent slot that is materialized by a scan.
      for (SlotId slotId: analyzer.getAllEquivSlots(applyFilterSlot.getDesc().getId())) {
        TupleDescriptor tupleDesc = analyzer.getSlotDesc(slotId).getParent();
        if (tupleDesc.getTable() == null || tids.contains(tupleDesc.getId())) continue;
        applyFilterTupleId = tupleDesc.getId();
        applyFilterSlot = new SlotRef(analyzer.getSlotDesc(slotId));
        break;
      }
    }
    if (applyFilterTupleId == null) return null;
    // Find the scan operator that materializes 'applyFilterTupleId'.
    ScanNode filterApplyingOp =
        findFilterApplyingOp(filterConstructingOp.getChild(0), applyFilterTupleId);
    if (filterApplyingOp == null) return null;

    return new RuntimeFilter(id, filterConstructingOp, filterApplyingOp,
        operatorMaterializingBuildExprTids, buildFilterExpr, applyFilterSlot);
  }

  /**
   * Identifies the scan operator in the plan tree rooted at 'root' that materializes
   * 'tid'. Returns null if no such operator exists or if the scan operator is below a
   * tuple materializing operator (e.g. sort, analytic fn) that is not an aggregation.
   */
  private static ScanNode findFilterApplyingOp(PlanNode root, TupleId tid) {
    if (root instanceof ScanNode && root.getTupleIds().contains(tid)) {
      return (ScanNode) root;
    }
    if (!(root instanceof ScanNode || root instanceof JoinNode
          || root instanceof AggregationNode)) {
      return null;
    }
    for (PlanNode childNode: root.getChildren()) {
      PlanNode node = findFilterApplyingOp(childNode, tid);
      if (node != null) return (ScanNode) node;
    }
    return null;
  }

  /**
   * Constructs or updates an edge in the filter propagation graph using a candidate
   * runtime filter. The edge nodes (u, v) are the ids of the operators applying and
   * materializing the input rows for constructing the filter, respectively. Every edge
   * is annotated with the runtime filters that are associated with a specific pair of
   * operators (edge nodes). If the edge already exists, 'filter' is added to the list of
   * filters of that edge.
   */
  private void addOrUpdateEdge(RuntimeFilter filter) {
    PlanNodeId u = filter.getFilterApplyingOp().getId();
    PlanNodeId v = filter.getMaterializingInputRowsOp().getId();
    Map<PlanNodeId, List<RuntimeFilter>> edges = edges_.get(u);
    if (edges == null) {
      // No edges originate from 'u'
      edges = Maps.newHashMap();
      edges_.put(u, edges);
    }
    List<RuntimeFilter> edgeFilters = edges.get(v);
    if (edgeFilters == null) {
      // No filters in this edge
      edgeFilters = Lists.newArrayList();
      edges.put(v, edgeFilters);
    }
    edgeFilters.add(filter);
  }

  /**
   * Returns the list of runtime filters to apply to a query based on two speficied
   * predicates:
   * a) 'isFinalFilterDestination' specifies which operators are the final destinations
   *    for runtime filters when filters are propagated among operators.
   * b) 'isEligibleFilter' specifies which runtime filters to apply.
   */
  public List<RuntimeFilter> getRuntimeFilters(
      Predicate<ScanNode> isFinalFilterDestination,
      Predicate<RuntimeFilter> isEligibleFilter) {
    List<RuntimeFilter> filters = Lists.newArrayList();
    Set<DynamicFilterId> visitedFilterIds = Sets.newHashSet();
    // For every operator that is a final filter destination, collect the runtime
    // filters that are reachable from that operator in the filter propagation graph
    // and satisfy the 'isEligibleFilter' predicate.
    for (ScanNode operator: filterApplyingOperators_.values()) {
      if (!isFinalFilterDestination.apply(operator)) continue;
      filters.addAll(getReacheableFilters(operator, isEligibleFilter,
          visitedFilterIds));
    }
    return filters;
  }

  /**
   * Finds all the filters in the filter propagation graph that:
   * a) are within MAX_FILTER_PROPAGATION_PATH_LEN distance from 'operator'
   * b) satisfy the specified 'filterPredicate'
   */
  private List<RuntimeFilter> getReacheableFilters(ScanNode operator,
      Predicate<RuntimeFilter> filterPredicate, Set<DynamicFilterId> visitedFilterIds) {
    Preconditions.checkState(operator instanceof ScanNode);
    List<RuntimeFilter> filters = Lists.newArrayList();
    collectFilters(operator.getId(), MAX_FILTER_PROPAGATION_PATH_LEN, filters,
        filterPredicate, visitedFilterIds);
    return filters;
  }

  /**
   * Recursive function that collects all the filters that are reacheable from a
   * node 'u' in the filter propagation graph and are within maximum distance
   * 'distance' from 'u'. Filters are collected in 'filters'. 'visitedFilterIds' keeps
   * track of all visited filters to avoid returning duplicates when runtime filters are
   * collected from different destination nodes.
   */
  private void collectFilters(PlanNodeId u, int distance, List<RuntimeFilter> filters,
      Predicate<RuntimeFilter> filterPredicate, Set<DynamicFilterId> visitedFilterIds) {
    if (distance == 0) return;
    Map<PlanNodeId, List<RuntimeFilter>> edges = edges_.get(u);
    if (edges == null) return;
    for (Map.Entry<PlanNodeId, List<RuntimeFilter>> entry: edges.entrySet()) {
      Iterator<RuntimeFilter> filterIter = entry.getValue().iterator();
      while (filterIter.hasNext()) {
        RuntimeFilter filter = filterIter.next();
        if (visitedFilterIds.contains(filter.getFilterId())) continue;
        if (!filterPredicate.apply(filter)) {
          filterIter.remove();
          continue;
        }
        filters.add(filter);
        visitedFilterIds.add(filter.getFilterId());
      }
      if (!entry.getValue().isEmpty()) {
        collectFilters(entry.getKey(), distance - 1, filters, filterPredicate,
            visitedFilterIds);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder();
    for (Map.Entry<PlanNodeId, Map<PlanNodeId, List<RuntimeFilter>>> entry:
         edges_.entrySet()) {
      for (Map.Entry<PlanNodeId, List<RuntimeFilter>> edge:
           entry.getValue().entrySet()) {
        output.append("[" + entry.getKey() + ", " + edge.getKey() + "]\n");
        for (RuntimeFilter filter: edge.getValue()) {
          output.append(filter);
        }
      }
    }
    return output.toString();
  }
}

/**
 * Performs dynamic partition pruning by constructing a filter propagation graph
 * and identifying the runtime filters that can prune the partitions of partitioned
 * tables. Currently, dynamic partition pruning is only applied on HdfsTables.
 * TODO: Enable dynamic parition pruning for Kudu tables.
 */
public class DynamicPartitionPruner {
  private final static Logger LOG =
      LoggerFactory.getLogger(DynamicPartitionPruner.class);

  // Final filter destination operators are scan nodes of partitioned tables
  private final static Predicate<ScanNode> scansPartitionedTable_ =
      new Predicate<ScanNode>() {
        @Override
        public boolean apply(ScanNode arg) {
          return arg instanceof HdfsScanNode &&
              ((HdfsScanNode) arg).isPartitionedTable();
        }
      };

  // Eligible runtime filters are all the filters applied on slots of non-partitioned
  // tables and filters applied on partition columns.
  private final static Predicate<FilterPropagationGraph.RuntimeFilter>
        useFilterInDpp_ =
      new Predicate<FilterPropagationGraph.RuntimeFilter>() {
        @Override
        public boolean apply(FilterPropagationGraph.RuntimeFilter arg) {
          SlotDescriptor slotDesc = arg.getApplyFilterSlot().getDesc();
          Table tbl = arg.getFilterApplyingOp().getTupleDesc().getTable();
          if (scansPartitionedTable_.apply(arg.getFilterApplyingOp())) {
            return slotDesc.getColumn().getPosition() < tbl.getNumClusteringCols();
          }
          return true;
        }
      };

  /**
   * Computes the runtime filters for dynamic partition pruning and registers
   * the filters to the associated operators.
   */
  public static void computeRuntimeFilters(Analyzer analyzer, PlanNode plan) {
    // Return immediately if the query doesn't access any partitioned tables.
    boolean hasPartitionedTables = false;
    for (TupleDescriptor desc: analyzer.getDescTbl().getTupleDescs()) {
      Table tbl = desc.getTable();
      if (tbl instanceof HdfsTable && tbl.getNumClusteringCols() > 0) {
        hasPartitionedTables = true;
        break;
      }
    }
    if (!hasPartitionedTables) return;

    FilterPropagationGraph graph =
        FilterPropagationGraph.buildFilterPropagationGraph(analyzer, plan);
    LOG.trace("DPP filter propagation graph: " + graph.toString());
    List<FilterPropagationGraph.RuntimeFilter> runtimeFilters =
        graph.getRuntimeFilters(scansPartitionedTable_, useFilterInDpp_);
    for (FilterPropagationGraph.RuntimeFilter filter: runtimeFilters) {
      filter.registerRuntimeFilter();
    }
  }
}
