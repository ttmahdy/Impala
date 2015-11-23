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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.planner.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for generating and assigning runtime filters to a query plan using
 * runtime filter propagation. Runtime filter propagation is an optimization technique
 * used to filter scanned tuples or scan ranges based on information collected at
 * runtime. A runtime filter is constructed during the build phase of a join node, and is
 * applied at a scan node on the probe side of that join node. Runtime filters are
 * generated from equi-join predicates but they do not replace the original predicates.
 *
 * Example: select * from T1, T2 where T1.a = T2.b and T2.c = '1';
 * Assuming that T1 is a fact table and T2 is a significantly smaller dimension table, a
 * runtime filter is constructed at the join node between tables T1 and T2 while building
 * the hash table on the values of T2.b (rhs of the join condition) from the tuples of T2
 * that satisfy predicate T2.c = '1'. The runtime filter is subsequently sent to the
 * scan node of table T1 and is applied on the values of T1.a (lhs of the join condition)
 * to prune tuples of T2 that cannot be part of the join result.
 */
public final class RuntimeFilterGenerator {
  private final static Logger LOG =
      LoggerFactory.getLogger(RuntimeFilterGenerator.class);

  // Map of scan tuple id to a list of runtime filters that can be applied at the
  // corresponding scan node.
  private final Map<TupleId, List<RuntimeFilter>> runtimeFiltersByTid_ =
      Maps.newHashMap();

  // Generator for filter ids
  private final IdGenerator<RuntimeFilterId> filterIdGenerator =
      RuntimeFilterId.createGenerator();

  private RuntimeFilterGenerator() {};

  /**
   * Internal representation of a runtime filter. A runtime filter is generated from
   * an equi-join predicate of the form <lhs_expr> = <rhs_expr>, where lhs_expr is the
   * expr on which the filter is applied and must be bound by a single tuple id from
   * the left plan subtree of the associated join node, while rhs_expr is the expr on
   * which the filter is built and can be bound by any number of tuple ids from the
   * right plan subtree. Every runtime filter must record the join node that constructs
   * the filter and the scan node that applies the filter (destination node).
   */
  public static class RuntimeFilter {
    // Identifier of the filter (unique within a query)
    private final RuntimeFilterId id_;
    // Join node that builds the filter
    private final JoinNode buildingNode_;
    // Scan node that applies the filter
    private ScanNode applyingNode_;
    // Expr (rhs of join predicate) on which the filter is built
    private final Expr buildExpr_;
    // Expr (lhs of join predicate) on which the filter is applied
    private Expr applyExpr_;
    // Slots from scan tuples that are in the same equivalent classes as the slots of
    // 'applyExpr_'. The slots are grouped by tuple id.
    private Map<TupleId, List<SlotId>> applyExprSlotsByTid_;

    private RuntimeFilter(RuntimeFilterId filterId, JoinNode filterBuildingNode,
        Expr buildExpr, Expr applyExpr, Map<TupleId, List<SlotId>> applyExprSlots) {
      id_ = filterId;
      buildingNode_ = filterBuildingNode;
      buildExpr_ = buildExpr;
      applyExpr_ = applyExpr;
      applyExprSlotsByTid_ = applyExprSlots;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof RuntimeFilter)) return false;
      return ((RuntimeFilter) obj).id_.equals(id_);
    }

    @Override
    public int hashCode() { return id_.hashCode(); }

    /**
     * Static function to create a RuntimeFilter from 'joinPredicate' that is assigned
     * to the join node 'filterBuildingNode'. Returns an instance of RuntimeFilter
     * or null if a runtime filter cannot be generated from the specified predicate.
     */
    public static RuntimeFilter create(IdGenerator<RuntimeFilterId> idGen,
        Analyzer analyzer, Expr joinPredicate, JoinNode filterBuildingNode) {
      Preconditions.checkNotNull(idGen);
      Preconditions.checkNotNull(joinPredicate);
      Preconditions.checkNotNull(filterBuildingNode);
      // Only consider binary equality predicates
      if (!Predicate.isEqBinaryPredicate(joinPredicate)) return null;

      if (joinPredicate.getChild(0).isConstant()
          || joinPredicate.getChild(1).isConstant()) {
        return null;
      }

      List<TupleId> rhsTupleIds = filterBuildingNode.getChild(1).getTupleIds();
      Expr buildExpr = null;
      if (joinPredicate.getChild(0).isBoundByTupleIds(rhsTupleIds)) {
        buildExpr = joinPredicate.getChild(0);
      } else if (joinPredicate.getChild(1).isBoundByTupleIds(rhsTupleIds)) {
        buildExpr = joinPredicate.getChild(1);
      } else {
        return null;
      }

      List<TupleId> lhsTupleIds = filterBuildingNode.getChild(0).getTupleIds();
      Expr applyExpr = null;
      if (joinPredicate.getChild(1).isBoundByTupleIds(lhsTupleIds)) {
        applyExpr = joinPredicate.getChild(1);
      } else if (joinPredicate.getChild(0).isBoundByTupleIds(lhsTupleIds)) {
        applyExpr = joinPredicate.getChild(0);
      } else {
        return null;
      }

      List<TupleId> tids = Lists.newArrayList();
      applyExpr.getIds(tids, null);
      if (tids.size() != 1) return null;

      Map<TupleId, List<SlotId>> applyExprScanSlots =
          getFilterExprScanSlotIds(analyzer, applyExpr);
      Preconditions.checkNotNull(applyExprScanSlots);
      if (applyExprScanSlots.isEmpty()) return null;

      LOG.trace("Generating runtime filter from predicate " + joinPredicate);
      return new RuntimeFilter(idGen.getNextId(), filterBuildingNode,
          buildExpr, applyExpr, applyExprScanSlots);
    }

    /**
     * Returns the ids of scan tuple slots on which a runtime filter expr can be
     * applied. Due to the existence of equivalence classes, a filter expr may be
     * applicable at multiple scan nodes. The returned slot ids are grouped by tuple id.
     * Returns an empty collection if the filter expr cannot be applied at a scan node.
     */
    private static Map<TupleId, List<SlotId>> getFilterExprScanSlotIds(Analyzer analyzer,
        Expr expr) {
      Map<TupleId, List<SlotId>> slotsByTid = Maps.newHashMap();
      SlotRef slotRef = expr.unwrapSlotRef(false);
      if (slotRef != null) return getScanBoundEquivSlots(analyzer, slotRef.getSlotId());

      // 'expr' is not a SlotRef and may contain multiple SlotRefs
      List<TupleId> tids = Lists.newArrayList();
      List<SlotId> sids = Lists.newArrayList();
      expr.getIds(tids, sids);
      Preconditions.checkState(tids.size() == 1);
      // We need to iterate over all the slots of 'expr' and check if they have
      // equivalent slots that are bound by the same scan tuple(s).
      for (SlotId slotId: sids) {
        Map<TupleId, List<SlotId>> currSlotsByTid =
            getScanBoundEquivSlots(analyzer, slotId);
        if (currSlotsByTid.isEmpty()) return Collections.emptyMap();
        if (slotsByTid.isEmpty()) {
          slotsByTid.putAll(currSlotsByTid);
          continue;
        }

        // Compute the intersection between tuple ids from 'slotsByTid' and
        // 'currSlotsByTid'. If the intersection is empty, an empty collection
        // is returned.
        Iterator<Map.Entry<TupleId, List<SlotId>>> iter =
            slotsByTid.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<TupleId, List<SlotId>> entry = iter.next();
          List<SlotId> slotIds = currSlotsByTid.get(entry.getKey());
          if (slotIds == null) {
            iter.remove();
          } else {
            entry.getValue().addAll(slotIds);
          }
        }
        if (slotsByTid.isEmpty()) return Collections.emptyMap();
      }
      return slotsByTid;
    }

    /**
     * Static function that returns the ids of slots bound by scan tuples for which
     * there is a value transfer from 'slotId'. The slots are grouped by tuple id.
     */
    private static Map<TupleId, List<SlotId>> getScanBoundEquivSlots(Analyzer analyzer,
        SlotId slotId) {
      Map<TupleId, List<SlotId>> slotsByTid = Maps.newHashMap();
      TupleDescriptor parentTupleDesc = analyzer.getSlotDesc(slotId).getParent();
      if (parentTupleDesc.getTable() != null) {
        slotsByTid.put(parentTupleDesc.getId(), Lists.newArrayList(slotId));
      }

      List<SlotId> equivSlotIds = Lists.newArrayList(analyzer.getEquivSlots(slotId));
      Iterator<SlotId> iter = equivSlotIds.iterator();
      while (iter.hasNext()) {
        SlotId equivSlotId = iter.next();
        if (equivSlotId.equals(slotId)) continue;
        TupleDescriptor tupleDesc = analyzer.getSlotDesc(equivSlotId).getParent();
        if (tupleDesc.getTable() == null
            || !analyzer.hasValueTransfer(slotId, equivSlotId)) {
          iter.remove();
          continue;
        }
        List<SlotId> sids = slotsByTid.get(tupleDesc.getId());
        if (sids == null) {
          sids = Lists.newArrayList();
          slotsByTid.put(tupleDesc.getId(), sids);
        }
        sids.add(equivSlotId);
      }
      return slotsByTid;
    }

    public RuntimeFilterId getFilterId() { return id_; }
    public ScanNode getApplyingNode() { return applyingNode_; }
    public boolean hasApplyingNode() { return applyingNode_ != null; }
    public JoinNode getFilterBuildingNode() { return buildingNode_; }
    public Expr getBuildFilterExpr() { return buildExpr_; }
    public Expr getApplyFilterExpr() { return applyExpr_; }
    public Map<TupleId, List<SlotId>> getApplyExprSlots() {
      return applyExprSlotsByTid_;
    }

    public void setFilterApplyingNode(ScanNode node) {
      applyingNode_ = node;
    }

    public void setApplyFilterExpr(Expr expr) {
      Preconditions.checkNotNull(expr);
      applyExpr_ = expr;
    }

    /**
     * Assigns this runtime filter to the corresponding plan nodes.
     */
    public void assignToPlanNodes() {
      Preconditions.checkNotNull(applyingNode_);
      buildingNode_.addRuntimeFilter(id_, buildExpr_);
      applyingNode_.addRuntimeFilter(id_, applyExpr_);
    }

    public String debugString() {
      StringBuilder output = new StringBuilder();
      output.append("FilterID: " + id_ + " ");
      output.append("Operator constructing the filter: " +
          buildingNode_.getId() + " ");
      output.append("Operator applying the filter: " + applyingNode_.getId() +
          " ");
      output.append("BuildFilterExpr: " + getBuildFilterExpr().debugString() +  " ");
      output.append("ApplyFilterExpr: " + getApplyFilterExpr().debugString());
      return output.toString();
    }
  }

  /**
   * Generates and assigns runtime filters to a query plan tree.
   */
  public static void generateRuntimeFilters(Analyzer analyzer, PlanNode plan) {
    RuntimeFilterGenerator filterGenerator = new RuntimeFilterGenerator();
    filterGenerator.generateFilters(analyzer, plan);
  }

  /**
   * Generates the runtime filters for a query by recursively traversing the single-node
   * plan tree rooted at 'root'. In the top-down traversal of the plan tree, candidate
   * runtime filters are generated from equi-join predicates. In the bottom-up traversal
   * of the plan tree, the filters are assigned to destination (scan) nodes. Filters
   * that cannot be assigned to a scan node are discarded.
   */
  private void generateFilters(Analyzer analyzer, PlanNode root) {
    if (root instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) root;
      List<Expr> joinConjuncts = Lists.newArrayList();
      if (!joinNode.getJoinOp().isLeftOuterJoin()
          && !joinNode.getJoinOp().isFullOuterJoin()
          && !joinNode.getJoinOp().isAntiJoin()) {
        // It's not correct to push runtime filters to the left side of a left outer,
        // full outer or anti join if the filter corresponds to an equi-join predicate
        // from the ON clause.
        joinConjuncts.addAll(joinNode.getEqJoinConjuncts());
      }
      joinConjuncts.addAll(joinNode.getConjuncts());
      List<RuntimeFilter> filters = Lists.newArrayList();
      for (Expr conjunct: joinConjuncts) {
        RuntimeFilter filter = RuntimeFilter.create(filterIdGenerator, analyzer,
            conjunct, joinNode);
        if (filter == null) continue;
        registerRuntimeFilter(filter);
        filters.add(filter);
      }
      generateFilters(analyzer, root.getChild(0));
      // Unregister all the runtime filters for which no destination scan node could be
      // found in the left subtree of the join node. This is to ensure that we don't
      // assign a filter to a scan node from the right subtree of joinNode or ancestor
      // join nodes in case we don't find a destination node in the left subtree.
      for (RuntimeFilter runtimeFilter: filters) {
        if (!runtimeFilter.hasApplyingNode()) unregisterRuntimeFilter(runtimeFilter);
      }
      generateFilters(analyzer, root.getChild(1));
    } else if (root instanceof ScanNode) {
      assignRuntimeFilters(analyzer, (ScanNode) root);
    } else {
      for (PlanNode childNode: root.getChildren()) {
        generateFilters(analyzer, childNode);
      }
    }
  }

  /**
   * Registers a runtime filter with the tuple id of every scan node that is a candidate
   * destination node for that filter.
   */
  private void registerRuntimeFilter(RuntimeFilter filter) {
    Map<TupleId, List<SlotId>> applyExprSlots = filter.getApplyExprSlots();
    Preconditions.checkState(applyExprSlots != null && !applyExprSlots.isEmpty());
    for (TupleId tupleId: applyExprSlots.keySet()) {
      List<RuntimeFilter> filters = runtimeFiltersByTid_.get(tupleId);
      if (filters == null) {
        filters = Lists.newArrayList();
        runtimeFiltersByTid_.put(tupleId, filters);
      }
      filters.add(filter);
    }
  }

  private void unregisterRuntimeFilter(RuntimeFilter runtimeFilter) {
    for (TupleId tupleId: runtimeFilter.getApplyExprSlots().keySet()) {
      runtimeFiltersByTid_.get(tupleId).remove(runtimeFilter);
    }
  }

  /**
   * Assigns runtime filters to a specific scan node 'scanNode'.
   * The assigned filters are the ones for which 'scanNode' can be used a destination
   * node. A scan node may be used as a destination node for multiple runtime filters.
   */
  private void assignRuntimeFilters(Analyzer analyzer, ScanNode scanNode) {
    Preconditions.checkNotNull(scanNode);
    TupleId tid = scanNode.getTupleIds().get(0);
    // Return if no runtime filter is associated with this scan tuple.
    if (!runtimeFiltersByTid_.containsKey(tid)) return;
    for (RuntimeFilter filter: runtimeFiltersByTid_.get(tid)) {
      if (filter.getApplyingNode() != null) continue;
      filter.setFilterApplyingNode(scanNode);
      if (!filter.getApplyFilterExpr().isBound(tid)) {
        Preconditions.checkState(filter.getApplyExprSlots().containsKey(tid));
        // Modify the filter apply expr using the equivalent slots from the scan node
        // on which the filter will be applied.
        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        Expr applyExpr = filter.getApplyFilterExpr();
        List<SlotRef> exprSlots = Lists.newArrayList();
        applyExpr.collect(SlotRef.class, exprSlots);
        List<SlotId> sids = filter.getApplyExprSlots().get(tid);
        for (SlotRef slotRef: exprSlots) {
          for (SlotId sid: sids) {
            if (analyzer.hasValueTransfer(slotRef.getSlotId(), sid)) {
              SlotRef newSlotRef = new SlotRef(analyzer.getSlotDesc(sid));
              newSlotRef.analyzeNoThrow(analyzer);
              smap.put(slotRef, newSlotRef);
              break;
            }
          }
        }
        Preconditions.checkState(exprSlots.size() == smap.size());
        filter.setApplyFilterExpr(applyExpr.substitute(smap, analyzer, false));
      }
      filter.assignToPlanNodes();
    }
  }
}
