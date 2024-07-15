package org.springframework.data.neo4j.repository.query;

import org.apiguardian.api.API;
import org.neo4j.cypherdsl.core.*;
import org.springframework.data.neo4j.core.mapping.PropertyFilter;
import org.springframework.data.neo4j.core.mapping.NodeDescription;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Collects the parts of a Cypher query to be handed over to the Cypher generator.
 *
 * @since 6.0.4
 */
@API(status = API.Status.INTERNAL, since = "6.0.4")
public final class QueryFragments {
	private List<PatternElement> matchOn = new ArrayList<>();
	private Condition condition;
	private Collection<Expression> returnExpressions = new ArrayList<>();
	private Collection<SortItem> orderBy;
	private Number limit;
	private Long skip;
	private ReturnTuple returnTuple;
	private boolean scalarValueReturn = false;
	private boolean renderConstantsAsParameters = false;
	private Expression deleteExpression;
	/**
	 * This flag becomes {@literal true} for backward scrolling keyset pagination. Any {@code AbstractNeo4jQuery} will in turn reverse the result list.
	 */
	private boolean requiresReverseSort = false;
	private Predicate<PropertyFilter.RelaxedPropertyPath> projectingPropertyFilter;

	public void addMatchOn(PatternElement match) {
		this.matchOn.add(match);
	}

	public void setMatchOn(List<PatternElement> match) {
		this.matchOn = match;
	}

	public List<PatternElement> getMatchOn() {
		return matchOn;
	}

	public void setCondition(@Nullable Condition condition) {
		this.condition = Optional.ofNullable(condition).orElse(org.neo4j.cypherdsl.core.Cypher.noCondition());
	}

	public Condition getCondition() {
		return condition;
	}

	public void setReturnExpressions(Collection<Expression> expression) {
		this.returnExpressions = expression;
	}

	public void setDeleteExpression(Expression expression) {
		this.deleteExpression = expression;
	}

	public void setReturnExpression(Expression returnExpression, boolean isScalarValue) {
		this.returnExpressions = Collections.singletonList(returnExpression);
		this.scalarValueReturn = isScalarValue;
	}

	public void setProjectingPropertyFilter(Predicate<PropertyFilter.RelaxedPropertyPath> projectingPropertyFilter) {
		this.projectingPropertyFilter = projectingPropertyFilter;
	}

	public boolean includeField(PropertyFilter.RelaxedPropertyPath fieldName) {
		return (projectingPropertyFilter == null || projectingPropertyFilter.test(fieldName))
				&& (this.returnTuple == null || this.returnTuple.include(fieldName));
	}

	public void setOrderBy(Collection<SortItem> orderBy) {
		this.orderBy = orderBy;
	}

	public void setLimit(Number limit) {
		this.limit = limit;
	}

	public void setSkip(Long skip) {
		this.skip = skip;
	}

	public void setReturnBasedOn(NodeDescription<?> nodeDescription, Collection<PropertyFilter.ProjectedPath> includedProperties,
								 boolean isDistinct) {
		this.returnTuple = new ReturnTuple(nodeDescription, includedProperties, isDistinct);
	}

	public boolean isScalarValueReturn() {
		return scalarValueReturn;
	}

	public boolean requiresReverseSort() {
		return requiresReverseSort;
	}

	public void setRequiresReverseSort(boolean requiresReverseSort) {
		this.requiresReverseSort = requiresReverseSort;
	}

	public void setRenderConstantsAsParameters(boolean renderConstantsAsParameters) {
		this.renderConstantsAsParameters = renderConstantsAsParameters;
	}

	public Statement toStatement() {
		return new StatementBuilderHelper(this).toStatement();
	}

	public Collection<Expression> getReturnExpressions() {
		return returnExpressions;
	}

	public ReturnTuple getReturnTuple() {
		return returnTuple;
	}

	public Expression getDeleteExpression() {
		return deleteExpression;
	}

	public Collection<SortItem> getOrderBy() {
		return orderBy;
	}

	public Number getLimit() {
		return limit;
	}

	public Long getSkip() {
		return skip;
	}

	public boolean isRenderConstantsAsParameters() {
		return renderConstantsAsParameters;
	}
}