package org.springframework.data.neo4j.repository.query;

import org.neo4j.cypherdsl.core.Condition;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Expression;
import org.neo4j.cypherdsl.core.PatternElement;
import org.neo4j.cypherdsl.core.SortItem;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.StatementBuilder;
import org.springframework.data.neo4j.core.mapping.CypherGenerator;
import org.springframework.data.neo4j.core.mapping.Neo4jPersistentEntity;
import org.springframework.data.neo4j.core.mapping.PropertyFilter;
import org.springframework.data.neo4j.core.mapping.NodeDescription;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class StatementBuilderHelper {

	private final QueryFragments queryFragments;

	public StatementBuilderHelper(QueryFragments queryFragments) {
		this.queryFragments = queryFragments;
	}

	public Statement toStatement() {

		StatementBuilder.OngoingReadingWithoutWhere match = null;

		for (PatternElement patternElement : queryFragments.getMatchOn()) {
			if (match == null) {
				match = Cypher.match(queryFragments.getMatchOn().get(0));
			} else {
				match = match.match(patternElement);
			}
		}

		StatementBuilder.OngoingReadingWithWhere matchWithWhere = match.where(queryFragments.getCondition());

		if (queryFragments.getDeleteExpression() != null) {
			matchWithWhere = (StatementBuilder.OngoingReadingWithWhere) matchWithWhere.detachDelete(queryFragments.getDeleteExpression());
		}

		StatementBuilder.OngoingReadingAndReturn returnPart = isDistinctReturn()
				? matchWithWhere.returningDistinct(getReturnExpressions())
				: matchWithWhere.returning(getReturnExpressions());

		Statement statement = returnPart
				.orderBy(getOrderBy())
				.skip(queryFragments.getSkip())
				.limit(queryFragments.getLimit()).build();

		statement.setRenderConstantsAsParameters(queryFragments.isRenderConstantsAsParameters());
		return statement;
	}

	private Collection<Expression> getReturnExpressions() {
		return queryFragments.getReturnExpressions().size() > 0
				? queryFragments.getReturnExpressions()
				: CypherGenerator.INSTANCE.createReturnStatementForMatch(
				(Neo4jPersistentEntity<?>) queryFragments.getReturnTuple().getNodeDescription(),
				queryFragments::includeField);
	}

	private boolean isDistinctReturn() {
		return queryFragments.getReturnExpressions().isEmpty() && queryFragments.getReturnTuple().isDistinct();
	}

	public Collection<SortItem> getOrderBy() {

		if (queryFragments.getOrderBy() == null) {
			return List.of();
		} else if (!queryFragments.requiresReverseSort()) {
			return queryFragments.getOrderBy();
		} else {
			return queryFragments.getOrderBy().stream().map(StatementBuilderHelper::reverse).toList();
		}
	}

	// Yeah, would be kinda nice having a simple method in Cypher-DSL ;)
	private static SortItem reverse(SortItem sortItem) {

		var sortedExpression = new AtomicReference<Expression>();
		var sortDirection = new AtomicReference<SortItem.Direction>();

		sortItem.accept(segment -> {
			if (segment instanceof SortItem.Direction direction) {
				sortDirection.compareAndSet(null, direction == SortItem.Direction.UNDEFINED || direction == SortItem.Direction.ASC ? SortItem.Direction.DESC : SortItem.Direction.ASC);
			} else if (segment instanceof Expression expression) {
				sortedExpression.compareAndSet(null, expression);
			}
		});

		// Default might not explicitly set.
		sortDirection.compareAndSet(null, SortItem.Direction.DESC);
		return Cypher.sort(sortedExpression.get(), sortDirection.get());
	}
}
