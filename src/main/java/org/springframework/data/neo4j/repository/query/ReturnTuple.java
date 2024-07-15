package org.springframework.data.neo4j.repository.query;

import org.springframework.data.neo4j.core.mapping.Neo4jPersistentProperty;
import org.springframework.data.neo4j.core.mapping.PropertyFilter;
import org.springframework.data.neo4j.core.mapping.NodeDescription;
import org.springframework.data.neo4j.core.schema.Property;

import java.util.Collection;

final class ReturnTuple {
	final NodeDescription<?> nodeDescription;
	final PropertyFilter filteredProperties;
	final boolean isDistinct;

	ReturnTuple(NodeDescription<?> nodeDescription, Collection<PropertyFilter.ProjectedPath> filteredProperties, boolean isDistinct) {
		this.nodeDescription = nodeDescription;
		this.filteredProperties = PropertyFilter.from(filteredProperties, nodeDescription);
		this.isDistinct = isDistinct;
	}

	boolean include(PropertyFilter.RelaxedPropertyPath fieldName) {
		String dotPath = nodeDescription.getGraphProperty(fieldName.getSegment())
				.filter(Neo4jPersistentProperty.class::isInstance)
				.map(Neo4jPersistentProperty.class::cast)
				.filter(p -> p.findAnnotation(Property.class) != null)
				.map(p -> fieldName.toDotPath(p.getPropertyName()))
				.orElseGet(fieldName::toDotPath);
		return this.filteredProperties.contains(dotPath, fieldName.getType());
	}
	public NodeDescription<?> getNodeDescription() {
		return nodeDescription;
	}

	public boolean isDistinct() {
		return isDistinct;
	}
}
