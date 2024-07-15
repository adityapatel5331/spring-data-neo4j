package org.springframework.data.neo4j.core;

import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;
import org.springframework.core.log.LogAccessor;

import static org.neo4j.driver.NotificationCategory.*;

/**
 * Utility class for dealing with result summaries.
 *
 * @author Michael J. Simons
 * @soundtrack Fatoni & Dexter - Yo, Picasso
 * @since 6.0
 */
final class ResultSummaries {

	private static final String LINE_SEPARATOR = System.lineSeparator();

	private static final LogAccessor cypherLog = new LogAccessor(LogFactory.getLog("org.springframework.data.neo4j.cypher"));

	private static final LogAccessor cypherPerformanceNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.performance");
	private static final LogAccessor cypherHintNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.hint");
	private static final LogAccessor cypherUnrecognizedNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.unrecognized");
	private static final LogAccessor cypherUnsupportedNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.unsupported");
	private static final LogAccessor cypherDeprecationNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.deprecation");
	private static final LogAccessor cypherGenericNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.generic");
	private static final LogAccessor cypherSecurityNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.security");
	private static final LogAccessor cypherTopologyNotificationLog = getLogAccessor("org.springframework.data.neo4j.cypher.topology");

	/**
	 * Does some post-processing on the given result summary, especially logging all notifications
	 * and potentially query plans.
	 *
	 * @param resultSummary The result summary to process
	 * @return The same, unmodified result summary.
	 */
	static ResultSummary process(ResultSummary resultSummary) {
		logNotifications(resultSummary);
		logPlan(resultSummary);
		return resultSummary;
	}

	private static void logNotifications(ResultSummary resultSummary) {
		if (!cypherLog.isWarnEnabled() || resultSummary.notifications().isEmpty()) {
			return;
		}

		String query = resultSummary.query().text();
		resultSummary.notifications().forEach(notification -> {
			LogAccessor log = getLogAccessor(notification.category().orElse(null));
			Consumer<String> logFunction = getLogFunction(notification.severity());
			logFunction.accept(format(notification, query));
		});
	}

	private static Consumer<String> getLogFunction(String severity) {
		switch (severity) {
			case "WARNING":
				return cypherLog::warn;
			case "INFORMATION":
				return cypherLog::info;
			default:
				return cypherLog::debug;
		}
	}

	private static LogAccessor getLogAccessor(NotificationCategory category) {

		if (category.equals(HINT)) {
			return cypherHintNotificationLog;
		} else if (category.equals(DEPRECATION)) {
			return cypherDeprecationNotificationLog;
		} else if (category.equals(PERFORMANCE)) {
			return cypherPerformanceNotificationLog;
		} else if (category.equals(GENERIC)) {
			return cypherGenericNotificationLog;
		} else if (category.equals(UNSUPPORTED)) {
			return cypherUnsupportedNotificationLog;
		} else if (category.equals(UNRECOGNIZED)) {
			return cypherUnrecognizedNotificationLog;
		} else if (category.equals(SECURITY)) {
			return cypherSecurityNotificationLog;
		} else if (category.equals(TOPOLOGY)) {
			return cypherTopologyNotificationLog;
		}
		return cypherLog;
	}

	/**
	 * Creates a formatted string for a notification issued for a given query.
	 *
	 * @param notification The notification to format
	 * @param forQuery     The query that caused the notification
	 * @return A formatted string
	 */
	private static String format(Notification notification, String forQuery) {
		InputPosition position = notification.position();
		boolean hasPosition = position != null;

		StringBuilder queryHint = new StringBuilder();
		String[] lines = forQuery.split("(\r\n|\n)");
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			queryHint.append("\t").append(line).append(LINE_SEPARATOR);
			if (hasPosition && i + 1 == position.line()) {
				queryHint.append("\t").append(Stream.generate(() -> " ").limit(position.column() - 1)
						.collect(Collectors.joining())).append("^").append(System.lineSeparator());
			}
		}
		return String.format("%s: %s%n%s%s", notification.code(), notification.title(), queryHint,
				notification.description());
	}

	/**
	 * Logs the plan of the result summary if available and log level is at least debug.
	 *
	 * @param resultSummary The result summary that might contain a plan
	 */
	private static void logPlan(ResultSummary resultSummary) {
		if (!resultSummary.hasPlan() || !cypherLog.isDebugEnabled()) {
			return;
		}

		Consumer<String> log = cypherLog::debug;
		log.accept("Plan:");
		printPlan(log, resultSummary.plan(), 0);
	}

	private static void printPlan(Consumer<String> log, Plan plan, int level) {
		String tabs = Stream.generate(() -> "\t").limit(level).collect(Collectors.joining());
		log.accept(tabs + "operatorType: " + plan.operatorType());
		log.accept(tabs + "identifiers: " + String.join(",", plan.identifiers()));
		log.accept(tabs + "arguments: ");
		plan.arguments().forEach((k, v) -> log.accept(tabs + "\t" + k + "=" + v));
		if (!plan.children().isEmpty()) {
			log.accept(tabs + "children: ");
			plan.children().forEach(childPlan -> printPlan(log, childPlan, level + 1));
		}
	}

	private static LogAccessor getLogAccessor(String name) {
		return new LogAccessor(LogFactory.getLog(name));
	}

	private ResultSummaries() {
	}
}
