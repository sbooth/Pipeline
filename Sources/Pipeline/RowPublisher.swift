//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

#if canImport(Combine)

import Foundation
import CSQLite
import Combine

extension Connection {
	/// Creates and returns a publisher for an SQL statement's result rows.
	///
	/// - parameter sql: The SQL statement to compile.
	/// - parameter bindings: A closure binding desired SQL parameters.
	///
	/// - returns: A publisher for the statement's result rows.
	public func rowPublisher(sql: String, bindings: @escaping (_ statement: Statement) throws -> Void = { _ in }) -> AnyPublisher<Row, SQLiteError> {
		Publishers.RowPublisher(connection: self, sql: sql, bindings: bindings)
			.eraseToAnyPublisher()
	}
}

private extension Publishers {
	struct RowPublisher: Publisher {
		typealias Output = Row
		typealias Failure = SQLiteError

		private let connection: Connection
		private let sql: String
		private let bindings: (_ statement: Statement) throws -> Void

		init(connection: Connection, sql: String, bindings: @escaping (_ statement: Statement) throws -> Void) {
			self.connection = connection
			self.sql = sql
			self.bindings = bindings
		}

		func receive<S>(subscriber: S) where S: Subscriber, Failure == S.Failure, Output == S.Input {
			do {
				let statement = try connection.prepare(sql: sql)
				try bindings(statement)
				let subscription = Subscription(subscriber: subscriber, statement: statement)
				subscriber.receive(subscription: subscription)
			} catch let error as SQLiteError {
				Fail<Output, Failure>(error: error).subscribe(subscriber)
			} catch {
				Fail<Output, Failure>(error: SQLiteError("Unknown error creating a row publisher subscription. Did the binding closure throw something other than SQLiteError?")).subscribe(subscriber)
			}
		}
	}
}

private extension Publishers.RowPublisher {
	final class Subscription<S>: Combine.Subscription where S: Subscriber, S.Input == Output, S.Failure == Failure {
		/// The subscriber.
		private let subscriber: AnySubscriber<Output, Failure>
		/// The current subscriber demand.
		private var demand: Subscribers.Demand = .none
		/// The statement providing the result rows.
		private let statement: Statement

		init(subscriber: S, statement: Statement) {
			self.subscriber = AnySubscriber(subscriber)
			self.statement = statement
		}

		func request(_ demand: Subscribers.Demand) {
			self.demand = demand
			while self.demand != .none {
				let result = sqlite3_step(statement.preparedStatement)
				switch result {
				case SQLITE_ROW:
					self.demand -= 1
					self.demand += subscriber.receive(Row(statement: statement))
				case SQLITE_DONE:
					subscriber.receive(completion: .finished)
					self.demand = .none
				default:
					subscriber.receive(completion: .failure(SQLiteError("Error evaluating statement", takingErrorCodeFromPreparedStatement: statement.preparedStatement)))
					self.demand = .none
				}
			}
		}

		func cancel() {
			demand = .none
		}
	}
}

#endif
