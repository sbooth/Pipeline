//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import os.log
import Foundation
import CSQLite

/// A queue providing serialized execution of work items on a database connection.
///
/// A connection queue manages the execution of database operations to ensure they
/// occur one at a time in FIFO order.  This provides thread-safe access to the database
/// connection.
///
/// Database operations may be submitted for synchronous or asynchronous execution.
///
/// The interface is similar to `DispatchQueue` and a dispatch queue is used
/// internally for work item management.
///
/// ```swift
/// let connectionQueue = try ConnectionQueue()
/// try connectionQueue.sync { connection in
///     // Do something with `connection`
/// }
/// ```
///
/// A connection queue also supports transactions and savepoints:
///
/// ```swift
/// connectionQueue.transaction { connection in
///     // All database operations here are contained within a transaction
///     return .commit
/// }
/// ```
public final class ConnectionQueue {
	/// The underlying database connection.
	let connection: Connection
	/// The dispatch queue used to serialize access to the underlying database connection.
	public let queue: DispatchQueue

	/// Creates a connection queue for serialized access to an in-memory database.
	///
	/// - parameter label: The label to attach to the queue.
	/// - parameter qos: The quality of service class for the work performed by the connection queue.
	/// - parameter target: The target dispatch queue on which to execute blocks.
	///
	/// - throws: An error if the connection could not be created.
	public init(label: String, qos: DispatchQoS = .default, target: DispatchQueue? = nil) throws {
		self.connection = try Connection()
		self.queue = DispatchQueue(label: label, qos: qos, target: target)
	}

	/// Creates a connection queue for serialized access to an on-disk database.
	///
	/// - parameter url: The location of the SQLite database.
	/// - parameter label: The label to attach to the queue.
	/// - parameter qos: The quality of service class for the work performed by the connection queue.
	/// - parameter target: The target dispatch queue on which to execute blocks.
	///
	/// - throws: An error if the connection could not be created.
	public init(url: URL, label: String, qos: DispatchQoS = .default, target: DispatchQueue? = nil) throws {
		self.connection = try Connection(url: url)
		self.queue = DispatchQueue(label: label, qos: qos, target: target)
	}

	/// Creates a connection queue for serialized access to an existing database connection.
	///
	/// - attention: The connection queue takes ownership of `connection`.  The result of further use of `connection` is undefined.
	///
	/// - parameter connection: The connection to be serialized.
	/// - parameter label: The label to attach to the queue.
	/// - parameter qos: The quality of service class for the work performed by the connection queue.
	/// - parameter target: The target dispatch queue on which to execute blocks.
	public init(connection: Connection, label: String, qos: DispatchQoS = .default, target: DispatchQueue? = nil) {
		self.connection = connection
		self.queue = DispatchQueue(label: label, qos: qos, target: target)
	}

	/// Performs a synchronous operation on the database connection.
	///
	/// - parameter block: A closure performing the database operation.
	/// - parameter connection: A `Connection` used for database access within `block`.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func sync<T>(block: (_ connection: Connection) throws -> (T)) rethrows -> T {
		try queue.sync {
			try block(connection)
		}
	}

	/// Submits an asynchronous operation to the queue.
	///
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`.
	/// - parameter qos: The quality of service for `block`.
	/// - parameter block: A closure performing the database operation.
	/// - parameter connection: A `Connection` used for database access within `block`.
	public func async(group: DispatchGroup? = nil, qos: DispatchQoS = .default, block: @escaping (_ connection: Connection) -> (Void)) {
		queue.async(group: group, qos: qos) {
			block(self.connection)
		}
	}

	/// Performs a synchronous transaction on the database connection.
	///
	/// - parameter type: The type of transaction to perform.
	/// - parameter block: A closure performing the database operation.
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed.
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown.
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown.
	public func transaction(type: Connection.TransactionType = .deferred, _ block: Connection.TransactionBlock) throws {
		try queue.sync {
			try connection.transaction(type: type, block)
		}
	}

	/// Submits an asynchronous transaction to the queue.
	///
	/// - parameter type: The type of transaction to perform.
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`.
	/// - parameter qos: The quality of service for `block`.
	/// - parameter block: A closure performing the database operation.
	public func asyncTransaction(type: Connection.TransactionType = .deferred, group: DispatchGroup? = nil, qos: DispatchQoS = .default, _ block: @escaping Connection.TransactionBlock) {
		queue.async(group: group, qos: qos) {
			do {
				try self.connection.transaction(type: type, block)
			} catch let error {
				os_log("Error performing database transaction: %{public}@", type: .info, error.localizedDescription)
			}
		}
	}

	/// Performs a synchronous savepoint on the database connection.
	///
	/// - parameter block: A closure performing the database operation.
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released.
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown.
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown.
	public func savepoint(block: Connection.SavepointBlock) throws {
		try queue.sync {
			try connection.savepoint(block: block)
		}
	}

	/// Submits an asynchronous savepoint to the queue.
	///
	/// - parameter group: An optional `DispatchGroup` with which to associate `block`.
	/// - parameter qos: The quality of service for `block`.
	/// - parameter block: A closure performing the database operation.
	public func asyncSavepoint(group: DispatchGroup? = nil, qos: DispatchQoS = .default, block: @escaping Connection.SavepointBlock) {
		queue.async(group: group, qos: qos) {
			do {
				try self.connection.savepoint(block: block)
			} catch let error {
				os_log("Error performing database savepoint: %{public}@", type: .info, error.localizedDescription)
			}
		}
	}

	/// Performs an unsafe database connection operation.
	///
	/// - warning: To ensure thread safety all access to `connection` within `block` **must** use `queue`.
	/// - attention: Use of this function should be avoided whenever possible.
	///
	/// - parameter block: A closure performing the database operation.
	/// - parameter connection: The `Connection` object.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeDatabase<T>(block: (_ connection: Connection) throws -> (T)) rethrows -> T {
		try block(connection)
	}
}