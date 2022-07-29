//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// Possible database transaction types.
///
/// - seealso: [Transactions in SQLite](https://sqlite.org/lang_transaction.html)
public enum TransactionType {
	/// A deferred transaction.
	case deferred
	/// An immediate transaction.
	case immediate
	/// An exclusive transaction.
	case exclusive
}

/// Possible transaction states for a database.
///
/// - seealso: [Determine the transaction state of a database](https://www.sqlite.org/c3ref/txn_state.html)
public enum TransactionState {
	/// No transaction is currently pending.
	case none
	/// The database is currently in a read transaction.
	case read
	/// The database is currently in a write transaction.
	case write
}

extension Connection {
	/// Begins a database transaction.
	///
	/// - note: Database transactions may not be nested.
	///
	/// - parameter type: The type of transaction to initiate.
	///
	/// - throws: An error if the transaction couldn't be started.
	///
	/// - seealso: [BEGIN TRANSACTION](https://sqlite.org/lang_transaction.html)
	public func begin(type: TransactionType = .deferred) throws {
		let sql: String
		switch type {
		case .deferred:
			sql = "BEGIN DEFERRED TRANSACTION;"
		case .immediate:
			sql = "BEGIN IMMEDIATE TRANSACTION;"
		case .exclusive:
			sql = "BEGIN EXCLUSIVE TRANSACTION;"
		}

		guard sqlite3_exec(databaseConnection, sql, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error beginning \(type) transaction", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Rolls back the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be rolled back or there is no active transaction.
	public func rollback() throws {
		guard sqlite3_exec(databaseConnection, "ROLLBACK;", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error rolling back", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Commits the active database transaction.
	///
	/// - throws: An error if the transaction couldn't be committed or there is no active transaction.
	public func commit() throws {
		guard sqlite3_exec(databaseConnection, "COMMIT;", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error committing", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Determines the transaction state of a database.
	///
	/// - note: If `schema` is `nil` the highest transaction state of any schema is returned.
	///
	/// - parameter schema: The name of the database schema to query or `nil`.
	///
	/// - throws: An error if `schema` is not the name of a known schema.
	public func transactionState(_ schema: String? = nil) throws -> TransactionState {
		let transactionState = sqlite3_txn_state(databaseConnection, schema)
		switch transactionState {
		case SQLITE_TXN_NONE:
			return .none
		case SQLITE_TXN_READ:
			return .read
		case SQLITE_TXN_WRITE:
			return .write
		default:
			fatalError("Unknown SQLite transaction state \(transactionState) encountered")
		}
	}

	/// `true` if this database is in autocommit mode, `false` otherwise.
	///
	/// - seealso: [Test For Auto-Commit Mode](https://www.sqlite.org/c3ref/get_autocommit.html)
	public var isInAutocommitMode: Bool {
		sqlite3_get_autocommit(databaseConnection) != 0
	}

	/// Possible ways to complete a transaction.
	public enum TransactionCompletion {
		/// The transaction should be committed.
		case commit
		/// The transaction should be rolled back.
		case rollback
	}

	/// A series of database actions grouped into a transaction.
	///
	/// - parameter connection: A `Connection` used for database access within the block.
	///
	/// - returns: `.commit` if the transaction should be committed or `.rollback` if the transaction should be rolled back.
	public typealias TransactionBlock = (_ connection: Connection) throws -> TransactionCompletion

	/// Performs a transaction on the database.
	///
	/// - parameter type: The type of transaction to perform.
	/// - parameter block: A closure performing the database operation.
	///
	/// - throws: Any error thrown in `block` or an error if the transaction could not be started, rolled back, or committed.
	///
	/// - returns: The result of the transaction.
	///
	/// - note: If `block` throws an error the transaction will be rolled back and the error will be re-thrown.
	/// - note: If an error occurs committing the transaction a rollback will be attempted and the error will be re-thrown.
	public func transaction(type: TransactionType = .deferred, _ block: TransactionBlock) throws -> TransactionCompletion {
		try begin(type: type)
		do {
			let action = try block(self)
			switch action {
			case .commit:
				try commit()
				return .commit
			case .rollback:
				try rollback()
				return .rollback
			}
		} catch let error {
			if !isInAutocommitMode {
				try rollback()
			}
			throw error
		}
	}
}
