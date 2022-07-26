//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database {
	/// Begins a database savepoint transaction.
	///
	/// - note: Savepoint transactions may be nested.
	///
	/// - parameter name: The name of the savepoint transaction.
	///
	/// - throws: An error if the savepoint transaction couldn't be started.
	///
	/// - seealso: [SAVEPOINT](https://sqlite.org/lang_savepoint.html)
	public func begin(savepoint name: String) throws {
		guard sqlite3_exec(databaseConnection, "SAVEPOINT '\(name)';", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error creating savepoint \(name)", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Rolls back a database savepoint transaction.
	///
	/// - parameter name: The name of the savepoint transaction.
	///
	/// - throws: An error if the savepoint transaction couldn't be rolled back or doesn't exist.
	public func rollback(to name: String) throws {
		guard sqlite3_exec(databaseConnection, "ROLLBACK TO '\(name)';", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error rolling back to savepoint \(name)", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Releases (commits) a database savepoint transaction.
	///
	/// - note: Changes are not saved until the outermost transaction is released or committed.
	///
	/// - parameter name: The name of the savepoint transaction.
	///
	/// - throws: An error if the savepoint transaction couldn't be committed or doesn't exist.
	public func release(savepoint name: String) throws {
		guard sqlite3_exec(databaseConnection, "RELEASE '\(name)';", nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error releasing savepoint \(name)", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Possible ways to complete a savepoint.
	public enum SavepointCompletion {
		/// The savepoint should be released.
		case release
		/// The savepoint should be rolled back.
		case rollback
	}

	/// A series of database actions grouped into a savepoint transaction.
	///
	/// - parameter database: A `Database` used for database access within the block.
	///
	/// - returns: `.release` if the savepoint should be released or `.rollback` if the savepoint should be rolled back.
	public typealias SavepointBlock = (_ database: Database) throws -> SavepointCompletion

	/// Performs a savepoint transaction on the database.
	///
	/// - parameter block: A closure performing the database operation.
	///
	/// - throws: Any error thrown in `block` or an error if the savepoint could not be started, rolled back, or released.
	///
	/// - note: If `block` throws an error the savepoint will be rolled back and the error will be re-thrown.
	/// - note: If an error occurs releasing the savepoint a rollback will be attempted and the error will be re-thrown.
	public func savepoint(block: SavepointBlock) throws {
		let savepointUUID = UUID().uuidString
		try begin(savepoint: savepointUUID)
		do {
			let action = try block(self)
			switch action {
			case .release:
				try release(savepoint: savepointUUID)
			case .rollback:
				try rollback(to: savepointUUID)
			}
		}
		catch let error {
			try? rollback(to: savepointUUID)
			throw error
		}
	}
}
