//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Statement {
	/// Available statement counters.
	///
	/// - seealso: [Status Parameters for prepared statements](https://www.sqlite.org/c3ref/c_stmtstatus_counter.html)
	public enum	Counter {
		/// The number of times that SQLite has stepped forward in a table as part of a full table scan.
		case fullscanStep
		/// The number of sort operations that have occurred.
		case sort
		/// The number of rows inserted into transient indices that were created automatically in order to help joins run faster.
		case autoindex
		/// The number of virtual machine operations executed by the prepared statement.
		case vmStep
		/// The number of times that the prepare statement has been automatically regenerated due to schema changes or change to bound parameters that might affect the query plan.
		case reprepare
		/// The number of times that the prepared statement has been run.
		case run
		/// The approximate number of bytes of heap memory used to store the prepared statement.
		case memused
	}

	/// Returns information on a statement counter.
	///
	/// - parameter counter: The desired statement counter.
	/// - parameter reset: If `true` the counter is reset to zero.
	///
	/// - returns: The current value of the counter.
	///
	/// - seealso: [Prepared Statement Status](https://www.sqlite.org/c3ref/stmt_status.html)
	public func count(of counter: Counter, reset: Bool = false) -> Int {
		let op: Int32
		switch counter {
		case .fullscanStep: 	op = SQLITE_STMTSTATUS_FULLSCAN_STEP
		case .sort:				op = SQLITE_STMTSTATUS_SORT
		case .autoindex:		op = SQLITE_STMTSTATUS_AUTOINDEX
		case .vmStep:			op = SQLITE_STMTSTATUS_VM_STEP
		case .reprepare:		op = SQLITE_STMTSTATUS_REPREPARE
		case .run:				op = SQLITE_STMTSTATUS_RUN
		case .memused:			op = SQLITE_STMTSTATUS_MEMUSED
		}

		return Int(sqlite3_stmt_status(preparedStatement, op, reset ? 1 : 0))
	}
}
