//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Connection {
	/// Available database connection status parameters.
	///
	/// - seealso: [Status Parameters for database connections](https://www.sqlite.org/c3ref/c_dbstatus_options.html)
	public enum StatusParameter {
		/// The number of lookaside memory slots currently checked out.
		case lookasideUsed
		/// The approximate number of bytes of heap memory used by all pager caches.
		case cacheUsed
		/// The approximate number of bytes of heap memory used to store the schema for all databases.
		case schemaUsed
		/// The approximate number of bytes of heap and lookaside memory used by all prepared statements.
		case stmtUsed
		/// The number malloc attempts that were satisfied using lookaside memory.
		case lookasideHit
		/// The number malloc attempts that might have been satisfied using lookaside memory but failed due to the amount of memory requested being larger than the lookaside slot size.
		case lookasideMissSize
		/// The number malloc attempts that might have been satisfied using lookaside memory but failed due to all lookaside memory already being in use.
		case lookasideMissFull
		/// The number of pager cache hits that have occurred.
		case cacheHit
		/// The number of pager cache misses that have occurred.
		case cacheMiss
		/// The number of dirty cache entries that have been written to disk.
		case cacheWrite
		/// Returns zero for the current value if and only if all foreign key constraints (deferred or immediate) have been resolved.
		case deferredForeignKeys
		/// Similar to `cacheUsed` except that if a pager cache is shared between two or more connections the bytes of heap memory used by that pager cache is divided evenly between the attached connections.
		case cacheUsedShared
	}

	/// Returns status information on the current and highwater values of `parameter`.
	///
	/// - note: Not all parameters support both current and highwater values.
	///
	/// - parameter parameter: The desired database parameter.
	/// - parameter resetHighwater: If `true` the highwater mark, if applicable, is reset to the current value.
	///
	/// - returns: A tuple containing the current and highwater values of the requested parameter, as applicable.
	///
	/// - seealso: [Database Connection Status](https://www.sqlite.org/c3ref/db_status.html)
	public func status(ofParameter parameter: StatusParameter, resetHighwater: Bool = false) throws -> (Int, Int) {
		let op: Int32
		switch parameter {
		case .lookasideUsed:
			op = SQLITE_DBSTATUS_LOOKASIDE_USED
		case .cacheUsed:
			op = SQLITE_DBSTATUS_CACHE_USED
		case .schemaUsed:
			op = SQLITE_DBSTATUS_SCHEMA_USED
		case .stmtUsed:
			op = SQLITE_DBSTATUS_STMT_USED
		case .lookasideHit:
			op = SQLITE_DBSTATUS_LOOKASIDE_HIT
		case .lookasideMissSize:
			op = SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE
		case .lookasideMissFull:
			op = SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL
		case .cacheHit:
			op = SQLITE_DBSTATUS_CACHE_HIT
		case .cacheMiss:
			op = SQLITE_DBSTATUS_CACHE_MISS
		case .cacheWrite:
			op = SQLITE_DBSTATUS_CACHE_WRITE
		case .deferredForeignKeys:
			op = SQLITE_DBSTATUS_DEFERRED_FKS
		case .cacheUsedShared:
			op = SQLITE_DBSTATUS_CACHE_USED_SHARED
		}

		var current: Int32 = 0
		var highwater: Int32 = 0

		guard sqlite3_db_status(databaseConnection, op, &current, &highwater, resetHighwater ? 1 : 0) == SQLITE_OK else {
			throw SQLiteError("Error retrieving status of database parameter \(parameter)", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}

		return (Int(current), Int(highwater))
	}
}
