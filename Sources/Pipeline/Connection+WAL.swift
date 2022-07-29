//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// Possible write-ahead log (WAL) checkpoint types.
///
/// - seealso: [Write-Ahead Logging](https://www.sqlite.org/wal.html)
public enum WALCheckpointType {
	/// Checkpoint as many frames as possible without waiting for any database readers or writers to finish.
	case passive
	/// Blocks until there is no writer and all readers are reading from the most recent database snapshot then checkpoints all frames.
	case full
	/// Same as `WALCheckpointType.full` except after checkpointing it blocks until all readers are reading from the database file.
	case restart
	/// Same as `WALCheckpointType.restart` except it also truncates the log file prior to a successful return.
	case truncate
}

extension Connection {
	/// Perform a write-ahead log checkpoint on the database.
	///
	/// - note: Checkpoints are only valid for databases using write-ahead logging (WAL) mode.
	///
	/// - parameter type: The type of checkpoint to perform.
	///
	/// - throws: An error if the checkpoint failed or if the database isn't in WAL mode.
	///
	/// - seealso: [Checkpoint a database](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html)
	/// - seealso: [PRAGMA wal_checkpoint](https://www.sqlite.org/pragma.html#pragma_wal_checkpoint)
	public func walCheckpoint(type: WALCheckpointType = .passive) throws {
		let mode: Int32
		switch type {
		case .passive:
			mode = SQLITE_CHECKPOINT_PASSIVE
		case .full:
			mode = SQLITE_CHECKPOINT_FULL
		case .restart:
			mode = SQLITE_CHECKPOINT_RESTART
		case .truncate:
			mode = SQLITE_CHECKPOINT_TRUNCATE
		}

		guard sqlite3_wal_checkpoint_v2(databaseConnection, nil, mode, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error performing \(type) WAL checkpoint", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}
}
