//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Connection {
	/// A callback for reporting the progress of a database backup.
	///
	/// - parameter remaining: The number of database pages left to copy.
	/// - parameter total: The total number of database pages.
	public typealias BackupProgress = (_ remaining: Int, _ total: Int) -> Void

	/// Backs up the database to the specified URL.
	///
	/// - parameter url: The destination for the backup.
	/// - parameter callback: An optional closure to receive progress information.
	///
	/// - throws: An error if the backup could not be completed.
	///
	/// - seealso: [Online Backup API](https://www.sqlite.org/c3ref/backup_finish.html)
	/// - seealso: [Using the SQLite Online Backup API](https://www.sqlite.org/backup.html)
	public func backup(to url: URL, progress callback: BackupProgress? = nil) throws {
		let destination = try Connection(url: url)

		if let backup = sqlite3_backup_init(destination.databaseConnection, "main", self.databaseConnection, "main") {
			var result: Int32
			repeat {
				result = sqlite3_backup_step(backup, 5)
				callback?(Int(sqlite3_backup_remaining(backup)), Int(sqlite3_backup_pagecount(backup)))
				if result == SQLITE_OK || result == SQLITE_BUSY || result == SQLITE_LOCKED {
					sqlite3_sleep(250)
				}
			} while result == SQLITE_OK || result == SQLITE_BUSY || result == SQLITE_LOCKED

			sqlite3_backup_finish(backup)
		}

		guard sqlite3_errcode(destination.databaseConnection) == SQLITE_OK else {
			throw SQLiteError("Unable to backup database", takingErrorCodeFromDatabaseConnection: destination.databaseConnection)
		}
	}
}
