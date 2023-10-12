import XCTest
import CSQLite

class CSQLitePerformanceTests: XCTestCase {
	override func setUp() {
		super.setUp()
		// It's necessary to call sqlite3_initialize() since SQLITE_OMIT_AUTOINIT is defined
		XCTAssert(sqlite3_initialize() == SQLITE_OK)
	}

	override func tearDown() {
		super.tearDown()
		XCTAssert(sqlite3_shutdown() == SQLITE_OK)
	}

	func testSQLiteInsertPerformance_1() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				var stmt: OpaquePointer?
				sqlite3_prepare_v2(db, "insert into t1(a, b) values (?, ?);", -1, &stmt, nil)

				sqlite3_bind_int64(stmt, 1, sqlite3_int64(i*2))
				sqlite3_bind_int64(stmt, 2, sqlite3_int64(i*2+1))

				sqlite3_step(stmt)
				sqlite3_finalize(stmt)
			}

			stopMeasuring()

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "select count(*) from t1;", -1, &stmt, nil)
			sqlite3_step(stmt)
			let count = Int(sqlite3_column_int64(stmt, 0))

			sqlite3_finalize(stmt)
			sqlite3_close(db)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testSQLiteInsertPerformance_2() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "insert into t1(a, b) values (?, ?);", -1, &stmt, nil)

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				sqlite3_bind_int64(stmt, 1, sqlite3_int64(i*2))
				sqlite3_bind_int64(stmt, 2, sqlite3_int64(i*2+1))

				sqlite3_step(stmt)

				sqlite3_clear_bindings(stmt)
				sqlite3_reset(stmt)
			}

			stopMeasuring()

			sqlite3_finalize(stmt)

			sqlite3_prepare_v2(db, "select count(*) from t1;", -1, &stmt, nil)
			sqlite3_step(stmt)
			let count = Int(sqlite3_column_int64(stmt, 0))

			sqlite3_finalize(stmt)
			sqlite3_close(db)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testSQLiteSelectPerformance_1() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			var db: OpaquePointer?
			sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)

			sqlite3_exec(db, "create table t1(a, b);", nil, nil, nil)

			var stmt: OpaquePointer?
			sqlite3_prepare_v2(db, "insert into t1(a, b) values (1, 2);", -1, &stmt, nil)

			let rowCount = 50_000
			for _ in 0..<rowCount {
				sqlite3_step(stmt)
				sqlite3_reset(stmt)
			}

			sqlite3_finalize(stmt)

			sqlite3_prepare_v2(db, "select a, b from t1;", -1, &stmt, nil)

			startMeasuring()

			var result = sqlite3_step(stmt)
			while result == SQLITE_ROW {
				let _ = Int(sqlite3_column_int64(stmt, 0))
				let _ = Int(sqlite3_column_int64(stmt, 1))
				result = sqlite3_step(stmt)
			}

			stopMeasuring()

			sqlite3_finalize(stmt)
			sqlite3_close(db)
		}
	}
}
