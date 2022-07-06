import XCTest
import CSQLite
import Pipeline

class PipelinePerformanceTests: XCTestCase {
	override func setUp() {
		super.setUp()
		// It's necessary to call sqlite3_initialize() since SQLITE_OMIT_AUTOINIT is defined
		XCTAssert(sqlite3_initialize() == SQLITE_OK)
		XCTAssert(csqlite_sqlite3_auto_extension_uuid() == SQLITE_OK)
	}

	override func tearDown() {
		super.tearDown()
		XCTAssert(sqlite3_shutdown() == SQLITE_OK)
	}

	func testPipelineInsertPerformance() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				let s = try! db.prepare(sql: "insert into t1(a, b) values (?, ?);")

				try! s.bind(.int(i*2), toParameter: 1)
				try! s.bind(.int(i*2+1), toParameter: 2)

				try! s.execute()
			}

			stopMeasuring()

			let s = try! db.prepare(sql: "select count(*) from t1;")
			var count = 0
			try! s.results { row in
				count = try row.value(at: 0, .int)
			}

			XCTAssertEqual(count, rowCount)
		}
	}

	func testPipelineInsertPerformance2() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			var s = try! db.prepare(sql: "insert into t1(a, b) values (?, ?);")

			startMeasuring()

			let rowCount = 50_000
			for i in 0..<rowCount {
				try! s.bind(.int(i*2), toParameter: 1)
				try! s.bind(.int(i*2+1), toParameter: 2)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			stopMeasuring()

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count = try! s.step()!.value(at: 0, .int)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testPipelineInsertPerformance31() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);")

			var s = try! db.prepare(sql: "insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

			let values: [DatabaseValue] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

			startMeasuring()

			let rowCount = 10_000
			for _ in 0..<rowCount {
				try! s.bind(values: values)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			stopMeasuring()

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count = try! s.step()!.value(at: 0, .int)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testPipelineInsertPerformance32() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);")

			var s = try! db.prepare(sql: "insert into t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

			let values: [SQLParameter] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

			startMeasuring()

			let rowCount = 10_000
			for _ in 0..<rowCount {
				try! s.bind(values)

				try! s.execute()

				try! s.clearBindings()
				try! s.reset()
			}

			stopMeasuring()

			s = try! db.prepare(sql: "select count(*) from t1;")
			let count = try! s.step()!.value(at: 0, .int)

			XCTAssertEqual(count, rowCount)
		}
	}

	func testPipelineSelectPerformance() {
		self.measureMetrics(XCTestCase.defaultPerformanceMetrics, automaticallyStartMeasuring: false) {
			let db = try! Database()

			try! db.execute(sql: "create table t1(a, b);")

			var s = try! db.prepare(sql: "insert into t1(a, b) values (1, 2);")

			let rowCount = 50_000
			for _ in 0..<rowCount {
				try! s.execute()
				try! s.reset()
			}

			s = try! db.prepare(sql: "select a, b from t1;")

			startMeasuring()

			try! s.results { row in
				_ = try row.value(at: 0, .int)
				_ = try row.value(at: 1, .int)
			}

			stopMeasuring()
		}
	}
}
