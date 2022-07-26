//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import XCTest
import CSQLite
#if canImport(Combine)
import Combine
#endif
@testable import Pipeline

final class PipelineTests: XCTestCase {
	override class func setUp() {
		super.setUp()
		// It's necessary to call sqlite3_initialize() since SQLITE_OMIT_AUTOINIT is defined
		XCTAssert(sqlite3_initialize() == SQLITE_OK)
		XCTAssert(csqlite_sqlite3_auto_extension_uuid() == SQLITE_OK)
		XCTAssert(csqlite_sqlite3_auto_extension_carray() == SQLITE_OK)
	}

	override class func tearDown() {
		super.tearDown()
		XCTAssert(sqlite3_shutdown() == SQLITE_OK)
	}

	func testSQLiteKeywords() {
		XCTAssertTrue(SQLite.isKeyword("BEGIN"))
		XCTAssertTrue(SQLite.isKeyword("begin"))
		XCTAssertTrue(SQLite.isKeyword("BeGiN"))
		XCTAssertFalse(SQLite.isKeyword("BEGINNING"))
	}

	func testDatabaseValueLiterals() {
		var v: DatabaseValue
		v = nil
		v = 100
		v = 10.0
		v = "lulu"
		v = false
		// Suppress compiler warning
		_ = v
	}

	func testDatabase() {
		let db = try! Database()
		XCTAssertNoThrow(try db.execute(sql: "create table t1(v1);"))

		let rowCount = 10
		for _ in 0 ..< rowCount {
			XCTAssertNoThrow(try db.execute(sql: "insert into t1 default values;"))
		}

		let count = try! db.prepare(sql: "select count(*) from t1;").step()!.get(.int, at: 0)
		XCTAssertEqual(count, rowCount)
	}

	func testBatch() {
		let db = try! Database()

		try! db.batch(sql: "pragma application_id;")
		try! db.batch(sql: "pragma application_id; pragma foreign_keys;")

		XCTAssertThrowsError(try db.batch(sql: "lulu"))

		try! db.batch(sql: "pragma application_id;") { row in
			XCTAssertEqual(row.keys.count, 1)
			XCTAssertEqual(row["application_id"], "0")
		}
	}

	func testInsert() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a text);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameters: [1])
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: ["feisty"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: 2.5)
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: .data(Data(count: 8)))

		try! db.prepare(sql: "insert into t1(a) values (?);").bind(.urlString(URL(fileURLWithPath: "/tmp")), toParameter: 1).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(.uuidString(UUID())).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind([.timeIntervalSinceReferenceDate(Date())]).execute()

		try! db.execute(sql: "insert into t1(a) values (?);", parameters: [.null])
	}

	func testIteration() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a);")

		let rowCount = 10

		for i in 0..<rowCount {
			try! db.prepare(sql: "insert into t1(a) values (?);").bind(.int(i)).execute()
		}

		let s = try! db.prepare(sql: "select * from t1;")
		var count = 0

		for row in s {
			for _ in row {
				XCTAssert(try! row.get(.int, at: 0) == count)
			}
			count += 1
		}

		XCTAssertEqual(count, rowCount)
	}

	func testIteration2() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a,b,c,d);")

		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(1,2,3,4).execute()
		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind("a","b","c","d").execute()
		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind("a",2,"c",4).execute()

		do {
			let s = try! db.prepare(sql: "select * from t1 limit 1 offset 0;")
			let r = try! s.step()!
			let v = [DatabaseValue](r)

			XCTAssertEqual(v, [DatabaseValue(1),DatabaseValue(2),DatabaseValue(3),DatabaseValue(4)])
		}

		do {
			let s = try! db.prepare(sql: "select * from t1 limit 1 offset 1;")
			let r = try! s.step()!
			let v = [DatabaseValue](r)

			XCTAssertEqual(v, [DatabaseValue("a"),DatabaseValue("b"),DatabaseValue("c"),DatabaseValue("d")])
		}
	}

	func testCodable() {
		let db = try! Database()

		struct TestStruct: Codable {
			let a: Int
			let b: Float
			let c: Date
			let d: String
		}

		try! db.execute(sql: "create table t1(a);")

		let a = TestStruct(a: 1, b: 3.14, c: Date(), d: "Lu")

		try! db.prepare(sql: "insert into t1(a) values (?);").bind(.json(a), toParameter: 1).execute()

		let b = try! db.prepare(sql: "select * from t1 limit 1;").step()!.get(.json(TestStruct.self), at: 0)

		XCTAssertEqual(a.a, b.a)
		XCTAssertEqual(a.b, b.b)
		XCTAssertEqual(a.c, b.c)
		XCTAssertEqual(a.d, b.d)
	}

	func testCustomCollation() {
		let db = try! Database()

		try! db.addCollation("reversed", { (a, b) -> ComparisonResult in
			return b.compare(a)
		})

		try! db.execute(sql: "create table t1(a text);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameters: "a")
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: ["c"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: .string("z"))
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: [.string("e")])

		var str = ""
		let s = try! db.prepare(sql: "select * from t1 order by a collate reversed;")
		try! s.results { row in
			let c = try row.get(.string, at: 0)
			str.append(c)
		}

		XCTAssertEqual(str, "zeca")
	}

	func testCustomFunction() {
		let db = try! Database()

		let rot13key: [Character: Character] = [
			"A": "N", "B": "O", "C": "P", "D": "Q", "E": "R", "F": "S", "G": "T", "H": "U", "I": "V", "J": "W", "K": "X", "L": "Y", "M": "Z",
			"N": "A", "O": "B", "P": "C", "Q": "D", "R": "E", "S": "F", "T": "G", "U": "H", "V": "I", "W": "J", "X": "K", "Y": "L", "Z": "M",
			"a": "n", "b": "o", "c": "p", "d": "q", "e": "r", "f": "s", "g": "t", "h": "u", "i": "v", "j": "w", "k": "x", "l": "y", "m": "z",
			"n": "a", "o": "b", "p": "c", "q": "d", "r": "e", "s": "f", "t": "g", "u": "h", "v": "i", "w": "j", "x": "k", "y": "l", "z": "m"]

		func rot13(_ s: String) -> String {
			return String(s.map { rot13key[$0] ?? $0 })
		}

		try! db.addFunction("rot13", arity: 1) { values in
			let value = values.first.unsafelyUnwrapped
			switch value {
			case .text(let s):
				return .text(rot13(s))
			default:
				return value
			}
		}

		try! db.execute(sql: "create table t1(a);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameters: "this")
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: ["is"])
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: .string("only"))
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: [.string("a")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: "test")

		let s = try! db.prepare(sql: "select rot13(a) from t1;")
		let results = s.map { try! $0.get(.string, at: 0) }

		XCTAssertEqual(results, ["guvf", "vf", "bayl", "n", "grfg"])

		try! db.removeFunction("rot13", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select rot13(a) from t1;"))
	}

	func testCustomAggregateFunction() {
		let db = try! Database()

		class IntegerSumAggregateFunction: SQLAggregateFunction {
			func step(_ values: [DatabaseValue]) throws {
				let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum += i
				default:
					throw DatabaseError("Only integer values supported")
				}
			}

			func final() throws -> DatabaseValue {
				defer {
					sum = 0
				}
				return .integer(sum)
			}

			var sum: Int64 = 0
		}

		try! db.addAggregateFunction("integer_sum", arity: 1, IntegerSumAggregateFunction())

		try! db.execute(sql: "create table t1(a);")

		for i in  0..<10 {
			try! db.execute(sql: "insert into t1(a) values (?);", parameters: [.int(i)])
		}

		let s = try! db.prepare(sql: "select integer_sum(a) from t1;").step()!.get(.int64, at: 0)
		XCTAssertEqual(s, 45)

		let ss = try! db.prepare(sql: "select integer_sum(a) from t1;").step()!.get(.int64, at: 0)
		XCTAssertEqual(ss, 45)

		try! db.removeFunction("integer_sum", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select integer_sum(a) from t1;"))
	}

	func testCustomAggregateWindowFunction() {
		let db = try! Database()

		class IntegerSumAggregateWindowFunction: SQLAggregateWindowFunction {
			func step(_ values: [DatabaseValue]) throws {
				let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum += i
				default:
					throw DatabaseError("Only integer values supported")
				}
			}

			func inverse(_ values: [DatabaseValue]) throws {
				let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum -= i
				default:
					throw DatabaseError("Only integer values supported")
				}
			}

			func value() throws -> DatabaseValue {
				return .integer(sum)
			}

			func final() throws -> DatabaseValue {
				defer {
					sum = 0
				}
				return .integer(sum)
			}

			var sum: Int64 = 0
		}

		try! db.addAggregateWindowFunction("integer_sum", arity: 1, IntegerSumAggregateWindowFunction())

		try! db.execute(sql: "create table t1(a);")

		for i in  0..<10 {
			try! db.execute(sql: "insert into t1(a) values (?);", parameters: .int(i))
		}

		let s = try! db.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;")
		let results = s.map { try! $0.get(.int64, at: 0) }

		XCTAssertEqual(results, [1, 3, 6, 9, 12, 15, 18, 21, 24, 17])

		try! db.removeFunction("integer_sum", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;"))	}

	func testCustomTokenizer() {

		/// A word tokenizer using CFStringTokenizer.
		class WordTokenizer: FTS5Tokenizer {
			var tokenizer: CFStringTokenizer!
			var text: CFString!

			required init(arguments: [String]) {
			}

			func setText(_ text: String, reason: Database.FTS5TokenizationReason) {
				self.text = text as CFString
				tokenizer = CFStringTokenizerCreate(kCFAllocatorDefault, self.text, CFRangeMake(0, CFStringGetLength(self.text)), kCFStringTokenizerUnitWord, nil)
			}

			func advance() -> Bool {
				let nextToken = CFStringTokenizerAdvanceToNextToken(tokenizer)
				guard nextToken != CFStringTokenizerTokenType(rawValue: 0) else {
					return false
				}
				return true
			}

			func currentToken() -> String? {
				let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
				guard tokenRange.location != kCFNotFound /*|| tokenRange.length != 0*/ else {
					return nil
				}
				return CFStringCreateWithSubstring(kCFAllocatorDefault, text, tokenRange) as String
			}

			func copyCurrentToken(to buffer: UnsafeMutablePointer<UInt8>, capacity: Int) throws -> Int {
				let tokenRange = CFStringTokenizerGetCurrentTokenRange(tokenizer)
				var bytesConverted = 0
				let charsConverted = CFStringGetBytes(text, tokenRange, CFStringBuiltInEncodings.UTF8.rawValue, 0, false, buffer, capacity, &bytesConverted)
				guard charsConverted > 0 else {
					throw DatabaseError("Insufficient buffer size")
				}
				return bytesConverted
			}
		}

		let db = try! Database()

		try! db.addTokenizer("word", type: WordTokenizer.self)

		try! db.execute(sql: "create virtual table t1 USING fts5(a, tokenize = 'word');")

		try! db.prepare(sql: "insert into t1(a) values (?);").bind("quick brown", toParameter: 1).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind("fox", toParameter: 1).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind("jumps over").execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(["the lazy dog"]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind("🦊🐶", toParameter: 1).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(.string(""), toParameter: 1).execute()
		try! db.prepare(sql: "insert into t1(a) values (NULL);").execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind("quick", toParameter: 1).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(.string("brown fox")).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind("jumps over the").execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind("lazy dog").execute()

		let s = try! db.prepare(sql: "select count(*) from t1 where t1 match 'o*';")
		let count = try! s.step()!.get(.int, at: 0)
		XCTAssertEqual(count, 2)

		let statement = try! db.prepare(sql: "select * from t1 where t1 match 'o*';")
		try! statement.results { row in
			let s = try row.get(.string, at: 0)
			XCTAssert(s.starts(with: "jumps over"))
		}
	}

	func testDatabaseBindings() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		for i in 0..<10 {
			try! db.prepare(sql: "insert into t1(a, b) values (?, ?);").bind([.int(i), .null]).execute()
		}

		let statement = try! db.prepare(sql: "select * from t1 where a = ?")
		try! statement.bind(integer: Int64(5), toParameter: 1)

		try! statement.results { row in
			let x = try row.get(.int, at: 0)
			let y = try row.optional(.int, named: "b")

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}

	func testDatabaseBindings2() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		let int = 5
		let optionalInt: Int? = nil
		try! db.prepare(sql: "insert into t1(a, b) values (?, ?);").bind(.value(.integer(Int64(int))), .int(optionalInt)).execute()

		let statement = try! db.prepare(sql: "select * from t1 where a = ?")
		try! statement.bind(integer: Int64(5), toParameter: 1)

		try! statement.results { row in
			let x = try row.get(.int, at: 0)
			let y = try row.optional(.int, named: "b")

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}

	func testDatabaseNamedBindings() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		for i in 0..<10 {
			try! db.execute(sql: "insert into t1(a, b) values (:b, :a);", parameters: [":a": .null, ":b": .int(i)])
		}

		let statement = try! db.prepare(sql: "select * from t1 where a = :a")
		try! statement.bind(integer: Int64(5), toParameter: ":a")

		try! statement.results { row in
			let x = try row.get(.int, at: 0)
			let y = try row.optional(.int, at: 1)

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}

	func testStatementColumns() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b, c);")

		for i in 0..<3 {
			try! db.prepare(sql: "insert into t1(a, b, c) values (?,?,?);").bind([.int(i), .int(i * 3), .int(i * 5)]).execute()
		}

		let statement = try! db.prepare(sql: "select * from t1")
		let cols = try! statement.columns([0,2], .int)
		XCTAssertEqual(cols[0], [0,1,2])
		XCTAssertEqual(cols[1], [0,5,10])
	}

	func testUUIDExtension() {
		let db = try! Database()
		let statement = try! db.prepare(sql: "select uuid();")
		let s: String = try! statement.step()!.text(at: 0)
		let u = UUID(uuidString: s)
		XCTAssertEqual(u?.uuidString.lowercased(), s.lowercased())
	}

	func testCArrayExtension() {
		let db = try! Database()

		try! db.execute(sql: "create table animals(kind);")

		try! db.prepare(sql: "insert into animals(kind) values ('dog');").execute()
		try! db.prepare(sql: "insert into animals(kind) values ('cat');").execute()
		try! db.prepare(sql: "insert into animals(kind) values ('bird');").execute()
		try! db.prepare(sql: "insert into animals(kind) values ('hedgehog');").execute()

		let pets = [ "dog", "dragon", "hedgehog" ]
		let statement = try! db.prepare(sql: "SELECT * FROM animals WHERE kind IN carray(?1);")
		try! statement.bind(.carray(pets), toParameter: 1)

		let results: [String] = statement.map({try! $0.text(at: 0)})

		XCTAssertEqual([ "dog", "hedgehog" ], results)
	}

	func testVirtualTable() {
		final class NaturalNumbersModule: EponymousVirtualTableModule {
			final class Cursor: VirtualTableCursor {
				var _rowid: Int64 = 0

				func column(_ index: Int32) -> DatabaseValue {
					.integer(_rowid)
				}

				func next() {
					_rowid += 1
				}

				func rowid() -> Int64 {
					_rowid
				}

				func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
					_rowid = 1
				}

				var eof: Bool {
					_rowid > 2147483647
				}
			}

			required init(database: Database, arguments: [String]) {
			}

			var declaration: String {
				"CREATE TABLE x(value)"
			}

			var options: Database.VirtualTableModuleOptions {
				[.innocuous]
			}

			func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
				.ok
			}

			func openCursor() -> VirtualTableCursor {
				Cursor()
			}
		}

		let db = try! Database()

		try! db.addModule("natural_numbers", type: NaturalNumbersModule.self)
		let statement = try! db.prepare(sql: "SELECT value FROM natural_numbers LIMIT 5;")

		let results: [Int] = try! statement.column(0, .int)
		XCTAssertEqual(results, [1,2,3,4,5])
	}

	func testVirtualTable2() {
		/// A port of the `generate_series` sqlite3 module
		/// - seealso: https://www.sqlite.org/src/file/ext/misc/series.c
		final class SeriesModule: EponymousVirtualTableModule {
			static let valueColumn: Int32 = 0
			static let startColumn: Int32 = 1
			static let stopColumn: Int32 = 2
			static let stepColumn: Int32 = 3

			struct QueryPlan: OptionSet {
				let rawValue: Int32
				static let start = QueryPlan(rawValue: 1 << 0)
				static let stop = QueryPlan(rawValue: 1 << 1)
				static let step = QueryPlan(rawValue: 1 << 2)
				static let isDescending = QueryPlan(rawValue: 1 << 3)
			}

			final class Cursor: VirtualTableCursor {
				let module: SeriesModule
				var _rowid: Int64 = 0
				var _value: Int64 = 0
				var _min: Int64 = 0
				var _max: Int64 = 0
				var _step: Int64 = 0
				var _isDescending = false

				init(_ module: SeriesModule) {
					self.module = module
				}

				func column(_ index: Int32) -> DatabaseValue {
					switch index {
					case SeriesModule.valueColumn:		return .integer(_value)
					case SeriesModule.startColumn:		return .integer(_min)
					case SeriesModule.stopColumn:		return .integer(_max)
					case SeriesModule.stepColumn:		return .integer(_step)
					default:							return nil
					}
				}

				func next() {
					if _isDescending {
						_value -= _step
					} else {
						_value += _step
					}
					_rowid += 1
				}

				func rowid() -> Int64 {
					return _rowid
				}

				func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
					_rowid = 1
					_min = 0
					_max = 0xffffffff
					_step = 1

					let queryPlan = QueryPlan(rawValue: indexNumber)
					var argumentNumber = 0
					if queryPlan.contains(.start) {
						if case let .integer(i) = arguments[argumentNumber] {
							_min = i
						}
						argumentNumber += 1
					}

					if queryPlan.contains(.stop) {
						if case let .integer(i) = arguments[argumentNumber] {
							_max = i
						}
						argumentNumber += 1
					}

					if queryPlan.contains(.step) {
						if case let .integer(i) = arguments[argumentNumber] {
							_step = max(i, 1)
						}
						argumentNumber += 1
					}

					if arguments.contains(where: { return $0 == .null ? true : false }) {
						_min = 1
						_max = 0
					}

					_isDescending = queryPlan.contains(.isDescending)
					_value = _isDescending ? _max : _min
					if _isDescending && _step > 0 {
						_value -= (_max - _min) % _step
					}
				}

				var eof: Bool {
					if _isDescending {
						return _value < _min
					} else {
						return _value > _max
					}
				}
			}

			required init(database: Database, arguments: [String]) {
			}

			var declaration: String {
				"CREATE TABLE x(value,start hidden,stop hidden,step hidden)"
			}

			var options: Database.VirtualTableModuleOptions {
				return [.innocuous]
			}

			func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
				// Inputs
				let constraintCount = Int(indexInfo.nConstraint)
				let constraints = UnsafeBufferPointer<sqlite3_index_constraint>(start: indexInfo.aConstraint, count: constraintCount)

				let orderByCount = Int(indexInfo.nOrderBy)
				let orderBy = UnsafeBufferPointer<sqlite3_index_orderby>(start: indexInfo.aOrderBy, count: orderByCount)

				// Outputs
				let constraintUsage = UnsafeMutableBufferPointer<sqlite3_index_constraint_usage>(start: indexInfo.aConstraintUsage, count: constraintCount)

				var queryPlan: QueryPlan = []

				var filterArgumentCount: Int32 = 1
				for i in 0 ..< constraintCount {
					let constraint = constraints[i]

					switch constraint.iColumn {
					case SeriesModule.startColumn:
						guard constraint.usable != 0 else {
							break
						}
						guard constraint.op == SQLITE_INDEX_CONSTRAINT_EQ else {
							return .constraint
						}
						queryPlan.insert(.start)
						constraintUsage[i].argvIndex = filterArgumentCount
						filterArgumentCount += 1

					case SeriesModule.stopColumn:
						guard constraint.usable != 0 else {
							break
						}
						guard constraint.op == SQLITE_INDEX_CONSTRAINT_EQ else {
							return .constraint
						}
						queryPlan.insert(.stop)
						constraintUsage[i].argvIndex = filterArgumentCount
						filterArgumentCount += 1

					case SeriesModule.stepColumn:
						guard constraint.usable != 0 else {
							break
						}
						guard constraint.op == SQLITE_INDEX_CONSTRAINT_EQ else {
							return .constraint
						}
						queryPlan.insert(.step)
						constraintUsage[i].argvIndex = filterArgumentCount
						filterArgumentCount += 1

					default:
						break
					}
				}

				if queryPlan.contains(.start) && queryPlan.contains(.stop) {
					indexInfo.estimatedCost = 2  - (queryPlan.contains(.step) ? 1 : 0)
					indexInfo.estimatedRows = 1000
					if orderByCount == 1 {
						if orderBy[0].desc == 1 {
							queryPlan.insert(.isDescending)
						}
						indexInfo.orderByConsumed = 1
					}
				} else {
					indexInfo.estimatedRows = 2147483647
				}

				indexInfo.idxNum = queryPlan.rawValue

				return .ok
			}

			func openCursor() -> VirtualTableCursor {
				return Cursor(self)
			}
		}

		let db = try! Database()

		try! db.addModule("generate_series", type: SeriesModule.self)

		// Eponymous tables should not be available via `CREATE VIRTUAL TABLE`
		XCTAssertThrowsError(try db.execute(sql: "CREATE VIRTUAL TABLE series USING generate_series;"))

		var statement = try! db.prepare(sql: "SELECT value FROM generate_series LIMIT 5;")
		var results: [Int] = try! statement.column(0, .int)
		XCTAssertEqual(results, [0,1,2,3,4])

		statement = try! db.prepare(sql: "SELECT value FROM generate_series(10) LIMIT 5;")
		results = try! statement.column(0, .int)
		XCTAssertEqual(results, [10,11,12,13,14])

		statement = try! db.prepare(sql: "SELECT value FROM generate_series(10,20,1) ORDER BY value DESC LIMIT 5;")
		results = try! statement.column(0, .int)
		XCTAssertEqual(results, [20,19,18,17,16])

		statement = try! db.prepare(sql: "SELECT value FROM generate_series(11,22,2) LIMIT 5;")
		results = try! statement.column(0, .int)
		XCTAssertEqual(results, [11,13,15,17,19])
	}

	func testVirtualTable3() {
		let db = try! Database()

		try! db.addModule("shuffled_sequence", type: ShuffledSequenceModule.self)

		// Non-eponymous tables should not be available without `CREATE VIRTUAL TABLE`
		XCTAssertThrowsError(try db.execute(sql: "SELECT value FROM shuffled_sequence LIMIT 5;"))

		try! db.execute(sql: "CREATE VIRTUAL TABLE temp.shuffled USING shuffled_sequence(count=5);")
		var statement = try! db.prepare(sql: "SELECT value FROM shuffled;")

		var results: [Int] = statement.map({try! $0.get(.int, at: 0)})
		// Probability of the shuffled sequence being the same as the original is 1/5! = 1/120 = 8% (?) so this isn't a good check
		//		XCTAssertNotEqual(results, [1,2,3,4,5])
		XCTAssertEqual(results.sorted(), [1,2,3,4,5])

		try! db.execute(sql: "CREATE VIRTUAL TABLE temp.shuffled2 USING shuffled_sequence(start=10,count=5);")
		statement = try! db.prepare(sql: "SELECT value FROM shuffled2;")

		results = statement.map({try! $0.get(.int, at: 0)})
//		XCTAssertNotEqual(results, [10,11,12,13,14])
		XCTAssertEqual(results.sorted(), [10,11,12,13,14])
	}

	func testVirtualTable4() {
		let tempURL = temporaryFileURL()
		let db1 = try! Database(url: tempURL)

		try! db1.addModule("shuffled_sequence", type: ShuffledSequenceModule.self)

		try! db1.execute(sql: "CREATE VIRTUAL TABLE shuffled USING shuffled_sequence(count=5);")
		var statement = try! db1.prepare(sql: "SELECT value FROM shuffled;")

		var results: [Int] = statement.map({try! $0.get(.int, at: 0)})
		XCTAssertEqual(results.sorted(), [1,2,3,4,5])

		let db2 = try! Database(url: tempURL)

		try! db2.addModule("shuffled_sequence", type: ShuffledSequenceModule.self)

		statement = try! db2.prepare(sql: "SELECT value FROM shuffled;")

		results = statement.map({try! $0.get(.int, at: 0)})
		XCTAssertEqual(results.sorted(), [1,2,3,4,5])
	}

#if SQLITE_ENABLE_PREUPDATE_HOOK

	func testPreUpdateHook() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a,b);")

		try! db.execute(sql: "insert into t1(a,b) values (?,?);", parameters: ["alpha","start"])
		try! db.execute(sql: "insert into t1(a,b) values (?,?);", parameters: ["beta",123])
		try! db.execute(sql: "insert into t1(a,b) values (?,?);", parameters: ["gamma","gamma value"])
		try! db.execute(sql: "insert into t1(a,b) values (?,?);", parameters: ["epsilon","epsilon value"])
		try! db.execute(sql: "insert into t1(a,b) values (?,?);", parameters: ["phi",123.456])

		db.setPreUpdateHook { change in
			guard case .insert(_) = change.changeType else {
				XCTFail("pre-update hook incorrect changeType")
				return
			}

			let value = try! change.newValue(at: 0)
			guard case .text(let s) = value, s == "skeleton" else {
				XCTFail("pre-update hook insert fail")
				return
			}

			do {
				XCTAssertThrowsError(try change.oldValue(at: 0))
			} catch {}
		}
		try! db.execute(sql: "insert into t1(a) values (?);", parameters: ["skeleton"])

		db.setPreUpdateHook { change in
			guard case .update(_, _) = change.changeType else {
				XCTFail("pre-update hook incorrect changeType")
				return
			}

			var value = try! change.newValue(at: 1)
			guard case .integer(let i) = value, i == 999 else {
				XCTFail("pre-update hook update fail")
				return
			}

			value = try! change.oldValue(at: 1)
			guard case .integer(let i2) = value, i2 == 123 else {
				XCTFail("pre-update hook update fail")
				return
			}
		}
		try! db.execute(sql: "update t1 set b=999 where a='beta';")

		db.setPreUpdateHook { change in
			guard case .delete(_) = change.changeType else {
				XCTFail("pre-update hook incorrect changeType")
				return
			}

			let value = try! change.oldValue(at: 1)
			guard case .integer(let i) = value, i == 999 else {
				XCTFail("pre-update hook update fail")
				return
			}

			do {
				XCTAssertThrowsError(try change.newValue(at: 0))
			} catch {}
		}
		try! db.execute(sql: "delete from t1 where a='beta';")


	}

#endif

#if SQLITE_ENABLE_PREUPDATE_HOOK && SQLITE_ENABLE_SESSION

	func testSession() {
		let db1 = try! Database()
		let db2 = try! Database()

		let sql = "CREATE TABLE birds(id integer primary key, kind);"

		try! db1.execute(sql: sql)
		try! db2.execute(sql: sql)

		let session = try! Session(database: db1, schema: "main")
		try! session.attach("birds")

		try! db1.prepare(sql: "insert into birds(kind) values ('robin');").execute()
		try! db1.prepare(sql: "insert into birds(kind) values ('cardinal');").execute()
		try! db1.prepare(sql: "insert into birds(kind) values ('finch');").execute()
		try! db1.prepare(sql: "insert into birds(kind) values ('sparrow');").execute()
		try! db1.prepare(sql: "insert into birds(kind) values ('utahraptor');").execute()

		XCTAssertFalse(session.isEmpty)

		let changes = try! session.changeset()

		try! db2.apply(changes) { conflict in
				.abort
		}

		let birds: [String] = try! db2.prepare(sql: "select kind from birds;").column(0, .string)
		XCTAssert(birds == ["robin","cardinal","finch","sparrow","utahraptor"])

		let inverse = try! changes.inverted()

		try! db2.apply(inverse) { conflict in
				.abort
		}

		let count: Int = try! db2.prepare(sql: "select count(*) from birds;").step()!.get(.int, at: 0)
		XCTAssert(count == 0)
	}

#endif

#if canImport(Combine)

	func testRowPublisher() {
		let db = try! Database()
		XCTAssertNoThrow(try db.execute(sql: "create table t1(v1 text default (uuid()));"))
		let rowCount = 10
		for _ in 0 ..< rowCount {
			XCTAssertNoThrow(try db.execute(sql: "insert into t1 default values;"))
		}

		let statement = try! db.prepare(sql: "select v1 from t1;")
		let uuids = try! statement.column(0, .uuidWithString)

		struct UUIDHolder: RowMapping {
			let u: UUID
			init(row: Row) throws {
				u = UUID(uuidString: try row.text(at: 0))!
			}
		}

		let expectation = self.expectation(description: "uuids")

		let publisher = db.rowPublisher(sql: "select v1 from t1;")

		var column: [UUIDHolder] = []
		var error: Error?

		var cancellables = Set<AnyCancellable>()

		publisher
			.mapRows(type: UUIDHolder.self)
			.collect()
			.sink { completion in
				switch completion {
				case .finished:
					break
				case .failure(let encounteredError):
					error = encounteredError
				}

				expectation.fulfill()
			} receiveValue: { value in
				column = value
			}
			.store(in: &cancellables)


		waitForExpectations(timeout: 5)

		XCTAssertNil(error)
		XCTAssertEqual(column.count, rowCount)
		for (i,value) in column.enumerated() {
			XCTAssertEqual(value.u, uuids[i])
		}
	}

#endif

	/// Creates a URL for a temporary file on disk. Registers a teardown block to
	/// delete a file at that URL (if one exists) during test teardown.
	func temporaryFileURL() -> URL {
		// Create a URL for an unique file in the system's temporary directory.
		let directory = NSTemporaryDirectory()
		let filename = UUID().uuidString
		let fileURL = URL(fileURLWithPath: directory).appendingPathComponent(filename)

		// Add a teardown block to delete any file at `fileURL`.
		addTeardownBlock {
			do {
				let fileManager = FileManager.default
				// Check that the file exists before trying to delete it.
				if fileManager.fileExists(atPath: fileURL.path) {
					// Perform the deletion.
					try fileManager.removeItem(at: fileURL)
					// Verify that the file no longer exists after the deletion.
					XCTAssertFalse(fileManager.fileExists(atPath: fileURL.path))
				}
			} catch {
				// Treat any errors during file deletion as a test failure.
				XCTFail("Error while deleting temporary file: \(error)")
			}
		}

		// Return the temporary file URL for use in a test method.
		return fileURL
	}
}

/// A virtual table module implementing a shuffled integer sequence
///
/// Usage:
/// ```
/// CREATE VIRTUAL TABLE temp.shuffled USING shuffled_sequence(count=10);
/// SELECT * from shuffled;
/// ```
///
/// Required parameter: count
/// Optional parameter: start
final class ShuffledSequenceModule: VirtualTableModule {
	final class Cursor: VirtualTableCursor {
		let table: ShuffledSequenceModule
		var _rowid: Int64 = 0

		init(_ table: ShuffledSequenceModule) {
			self.table = table
		}

		func column(_ index: Int32) -> DatabaseValue {
			return .integer(Int64(table.values[Int(_rowid - 1)]))
		}

		func next() {
			_rowid += 1
		}

		func rowid() -> Int64 {
			_rowid
		}

		func filter(_ arguments: [DatabaseValue], indexNumber: Int32, indexName: String?) {
			_rowid = 1
		}

		var eof: Bool {
			_rowid > table.values.count
		}
	}

	let values: [Int]

	required init(database: Database, arguments: [String], create: Bool) throws {
		var count = 0
		var start = 1

		for argument in arguments.suffix(from: 3) {
			let scanner = Scanner(string: argument)
			scanner.charactersToBeSkipped = .whitespaces
			guard let token = scanner.scanUpToString("=") else {
				continue
			}
			if token == "count" {
				guard scanner.scanString("=") != nil else {
					throw SQLiteError("Missing value for count")
				}
				guard scanner.scanInt(&count), count > 0 else {
					throw SQLiteError("Invalid value for count")
				}
			}
			else if token == "start" {
				guard scanner.scanString("=") != nil else {
					throw SQLiteError("Missing value for start")
				}
				guard scanner.scanInt(&start) else {
					throw SQLiteError("Invalid value for start")
				}
			}
		}

		guard count > 0 else {
			throw SQLiteError("Invalid value for count")
		}

		values = (start ..< start + count).shuffled()
	}

	var declaration: String {
		"CREATE TABLE x(value)"
	}

	var options: Database.VirtualTableModuleOptions {
		[.innocuous]
	}

	func bestIndex(_ indexInfo: inout sqlite3_index_info) -> VirtualTableModuleBestIndexResult {
		.ok
	}

	func openCursor() -> VirtualTableCursor {
		Cursor(self)
	}
}
