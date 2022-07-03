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

		let count = try! db.prepare(sql: "select count(*) from t1;").nextRow()!.value(forColumn: 0, .int)
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

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.integer(1)])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("feisty")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.real(2.5)])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.blob(Data(count: 8))])

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.urlString(URL(fileURLWithPath: "/tmp"))])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.uuidString(UUID())])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.timeIntervalSinceReferenceDate(Date())])

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.null()])
	}

	func testIteration() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a);")

		let rowCount = 10

		for i in 0..<rowCount {
			try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: .int(i)).execute()
		}

		let s = try! db.prepare(sql: "select * from t1;")
		var count = 0

		for row in s {
			for _ in row {
				XCTAssert(try! row.value(forColumn: 0, .int) == count)
			}
			count += 1
		}

		XCTAssertEqual(count, rowCount)
	}

	func testIteration2() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a,b,c,d);")

		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(parameterValues: .int(1),.int(2),.int(3),.int(4)).execute()
		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(parameterValues: .text("a"),.text("b"),.text("c"),.text("d")).execute()
		try! db.prepare(sql: "insert into t1(a,b,c,d) values (?,?,?,?);").bind(parameterValues: .text("a"),.int(2),.text("c"),.int(4)).execute()

		do {
			let s = try! db.prepare(sql: "select * from t1 limit 1 offset 0;")
			let r = try! s.nextRow()!
			let v = [DatabaseValue](r)

			XCTAssertEqual(v, [DatabaseValue(1),DatabaseValue(2),DatabaseValue(3),DatabaseValue(4)])
		}

		do {
			let s = try! db.prepare(sql: "select * from t1 limit 1 offset 1;")
			let r = try! s.nextRow()!
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

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.json(a)])

		let b = try! db.prepare(sql: "select * from t1 limit 1;").nextRow()!.value(forColumn: 0, .json(as: TestStruct.self))

		XCTAssertEqual(a.a, b.a)
		XCTAssertEqual(a.c, b.c)
		XCTAssertEqual(a.d, b.d)
	}

	func testCustomCollation() {
		let db = try! Database()

		try! db.addCollation("reversed", { (a, b) -> ComparisonResult in
			return b.compare(a)
		})

		try! db.execute(sql: "create table t1(a text);")

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("a")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("c")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("z")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("e")])

		var str = ""
		let s = try! db.prepare(sql: "select * from t1 order by a collate reversed;")
		try! s.results { row in
			let c = try row.value(forColumn: 0, .string)
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

		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("this")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("is")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("only")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("a")])
		try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.text("test")])

		let s = try! db.prepare(sql: "select rot13(a) from t1;")
		let results = s.map { try! $0.value(forColumn: 0, .string) }

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
					throw DatabaseError(message: "Only integer values supported")
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
			try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.int(i)])
		}

		let s = try! db.prepare(sql: "select integer_sum(a) from t1;").nextRow()!.value(forColumn: 0, .int64)
		XCTAssertEqual(s, 45)

		let ss = try! db.prepare(sql: "select integer_sum(a) from t1;").nextRow()!.value(forColumn: 0, .int64)
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
					throw DatabaseError(message: "Only integer values supported")
				}
			}

			func inverse(_ values: [DatabaseValue]) throws {
				let value = values.first.unsafelyUnwrapped
				switch value {
				case .integer(let i):
					sum -= i
				default:
					throw DatabaseError(message: "Only integer values supported")
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
			try! db.execute(sql: "insert into t1(a) values (?);", parameterValues: [.int(i)])
		}

		let s = try! db.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;")
		let results = s.map { try! $0.value(forColumn: 0, .int64) }

		XCTAssertEqual(results, [1, 3, 6, 9, 12, 15, 18, 21, 24, 17])

		try! db.removeFunction("integer_sum", arity: 1)
		XCTAssertThrowsError(try db.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;"))	}

	func testCustomTokenizer() {

		/// A word tokenizer using CFStringTokenizer
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
					throw DatabaseError(message: "Insufficient buffer size")
				}
				return bytesConverted
			}
		}

		let db = try! Database()

		try! db.addTokenizer("word", type: WordTokenizer.self)

		try! db.execute(sql: "create virtual table t1 USING fts5(a, tokenize = 'word');")

		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("quick brown")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("fox")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("jumps over")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("the lazy dog")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("🦊🐶")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (NULL);").execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("quick")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("brown fox")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("jumps over the")]).execute()
		try! db.prepare(sql: "insert into t1(a) values (?);").bind(parameterValues: [.text("lazy dog")]).execute()

		let s = try! db.prepare(sql: "select count(*) from t1 where t1 match 'o*';")
		let count = try! s.nextRow()!.value(forColumn: 0, .int)
		XCTAssertEqual(count, 2)

		let statement = try! db.prepare(sql: "select * from t1 where t1 match 'o*';")
		try! statement.results { row in
			let s = try row.value(forColumn: 0, .string)
			XCTAssert(s.starts(with: "jumps over"))
		}
	}

	func testDatabaseBindings() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		for i in 0..<10 {
			try! db.prepare(sql: "insert into t1(a, b) values (?, ?);").bind(parameterValues: [.int(i), .null()]).execute()
		}

		let statement = try! db.prepare(sql: "select * from t1 where a = ?")
		try! statement.bind(value: 5, toParameter: 1)

		try! statement.results { row in
			let x = try row.value(forColumn: 0, .int)
			let y = try row.valueOrNil(forColumn: "b", .int)

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}

	func testDatabaseNamedBindings() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b);")

		for i in 0..<10 {
			try! db.execute(sql: "insert into t1(a, b) values (:b, :a);", parameters: [":a": .null(), ":b": .int(i)])
		}

		let statement = try! db.prepare(sql: "select * from t1 where a = :a")
		try! statement.bind(value: 5, toParameter: ":a")

		try! statement.results { row in
			let x = try row.value(forColumn: 0, .int)
			let y = try row.valueOrNil(forColumn: 1, .int)

			XCTAssertEqual(x, 5)
			XCTAssertEqual(y, nil)
		}
	}

	func testStatementColumns() {
		let db = try! Database()

		try! db.execute(sql: "create table t1(a, b, c);")

		for i in 0..<3 {
			try! db.prepare(sql: "insert into t1(a, b, c) values (?,?,?);").bind(parameterValues: [.int(i), .int(i * 3), .int(i * 5)]).execute()
		}

		let statement = try! db.prepare(sql: "select * from t1")
		let cols = try! statement.values(forColumns: [0,2], .int)
		XCTAssertEqual(cols[0], [0,1,2])
		XCTAssertEqual(cols[1], [0,5,10])
	}

#if canImport(Combine)
	func testRowPublisher() {
		let db = try! Database()
		XCTAssertNoThrow(try db.execute(sql: "create table t1(v1 text default (uuid()));"))
		let rowCount = 10
		for _ in 0 ..< rowCount {
			XCTAssertNoThrow(try db.execute(sql: "insert into t1 default values;"))
		}

		let statement = try! db.prepare(sql: "select v1 from t1;")
		let uuids = try! statement.values(forColumn: 0, .uuidWithString)

		struct UUIDHolder: RowMapping {
			let u: UUID
			init(row: Row) throws {
				u = UUID(uuidString: try row.text(forColumn: 0))!
			}
		}

		let expectation = self.expectation(description: "uuids")

		let publisher = db.rowPublisher(sql: "select v1 from t1;")

		var values: [UUIDHolder] = []
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
				values = value
			}
			.store(in: &cancellables)


		waitForExpectations(timeout: 5)

		XCTAssertNil(error)
		XCTAssertEqual(values.count, rowCount)
		for (i,value) in values.enumerated() {
			XCTAssertEqual(value.u, uuids[i])
		}
	}
#endif
}
