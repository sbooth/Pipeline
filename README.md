# Pipeline

Pipeline provides Combine publishers for SQLite result sets, allowing you to write elegant and powerful data processing code:

```swift
struct UUIDHolder {
	let u: UUID
}

extension UUIDHolder: RowMapping
	init(row: Row) throws {
		u = try row.value(forColumn: 0, .uuidWithString)
	}
}

let database = try Database()

let sevenDaysAgo = Date() - 7 * 24 * 60 * 60

let publisher = database.rowPublisher(sql: "select uuid from table_one where date >= ?;") {
	try $0.bind(.timeIntervalSinceReferenceDate(sevenDaysAgo), toParameter: 1)
}

publisher
	.mapRows(type: UUIDHolder.self)
```
