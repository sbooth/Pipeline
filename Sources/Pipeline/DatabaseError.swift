//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

/// An error supplying a message and description.
public struct DatabaseError: Error {
	/// A brief message describing the error.
	public let message: String

	/// A more detailed description of the error's cause.
	public let details: String?
}

extension DatabaseError {
	/// Creates an error with the given message.
	///
	/// - parameter message: A brief message describing the error.
	init(_ message: String) {
		self.message = message
		self.details = nil
	}
}

extension DatabaseError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message): \(details)"
		} else {
			return message
		}
	}
}

extension DatabaseError: LocalizedError {
	public var errorDescription: String? {
		return message
	}

	public var failureReason: String? {
		return details
	}
}
