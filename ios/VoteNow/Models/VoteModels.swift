
import Foundation
import SwiftUI
struct Candidate: Identifiable {
    let id: String
    let name: String
    let color: Color
    let icon: String
}
struct VoteResult: Codable {
    let counts: [String: Int]
    var totalVotes: Int?

    init(counts: [String: Int], totalVotes: Int? = nil) {
        self.counts = counts
        self.totalVotes = totalVotes ?? counts.values.reduce(0, +)
    }
    var winner: String? {
        counts.max { $0.value < $1.value }?.key
    }
}
