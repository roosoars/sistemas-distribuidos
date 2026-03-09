import Foundation
import Network

final class NATSService {
    private let host: String
    private let port: UInt16
    private let user: String
    private let password: String
    private let roomId: String

    init(host: String, port: UInt16, user: String, password: String, roomId: String) {
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.roomId = roomId
    }

    func ping(timeout: TimeInterval = 2.0) async throws {
        _ = try await getResults(timeout: timeout)
    }

    func sendVote(option: String, voterId: String, timeout: TimeInterval = 5.0) async throws -> VoteResult {
        let voteId = UUID().uuidString
        let traceId = UUID().uuidString
        let payload: [String: Any] = [
            "schema": "vote.submit.v1",
            "vote_id": voteId,
            "room_id": roomId,
            "candidate_id": option,
            "voter_id": voterId,
            "client_ts": ISO8601DateFormatter().string(from: Date()),
            "trace_id": traceId
        ]

        var response = try await requestJSON(subject: "svc.vote.submit.\(roomId)", payload: payload, timeout: timeout)
        var status = (response["status"] as? String) ?? "error"
        let deadline = Date().addingTimeInterval(6.0)

        // Consulta o status enquanto o backend conclui o processamento.
        while status == "pending", Date() < deadline {
            try await Task.sleep(nanoseconds: 300_000_000)
            response = try await requestJSON(
                subject: "svc.vote.status.\(roomId)",
                payload: [
                    "schema": "vote.status.v1",
                    "room_id": roomId,
                    "vote_id": voteId,
                    "trace_id": traceId
                ],
                timeout: timeout
            )
            status = (response["status"] as? String) ?? "error"
        }

        if status == "success" || status == "duplicate" {
            let counts = response["counts"] as? [String: Int] ?? [:]
            let total = response["total_votes"] as? Int ?? counts.values.reduce(0, +)
            return VoteResult(counts: counts, totalVotes: total)
        }

        if let error = response["error"] as? String {
            throw VoteError.voteRejected(error)
        }
        throw VoteError.invalidResponse
    }

    func getResults(timeout: TimeInterval = 5.0) async throws -> VoteResult {
        let response = try await requestJSON(
            subject: "svc.vote.results.\(roomId)",
            payload: [
                "schema": "vote.results.v1",
                "room_id": roomId,
                "trace_id": UUID().uuidString
            ],
            timeout: timeout
        )

        guard (response["status"] as? String) == "success" else {
            let message = (response["error"] as? String) ?? "results_failed"
            throw VoteError.voteRejected(message)
        }

        let counts = response["counts"] as? [String: Int] ?? [:]
        let total = response["total_votes"] as? Int ?? counts.values.reduce(0, +)
        return VoteResult(counts: counts, totalVotes: total)
    }

    private func requestJSON(subject: String, payload: [String: Any], timeout: TimeInterval) async throws -> [String: Any] {
        let data = try JSONSerialization.data(withJSONObject: payload)
        let client = NATSRequestClient(
            host: host,
            port: port,
            user: user,
            password: password
        )
        let responseData = try await client.request(subject: subject, payload: data, timeout: timeout)
        guard let json = try JSONSerialization.jsonObject(with: responseData) as? [String: Any] else {
            throw VoteError.invalidResponse
        }
        return json
    }
}

private enum NATSFrame {
    case info
    case ping
    case pong
    case ok
    case err(String)
    case msg(subject: String, sid: String, payload: Data)
    case unknown
}

private final class NATSRequestClient {
    private let host: String
    private let port: UInt16
    private let user: String
    private let password: String
    private var buffer = Data()
    private var connection: NWConnection?

    init(host: String, port: UInt16, user: String, password: String) {
        self.host = host
        self.port = port
        self.user = user
        self.password = password
    }

    func request(subject: String, payload: Data, timeout: TimeInterval) async throws -> Data {
        let endpoint = NWEndpoint.hostPort(
            host: .init(host),
            port: .init(rawValue: port)!
        )
        let connection = NWConnection(to: endpoint, using: .tcp)
        self.connection = connection

        do {
            try await waitForReady(connection: connection, timeout: timeout)
            try await sendRaw("CONNECT {\"lang\":\"swift\",\"version\":\"1.0.0\",\"protocol\":1,\"verbose\":false,\"pedantic\":false,\"user\":\"\(user)\",\"pass\":\"\(password)\"}\r\n")
            try await sendRaw("PING\r\n")

            // Usa inbox temporária para receber a resposta da requisição.
            let inbox = "_INBOX.\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
            let sid = String(Int.random(in: 1000...9999))
            try await sendRaw("SUB \(inbox) \(sid)\r\n")

            var pub = Data("PUB \(subject) \(inbox) \(payload.count)\r\n".utf8)
            pub.append(payload)
            pub.append(Data("\r\n".utf8))
            try await sendData(pub)

            let response = try await readReply(sid: sid, timeout: timeout)
            try await sendRaw("UNSUB \(sid)\r\n")
            connection.cancel()
            return response
        } catch {
            connection.cancel()
            throw error
        }
    }

    private func waitForReady(connection: NWConnection, timeout: TimeInterval) async throws {
        let timeoutNs = UInt64(timeout * 1_000_000_000)
        try await withTimeout(nanoseconds: timeoutNs) {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                var resumed = false
                connection.stateUpdateHandler = { state in
                    guard !resumed else { return }
                    switch state {
                    case .ready:
                        resumed = true
                        continuation.resume()
                    case .failed(let error):
                        resumed = true
                        continuation.resume(throwing: VoteError.connectionFailed(error.localizedDescription))
                    case .cancelled:
                        resumed = true
                        continuation.resume(throwing: VoteError.connectionFailed("cancelled"))
                    default:
                        break
                    }
                }
                connection.start(queue: .global())
            }
        }
    }

    private func sendRaw(_ command: String) async throws {
        try await sendData(Data(command.utf8))
    }

    private func sendData(_ data: Data) async throws {
        guard let connection else { throw VoteError.connectionFailed("not connected") }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.send(content: data, completion: .contentProcessed { error in
                if let error {
                    continuation.resume(throwing: VoteError.sendFailed(error.localizedDescription))
                } else {
                    continuation.resume()
                }
            })
        }
    }

    private func readReply(sid: String, timeout: TimeInterval) async throws -> Data {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            while let frame = try parseFrame() {
                switch frame {
                case .info, .ok, .pong, .unknown:
                    continue
                case .ping:
                    try await sendRaw("PONG\r\n")
                case .err(let message):
                    throw VoteError.voteRejected(message)
                case .msg(subject: _, sid: let receivedSid, payload: let payload):
                    if receivedSid == sid {
                        return payload
                    }
                }
            }

            let remaining = deadline.timeIntervalSinceNow
            if remaining <= 0 {
                break
            }
            let chunk = try await receiveChunk(timeout: remaining)
            buffer.append(chunk)
        }
        throw VoteError.timeout
    }

    private func receiveChunk(timeout: TimeInterval) async throws -> Data {
        guard let connection else { throw VoteError.connectionFailed("not connected") }
        let timeoutNs = UInt64(timeout * 1_000_000_000)
        return try await withTimeout(nanoseconds: timeoutNs) {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Data, Error>) in
                connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { data, _, _, error in
                    if let error {
                        continuation.resume(throwing: VoteError.receiveFailed(error.localizedDescription))
                        return
                    }
                    if let data, !data.isEmpty {
                        continuation.resume(returning: data)
                    } else {
                        continuation.resume(throwing: VoteError.receiveFailed("empty response"))
                    }
                }
            }
        }
    }

    private func parseFrame() throws -> NATSFrame? {
        let delimiter = Data([13, 10]) // \r\n
        guard let lineRange = buffer.range(of: delimiter) else {
            return nil
        }
        let lineData = buffer.subdata(in: 0..<lineRange.lowerBound)
        guard let line = String(data: lineData, encoding: .utf8) else {
            throw VoteError.invalidResponse
        }

        if line.hasPrefix("MSG ") {
            let parts = line.split(separator: " ")
            guard parts.count == 4 || parts.count == 5 else {
                throw VoteError.invalidResponse
            }
            let subject = String(parts[1])
            let sid = String(parts[2])
            let bytesIndex = parts.count == 4 ? 3 : 4
            guard let byteCount = Int(parts[bytesIndex]) else {
                throw VoteError.invalidResponse
            }
            let payloadStart = lineRange.upperBound
            let frameLength = payloadStart + byteCount + 2
            guard buffer.count >= frameLength else {
                return nil
            }
            let payload = buffer.subdata(in: payloadStart..<(payloadStart + byteCount))
            buffer.removeSubrange(0..<frameLength)
            return .msg(subject: subject, sid: sid, payload: payload)
        }

        buffer.removeSubrange(0..<lineRange.upperBound)

        if line.hasPrefix("INFO ") {
            return .info
        }
        if line == "PING" {
            return .ping
        }
        if line == "PONG" {
            return .pong
        }
        if line == "+OK" {
            return .ok
        }
        if line.hasPrefix("-ERR") {
            return .err(line)
        }
        return .unknown
    }

    private func withTimeout<T: Sendable>(
        nanoseconds: UInt64,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        return try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask {
                return try await operation()
            }
            group.addTask {
                try await Task.sleep(nanoseconds: nanoseconds)
                throw VoteError.timeout
            }
            let result = try await group.next()!
            group.cancelAll()
            return result
        }
    }
}
