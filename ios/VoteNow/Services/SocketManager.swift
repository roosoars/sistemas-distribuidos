
import Foundation
import Network
import Combine
import UIKit

@MainActor
final class SocketManager: ObservableObject {
    @Published var connectionStatus: String = "Desconectado"
    @Published var voteResult: VoteResult?
    @Published var errorMessage: String?
    @Published var isLoading: Bool = false
    @Published var currentLeader: String = "Nenhum"

    private let directoryHost: String
    private let directoryPort: UInt16
    private let roomId: String
    private let deviceId: String = UIDevice.current.identifierForVendor?.uuidString ?? UUID().uuidString

    private var isHealthCheckRunning = false
    private let maxRetries = 3
    private let retryDelay: UInt64 = 2_000_000_000
    private var cachedLeaderHost: String?
    private var cachedLeaderPort: Int?

    init(directoryHost: String = "127.0.0.1", directoryPort: UInt16 = 9000, roomId: String = "default") {
        self.directoryHost = directoryHost
        self.directoryPort = directoryPort
        self.roomId = roomId
    }
    func startHealthCheck() {
        guard !isHealthCheckRunning else { return }
        isHealthCheckRunning = true

        Task {
            while true {
                do {
                    let start = Date()
                    try await ping(timeout: 2.0)
                    let duration = Date().timeIntervalSince(start) * 1000
                    await refreshLeaderInfo()

                    await MainActor.run {
                        self.connectionStatus = "Conectado (\(Int(duration))ms)"
                        self.isLoading = false
                    }
                } catch {
                    await MainActor.run {
                        self.connectionStatus = "Desconectado"
                        self.currentLeader = "Nenhum"
                        self.cachedLeaderHost = nil
                        self.cachedLeaderPort = nil
                    }
                }

                try? await Task.sleep(nanoseconds: 5 * 1_000_000_000)
            }
        }
    }
    private func refreshLeaderInfo() async {
        do {
            let leaderInfo = try await queryLeader()
            if let host = leaderInfo["address"] as? String,
               let port = leaderInfo["port"] as? Int,
               let serverId = leaderInfo["server_id"] as? String {
                await MainActor.run {
                    self.cachedLeaderHost = host
                    self.cachedLeaderPort = port
                    self.currentLeader = serverId.uppercased()
                }
            }
        } catch {
            await MainActor.run {
                self.currentLeader = "Buscando..."
            }
        }
    }
    private func ping(timeout: TimeInterval) async throws {
        let payload: [String: Any] = [:]

        let response = try await sendTCPMessage(
            to: directoryHost,
            port: directoryPort,
            type: "COMMAND",
            name: "Ping",
            payload: payload,
            timeout: timeout
        )

        guard response["type"] as? String == "RESPONSE",
              response["name"] as? String == "Pong" else {
            throw VoteError.invalidResponse
        }
    }
    func sendVote(option: String) async throws -> VoteResult {
        connectionStatus = "Enviando voto..."
        isLoading = true
        defer { isLoading = false }

        let voteId = UUID().uuidString
        var lastError: Error = VoteError.primaryNotFound
        for attempt in 1...maxRetries {
            do {
                let leaderInfo = try await queryLeader()
                guard let host = leaderInfo["address"] as? String,
                      let port = leaderInfo["port"] as? Int else {
                    throw VoteError.primaryNotFound
                }
                cachedLeaderHost = host
                cachedLeaderPort = port
                if let serverId = leaderInfo["server_id"] as? String {
                    currentLeader = serverId.uppercased()
                }

                connectionStatus = "Votando em \(currentLeader)..."
                let votePayload: [String: Any] = [
                    "voteId": voteId,
                    "voterId": deviceId,
                    "option": option
                ]

                let response = try await sendTCPMessage(
                    to: host,
                    port: UInt16(port),
                    type: "COMMAND",
                    name: "VoteCommand",
                    payload: votePayload
                )

                guard response["type"] as? String == "RESPONSE" else {
                    throw VoteError.invalidResponse
                }
                if response["name"] as? String == "VoteRejected",
                   let payload = response["payload"] as? [String: Any],
                   let error = payload["error"] as? String,
                   error == "Not leader" {
                    connectionStatus = "Lider mudou, buscando novo..."
                    try? await Task.sleep(nanoseconds: retryDelay)
                    continue
                }

                guard response["name"] as? String == "VoteAccepted",
                      let payload = response["payload"] as? [String: Any] else {
                    if let payload = response["payload"] as? [String: Any],
                       let error = payload["error"] as? String {
                        throw VoteError.voteRejected(error)
                    }
                    throw VoteError.invalidResponse
                }

                connectionStatus = "Voto registrado!"

                if let counts = payload["currentCounts"] as? [String: Int] {
                    let result = VoteResult(counts: counts)
                    voteResult = result
                    return result
                }

                throw VoteError.invalidResponse

            } catch {
                lastError = error
                let isConnectionError = error is VoteError &&
                    (error as! VoteError).isConnectionError

                if isConnectionError && attempt < maxRetries {
                    connectionStatus = "Servidor caiu, buscando novo lider... (tentativa \(attempt)/\(maxRetries))"
                    try? await Task.sleep(nanoseconds: retryDelay)
                    continue
                }

                throw error
            }
        }

        throw lastError
    }
    func getResults() async throws -> VoteResult {
        isLoading = true
        defer { isLoading = false }

        var lastError: Error = VoteError.primaryNotFound

        for attempt in 1...maxRetries {
            do {
                let leaderInfo = try await queryLeader()
                guard let host = leaderInfo["address"] as? String,
                      let port = leaderInfo["port"] as? Int else {
                    throw VoteError.primaryNotFound
                }

                let response = try await sendTCPMessage(
                    to: host,
                    port: UInt16(port),
                    type: "QUERY",
                    name: "GetResultsQuery",
                    payload: [:]
                )

                guard response["type"] as? String == "RESPONSE",
                      response["name"] as? String == "ResultsData",
                      let payload = response["payload"] as? [String: Any],
                      let counts = payload["counts"] as? [String: Int],
                      let totalVotes = payload["totalVotes"] as? Int else {
                    throw VoteError.invalidResponse
                }

                let result = VoteResult(counts: counts, totalVotes: totalVotes)
                voteResult = result
                return result

            } catch {
                lastError = error
                let isConnectionError = error is VoteError &&
                    (error as! VoteError).isConnectionError

                if isConnectionError && attempt < maxRetries {
                    try? await Task.sleep(nanoseconds: retryDelay)
                    continue
                }

                throw error
            }
        }

        throw lastError
    }
    private func queryLeader() async throws -> [String: Any] {
        let httpPort = directoryPort == 9000 ? 9080 : directoryPort + 80
        let urlString = "http://\(directoryHost):\(httpPort)/api/leader"

        guard let url = URL(string: urlString) else {
            throw VoteError.primaryNotFound
        }

        var request = URLRequest(url: url)
        request.timeoutInterval = 5.0

        let (data, response) = try await URLSession.shared.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse,
              httpResponse.statusCode == 200 else {
            throw VoteError.primaryNotFound
        }

        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let found = json["found"] as? Bool,
              found,
              let leader = json["leader"] as? [String: Any] else {
            throw VoteError.primaryNotFound
        }

        return leader
    }
    private func queryService(name: String) async throws -> [String: Any] {
        let payload: [String: Any] = ["serviceName": name]
        let response = try await sendTCPMessage(
            to: directoryHost,
            port: directoryPort,
            type: "QUERY",
            name: "QueryService",
            payload: payload
        )

        guard response["type"] as? String == "RESPONSE",
              response["name"] as? String == "ServiceFound",
              let serviceInfo = response["payload"] as? [String: Any] else {
            throw VoteError.primaryNotFound
        }
        return serviceInfo
    }
    private func sendTCPMessage(to host: String, port: UInt16, type: String, name: String, payload: [String: Any], timeout: TimeInterval = 5.0) async throws -> [String: Any] {
        let messageEnvelope: [String: Any] = [
            "id": UUID().uuidString,
            "type": type,
            "name": name,
            "sender": "iOSCLIENT-\(deviceId)",
            "timestamp": Date().timeIntervalSince1970,
            "payload": payload
        ]

        let jsonData = try JSONSerialization.data(withJSONObject: messageEnvelope)

        let endpoint = NWEndpoint.hostPort(
            host: NWEndpoint.Host(host),
            port: NWEndpoint.Port(rawValue: port)!
        )

        let connection = NWConnection(to: endpoint, using: .tcp)
        let timeoutNanoseconds: UInt64 = UInt64(timeout * 1_000_000_000)

        do {
            let responseData = try await withTimeout(nanoseconds: timeoutNanoseconds) {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    connection.stateUpdateHandler = { state in
                        switch state {
                        case .ready:
                            continuation.resume()
                        case .failed(let error):
                            continuation.resume(throwing: VoteError.connectionFailed(error.localizedDescription))
                        case .waiting:
                            break
                        default:
                            break
                        }
                    }
                    connection.start(queue: .global())
                }
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    connection.send(content: jsonData, completion: .contentProcessed { error in
                        if let error = error {
                            continuation.resume(throwing: VoteError.sendFailed(error.localizedDescription))
                        } else {
                            continuation.resume()
                        }
                    })
                }
                let data = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Data, Error>) in
                    connection.receive(minimumIncompleteLength: 1, maximumLength: 8192) { data, _, isComplete, error in
                        if let error = error {
                            continuation.resume(throwing: VoteError.receiveFailed(error.localizedDescription))
                        } else if let data = data {
                            continuation.resume(returning: data)
                        } else {
                            continuation.resume(throwing: VoteError.invalidResponse)
                        }
                    }
                }

                connection.cancel()
                return data
            }

            guard let response = try JSONSerialization.jsonObject(with: responseData) as? [String: Any] else {
                throw VoteError.invalidResponse
            }

            return response

        } catch {
            connection.cancel()
            throw error
        }
    }
    private func withTimeout<T: Sendable>(nanoseconds: UInt64, operation: @escaping @Sendable () async throws -> T) async throws -> T {
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
enum VoteError: LocalizedError {
    case primaryNotFound
    case invalidServerAddress
    case connectionFailed(String)
    case sendFailed(String)
    case receiveFailed(String)
    case voteRejected(String)
    case invalidResponse
    case timeout
    case leaderChanged

    var errorDescription: String? {
        switch self {
        case .primaryNotFound:
            return "Servidor Primary não encontrado."
        case .invalidServerAddress:
            return "Endereço do servidor inválido"
        case .connectionFailed(let msg):
            return "Falha ao conectar: \(msg)"
        case .sendFailed(let msg):
            return "Falha ao enviar: \(msg)"
        case .receiveFailed(let msg):
            return "Falha ao receber: \(msg)"
        case .voteRejected(let msg):
            return "Voto rejeitado: \(msg)"
        case .invalidResponse:
            return "Resposta inválida do servidor"
        case .timeout:
            return "Tempo limite excedido."
        case .leaderChanged:
            return "Líder mudou, tentando novamente..."
        }
    }
    var isConnectionError: Bool {
        switch self {
        case .connectionFailed, .sendFailed, .receiveFailed, .timeout, .primaryNotFound:
            return true
        default:
            return false
        }
    }
}
