import Foundation
import Combine
import UIKit

@MainActor
final class SocketManager: ObservableObject {
    @Published var connectionStatus: String = "Desconectado"
    @Published var voteResult: VoteResult?
    @Published var errorMessage: String?
    @Published var isLoading: Bool = false
    @Published var currentLeader: String = "NATS"

    private let natsService: NATSService
    private let deviceId: String = UIDevice.current.identifierForVendor?.uuidString ?? UUID().uuidString
    private var isHealthCheckRunning = false

    init(roomId: String = "default") {
        let info = Bundle.main.infoDictionary
        let natsHost = (info?["NATS_HOST"] as? String) ?? "127.0.0.1"
        let natsPort = UInt16(info?["NATS_PORT"] as? Int ?? 4222)
        let natsUser = (info?["NATS_USER"] as? String) ?? "app_ios"
        let natsPassword = (info?["NATS_PASSWORD"] as? String) ?? "app_ios_dev"

        self.natsService = NATSService(
            host: natsHost,
            port: natsPort,
            user: natsUser,
            password: natsPassword,
            roomId: roomId
        )
    }

    func startHealthCheck() {
        guard !isHealthCheckRunning else { return }
        isHealthCheckRunning = true

        Task {
            // Atualiza o status de conexão do app em intervalos fixos.
            while true {
                do {
                    let start = Date()
                    try await natsService.ping(timeout: 2.0)
                    let duration = Date().timeIntervalSince(start) * 1000
                    await MainActor.run {
                        self.connectionStatus = "Conectado NATS (\(Int(duration))ms)"
                        self.currentLeader = "NATS"
                        self.isLoading = false
                    }
                } catch {
                    await MainActor.run {
                        self.connectionStatus = "Desconectado"
                        self.currentLeader = "NATS indisponível"
                    }
                }

                try? await Task.sleep(nanoseconds: 5 * 1_000_000_000)
            }
        }
    }

    func sendVote(option: String) async throws -> VoteResult {
        connectionStatus = "Enviando voto no NATS..."
        isLoading = true
        defer { isLoading = false }

        let result = try await natsService.sendVote(option: option, voterId: deviceId)
        voteResult = result
        connectionStatus = "Voto registrado no NATS!"
        return result
    }

    func getResults() async throws -> VoteResult {
        isLoading = true
        defer { isLoading = false }

        let result = try await natsService.getResults()
        voteResult = result
        return result
    }
}

enum VoteError: LocalizedError {
    case connectionFailed(String)
    case sendFailed(String)
    case receiveFailed(String)
    case voteRejected(String)
    case invalidResponse
    case timeout

    var errorDescription: String? {
        switch self {
        case .connectionFailed(let message):
            return "Falha ao conectar: \(message)"
        case .sendFailed(let message):
            return "Falha ao enviar: \(message)"
        case .receiveFailed(let message):
            return "Falha ao receber: \(message)"
        case .voteRejected(let message):
            return "Voto rejeitado: \(message)"
        case .invalidResponse:
            return "Resposta invalida do servidor"
        case .timeout:
            return "Tempo limite excedido."
        }
    }
}
