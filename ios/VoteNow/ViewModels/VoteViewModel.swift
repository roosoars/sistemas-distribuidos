
import Foundation
import SwiftUI
import Combine

@MainActor
class VoteViewModel: ObservableObject {

    private let socketManager: SocketManager
    @Published var selectedCandidate: String?
    @Published var showResults: Bool = false
    @Published var voteResult: VoteResult?
    @Published var connectionStatus: String = "Desconectado"
    @Published var isLoading: Bool = false
    @Published var showAlert: Bool = false
    @Published var alertMessage: String = ""
    let candidates = [
        Candidate(id: "candidate1", name: "Maria Silva", color: .blue, icon: "person.circle.fill"),
        Candidate(id: "candidate2", name: "João Santos", color: .green, icon: "person.circle.fill"),
        Candidate(id: "candidate3", name: "Ana Costa", color: .orange, icon: "person.circle.fill")
    ]

    private var cancellables = Set<AnyCancellable>()

    init() {
        self.socketManager = SocketManager(roomId: "default")

        setupBindings()
    }
    private func setupBindings() {
        // Repassa para a View o estado vindo do SocketManager.
        socketManager.$connectionStatus
            .assign(to: \.connectionStatus, on: self)
            .store(in: &cancellables)

        socketManager.$isLoading
            .assign(to: \.isLoading, on: self)
            .store(in: &cancellables)

        socketManager.$voteResult
            .assign(to: \.voteResult, on: self)
            .store(in: &cancellables)
    }
    func startMonitoring() {
        socketManager.startHealthCheck()
    }
    func selectCandidate(_ id: String) {
        withAnimation(.easeInOut(duration: 0.2)) {
            self.selectedCandidate = id
        }
    }
    func submitVote() async {
        guard let candidateId = selectedCandidate else { return }

        do {
            _ = try await socketManager.sendVote(option: candidateId)
            let generator = UINotificationFeedbackGenerator()
            generator.notificationOccurred(.success)

            alertMessage = "Voto computado com sucesso!"
            showAlert = true

            withAnimation {
                self.showResults = true
            }

        } catch {
            let generator = UINotificationFeedbackGenerator()
            generator.notificationOccurred(.error)

            alertMessage = error.localizedDescription
            showAlert = true
        }
    }
    func loadResults() async {
        do {
            _ = try await socketManager.getResults()
            withAnimation {
                self.showResults = true
            }
        } catch {
            alertMessage = error.localizedDescription
            showAlert = true
        }
    }
    func backToVoting() {
        withAnimation {
            self.showResults = false
        }
    }
}
