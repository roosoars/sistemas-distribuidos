
import SwiftUI

struct ContentView: View {

    @StateObject private var viewModel = VoteViewModel()

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 24) {

                    headerView
                        .padding(.top, 20)

                    statusView
                        .padding(.horizontal)

                    Divider()

                    VStack(spacing: 20) {
                        if !viewModel.showResults {
                            candidatesView
                                .transition(.opacity)
                        } else if let results = viewModel.voteResult {
                            resultsView(results: results)
                                .transition(.opacity)
                        }
                    }
                    .padding(.horizontal)
                    .animation(.easeInOut(duration: 0.3), value: viewModel.showResults)

                    Spacer(minLength: 20)

                    actionButtons
                        .padding(.horizontal)
                        .padding(.bottom, 20)
                }
            }
            .background(Color(uiColor: .systemGroupedBackground))
            .navigationTitle("VoteNow")
            .navigationBarTitleDisplayMode(.inline)
            .alert("Status", isPresented: $viewModel.showAlert) {
                Button("OK", role: .cancel) { }
            } message: {
                Text(viewModel.alertMessage)
            }
            .onAppear {
                viewModel.startMonitoring()
            }
        }
    }
    private var headerView: some View {
        VStack(spacing: 12) {
            Image(systemName: "archivebox.fill")
                .font(.system(size: 50))
                .foregroundColor(.blue)

            Text("Sistema de Votação")
                .font(.title2)
                .bold()
                .foregroundColor(.primary)

            Text("Conectado via Directory Proxy")
                .font(.footnote)
                .foregroundColor(.secondary)
        }
    }
    private var statusView: some View {
        HStack {
            Text("Status da Conexão")
                .font(.subheadline)
                .foregroundColor(.primary)

            Spacer()

            HStack(spacing: 6) {
                Circle()
                    .fill(viewModel.connectionStatus.contains("Conectado") ? Color.green : Color.red)
                    .frame(width: 8, height: 8)

                Text(viewModel.connectionStatus)
                    .font(.caption)
                    .bold()
                    .foregroundColor(.secondary)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(Color(uiColor: .secondarySystemBackground))
            .cornerRadius(12)
        }
        .padding()
        .background(Color(uiColor: .systemBackground))
        .cornerRadius(12)
        .shadow(color: .black.opacity(0.05), radius: 2, x: 0, y: 1)
    }
    private var candidatesView: some View {
        VStack(spacing: 12) {
            Text("Escolha seu candidato")
                .font(.headline)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.bottom, 4)

            ForEach(viewModel.candidates) { candidate in
                Button(action: {
                    viewModel.selectCandidate(candidate.id)
                }) {
                    HStack {
                        Image(systemName: candidate.icon)
                            .font(.title2)
                            .foregroundColor(candidate.color)
                            .frame(width: 40)

                        Text(candidate.name)
                            .font(.body)
                            .fontWeight(.medium)
                            .foregroundColor(.primary)

                        Spacer()

                        if viewModel.selectedCandidate == candidate.id {
                            Image(systemName: "checkmark.circle.fill")
                                .foregroundColor(.blue)
                                .font(.title3)
                        } else {
                            Image(systemName: "circle")
                                .foregroundColor(.gray.opacity(0.3))
                                .font(.title3)
                        }
                    }
                    .padding()
                    .background(Color(uiColor: .systemBackground))
                    .cornerRadius(12)
                    .overlay(
                        RoundedRectangle(cornerRadius: 12)
                            .stroke(viewModel.selectedCandidate == candidate.id ? Color.blue : Color.clear, lineWidth: 2)
                    )
                    .shadow(color: .black.opacity(0.03), radius: 2, x: 0, y: 1)
                }
                .buttonStyle(PlainButtonStyle())
            }
        }
    }
    private func resultsView(results: VoteResult) -> some View {
        VStack(spacing: 16) {
            HStack {
                Text("Resultados Parciais")
                    .font(.headline)
                Spacer()
                Text("Total: \(results.totalVotes ?? 0)")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
            }

            ForEach(viewModel.candidates) { candidate in
                let votes = results.counts[candidate.id] ?? 0
                let total = results.totalVotes ?? 1
                let percentage = total > 0 ? Double(votes) / Double(total) * 100 : 0

                VStack(alignment: .leading, spacing: 6) {
                    HStack {
                        Text(candidate.name)
                            .font(.subheadline)
                            .fontWeight(.medium)

                        if candidate.id == results.winner {
                           Text("GANHADOR!")
                            .font(.caption)
                                .foregroundColor(.primary)
                        }

                        Spacer()

                        Text("\(Int(percentage))%")
                            .font(.subheadline)
                            .bold()
                    }
                    GeometryReader { geometry in
                        ZStack(alignment: .leading) {
                            Rectangle()
                                .fill(Color(.systemGray5))
                                .frame(height: 8)
                                .cornerRadius(4)

                            Rectangle()
                                .fill(candidate.color)
                                .frame(width: max(geometry.size.width * (percentage / 100), 0), height: 8)
                                .cornerRadius(4)
                        }
                    }
                    .frame(height: 8)

                    Text("\(votes) votos")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                .padding()
                .background(Color(uiColor: .systemBackground))
                .cornerRadius(12)
                .shadow(color: .black.opacity(0.03), radius: 2, x: 0, y: 1)
            }
        }
    }
    private var actionButtons: some View {
        VStack(spacing: 16) {
            if !viewModel.showResults {
                Button(action: { Task { await viewModel.submitVote() } }) {
                    HStack {
                        if viewModel.isLoading {
                            ProgressView()
                                .progressViewStyle(CircularProgressViewStyle(tint: .white))
                                .padding(.trailing, 8)
                        }
                        Text("Confirmar Voto")
                            .font(.headline)
                    }
                    .frame(maxWidth: .infinity)
                    .frame(height: 50)
                    .background(viewModel.selectedCandidate == nil ? Color.gray : Color.blue)
                    .foregroundColor(.white)
                    .cornerRadius(12)
                }
                .disabled(viewModel.selectedCandidate == nil || viewModel.isLoading)
            }

            Button(action: { Task { await viewModel.loadResults() } }) {
                Text(viewModel.showResults ? "Atualizar Resultados" : "Ver Resultados")
                    .font(.headline)
                    .frame(maxWidth: .infinity)
                    .frame(height: 50)
                    .background(Color(uiColor: .systemBackground))
                    .foregroundColor(.blue)
                    .overlay(
                        RoundedRectangle(cornerRadius: 12)
                            .stroke(Color.blue, lineWidth: 1)
                    )
                    .cornerRadius(12)
            }
            .disabled(viewModel.isLoading)

            if viewModel.showResults {
                Button(action: {
                    viewModel.backToVoting()
                }) {
                    Text("Voltar para Votação")
                        .font(.subheadline)
                        .fontWeight(.medium)
                        .foregroundColor(.secondary)
                        .padding()
                }
            }
        }
    }
}

#Preview {
    ContentView()
}
