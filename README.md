# YTX Server

YTX Server is a Rust-based backend server that provides WebSocket communication, database management, and Vault integration for secure credential handling. It is designed for high concurrency and security, leveraging async Rust and modern libraries.

## Features

- **WebSocket Server:** Handles real-time client connections and messaging.
- **Database Hub:** Manages PostgreSQL connections and SQL generation.
- **Vault Integration:** Securely retrieves and renews secrets from HashiCorp Vault.
- **Configurable:** Uses environment variables for flexible deployment.
- **Async & Scalable:** Built with Tokio for high performance.

## Getting Started

### Prerequisites

- Rust (latest stable recommended)
- PostgreSQL database
- HashiCorp Vault (for secret management, optional)
- Initial Database Setup (MUST RUN FIRST)
  - Before starting the server, you **must** initialize the authentication and main databases, as well as create required PostgreSQL roles and permissions.
  - For detailed instructions, please refer to [ytx-initdb](https://github.com/YtxErp/ytx-initdb).

### Setup

1. **Clone the repository:**

    ```sh
    git clone https://github.com/YtxErp/ytx-server.git
    cd ytx-server
    ```

2. **Configure environment variables:**

    - Copy `env_template.text` to `.env` and fill in your values.
    - Example variables:

        ```env
        BASE_POSTGRES_URL=postgres://localhost:5432
        VAULT_ADDR=<http://127.0.0.1:8200>
        VAULT_TOKEN=your-vault-token
        LISTEN_ADDR=127.0.0.1:8080
        AUTH_DB=ytx_auth
        AUTH_READWRITE_ROLE=ytx_auth_readwrite
        AUTH_READWRITE_PASSWORD=your-password
        ```

3. **Build the project:**

    ```sh
    cargo build --release
    ```

4. **Run the server:**

    ```sh
    cargo run --release
    ```

## Usage

- The server listens for WebSocket connections on the address specified by `LISTEN_ADDR`.
- It connects to PostgreSQL using credentials from Vault or environment variables.
- WebSocket sessions are managed in [`src/websocket/session.rs`](src/websocket/session.rs).
- Database operations are handled via [`src/dbhub/`](src/dbhub/).

## Development

- Code is organized by feature (config, dbhub, message, vault, websocket).
- Use `.env` for local development configuration.
- See [`src/main.rs`](src/main.rs) for application startup logic.

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file.

## Support Me

If YTX has been helpful to you, I’d be truly grateful for your support. Your encouragement helps me keep improving and creating!

Also may the force be with you!

[<img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" width="160" height="40">](https://buymeacoffee.com/ytx.cash)
