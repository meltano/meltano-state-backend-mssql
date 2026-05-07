# `meltano-state-backend-mssql`

This is a [Meltano] extension that provides a Microsoft SQL Server (MSSQL) [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with git+https://github.com/meltano/meltano-state-backend-mssql.git meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano git+https://github.com/meltano/meltano-state-backend-mssql.git
```

## Configuration

To store state in MSSQL, set the `state_backend.uri` setting to an MSSQL connection URI:

```
mssql+pymssql://<user>:<password>@<host>:<port>/<database>
```

State will be stored in tables that Meltano will create automatically:

- `state` — Stores the actual state data
- `state_lock` — Used for distributed locking during state writes

To authenticate to MSSQL, you'll need to provide:

```yaml
state_backend:
  uri: mssql+pymssql://my_user:my_password@localhost:1433/my_database
  mssql:
    schema: meltano  # Optional: defaults to meltano
    table: state  # Optional: defaults to state
```

Alternatively, you can provide credentials via individual settings:

```yaml
state_backend:
  uri: mssql+pymssql://localhost/my_database
  mssql:
    host: localhost
    port: 1433          # Defaults to 1433 if not specified
    user: my_user
    password: my_password
    database: my_database
    schema: meltano     # Optional: defaults to meltano
    table: state        # Optional: defaults to state
```

#### Connection Parameters

- **host**: MSSQL server hostname (default: localhost)
- **port**: MSSQL server port (default: 1433)
- **user**: The username for authentication
- **password**: The password for authentication
- **database**: The database where state will be stored
- **schema**: The schema where state tables will be created (default: `meltano`)
- **table**: The table name for state storage (default: `state`)

#### Security Considerations

When storing credentials:

- Use environment variables for sensitive values in production
- Ensure the user has `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, and `SELECT` privileges on the schema

Example using environment variables:

```bash
export MELTANO_STATE_BACKEND_MSSQL_PASSWORD='my_secure_password'
meltano config set meltano state_backend.uri 'mssql+pymssql://my_user@localhost:1433/my_database'
meltano config set meltano state_backend.mssql.schema 'my_schema'
```

## Development

### Setup

```bash
uv sync
```

### Run tests

Run all tests, type checks, linting, and coverage:

```bash
uvx --with tox-uv tox run-parallel
```

### Bump the version

Using the [GitHub CLI][gh]:

```bash
gh release create v<new-version>
```

[gh]: https://cli.github.com/
[meltano]: https://meltano.com
[pipx]: https://github.com/pypa/pipx
[state-backend]: https://docs.meltano.com/concepts/state_backends
[uv]: https://docs.astral.sh/uv
