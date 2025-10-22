# unstar

Expand `SELECT *` to explicit columns using downstream model analysis.

## Installation

```bash
# Basic installation
pip install git+https://github.com/joachimhodana/unstar.git

# With dbt support
pip install "git+https://github.com/joachimhodana/unstar.git#egg=unstar[dbt]"

# Development installation
git clone https://github.com/joachimhodana/unstar.git
cd unstar
uv sync --dev
```

## Usage

### dbt Projects

```bash
# Dry run - see what would change
unstar --project-dir . --models model_a model_b --dry-run

# Write changes in place (with backup)
unstar --path models/staging --write --backup

# Output to new directory
unstar --output ./expanded_models
```

### Quick Start

```bash
# Install with dbt support
pip install "git+https://github.com/joachimhodana/unstar.git#egg=unstar[dbt]"

# Navigate to your dbt project
cd your-dbt-project

# Run dbt to generate artifacts
dbt compile

# See what would change (dry run)
unstar --dry-run

# Apply changes with backup
unstar --write --backup
```

### Command Options

- `--adapter {dbt}` - Adapter to use (default: dbt)
- `--project-dir PATH` - Project root directory (default: .)
- `--models MODEL1 MODEL2` - Specific models to process
- `--path PATH` - Directory containing models to process
- `--write` - Edit files in place
- `--dry-run` - Show diff without making changes (default)
- `--output DIR` - Write updated files to directory
- `--backup` - Create .bak files when writing in place
- `--verbose` - Show detailed output

## How It Works

1. **Model Selection**: Choose models by name (`--models`) or directory (`--path`)
2. **Downstream Analysis**: Analyzes downstream models to determine which columns are actually used
3. **Star Expansion**: Replaces `SELECT *` with explicit column lists based on usage
4. **Safe Updates**: Supports dry-run, in-place editing with backups, or output to new directory

## Examples

### Before
```sql
-- models/users.sql
SELECT * FROM {{ ref('raw_users') }}
```

### After (if downstream models use id, name, email)
```sql
-- models/users.sql  
SELECT id, name, email FROM {{ ref('raw_users') }}
```

## Limitations

- Requires dbt artifacts (`target/manifest.json`) for dbt projects
- Complex Jinja expressions in SELECT lists may not be handled
- Ambiguous joins may produce warnings
- No downstream usage found will leave `*` unchanged

## Development

```bash
# Clone and setup
git clone https://github.com/joachimhodana/unstar.git
cd unstar

# Install with uv (recommended)
uv sync --dev

# Or with pip
pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v
# or: pytest tests/ -v

# Run linting
uv run ruff check .
# or: ruff check .

# Test CLI
uv run unstar --help
```

## Credits

This project's structure and approach was inspired by [dbt-artifacts-parser](https://github.com/yu-iskw/dbt-artifacts-parser) by [@yu-iskw](https://github.com/yu-iskw), which provides excellent Python parsing for dbt artifacts.

## License

MIT