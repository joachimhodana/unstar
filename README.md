# unstar

Expand `SELECT *` to explicit columns using downstream model analysis.

## Installation

```bash
# Basic installation
pip install unstar

# With dbt support
pip install unstar[dbt]
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

### Testing

```bash
# Install with uv
uv sync --dev

# Run tests
uv run pytest tests/ -v

# Test CLI
uv run unstar --help
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
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check .
```

## License

MIT