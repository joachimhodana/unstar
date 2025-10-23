# unstar

Expand `SELECT *` to explicit columns in dbt projects.

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
unstar --select model_a model_b --dry-run

# Write changes in place (with backup)
unstar --select models/staging --write --backup

# Use custom project directory
unstar --project-dir /path/to/dbt/project --select model_a --dry-run

# Use custom manifest path
unstar --manifest custom/path/manifest.json --select model_a --dry-run

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

- `--select SELECTION` - Models to process (like dbt select syntax)
- `--project-dir PATH` - Project root directory (default: .)
- `--manifest PATH` - Custom path to dbt manifest.json
- `--write` - Edit files in place
- `--dry-run` - Show changes without applying (default)
- `--output DIR` - Write updated files to directory
- `--reporter {human,diff,github}` - Output format for dry-run (default: human)
- `--backup` - Create .bak files when writing in place
- `--verbose` - Show detailed output

### Reporter Formats

- `human` - Human-readable summary (default)
- `diff` - Unified diff format
- `github` - GitHub Actions annotations format

## How It Works

1. **Model Selection**: Choose models using `--select` (like dbt syntax)
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

## CI/CD Integration

Use `unstar` in your CI pipeline to enforce explicit column selection:

### GitHub Actions Example

Lint SQL models for SELECT * usage in CI/CD pipeline.

```yaml
name: Lint SQL Models
on: [push, pull_request]

jobs:
  lint-sql:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          
      - name: Install dbt and unstar
        run: |
          pip install dbt-core
          pip install "git+https://github.com/joachimhodana/unstar.git#egg=unstar[dbt]"
          
      - name: Compile dbt models
        run: dbt compile
        
      - name: Check for SELECT * usage
        run: unstar --dry-run --reporter github
        # Exit code 1 if changes needed, 0 if all models are clean
```

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: unstar-check
        name: Check for SELECT * usage
        entry: unstar --dry-run
        language: system
        pass_filenames: false
        always_run: true
```

### Exit Codes

- `0`: No changes needed (all models use explicit columns)
- `1`: Changes detected (some models have `SELECT *` that can be expanded)
- `2`: Error occurred (invalid arguments, missing files, etc.)

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