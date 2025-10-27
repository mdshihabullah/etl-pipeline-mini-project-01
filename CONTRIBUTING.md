# Contributing to Mastodon ETL Pipeline

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/etl-pipeline-mini-project-01.git`
3. Create virtual environment: `python -m venv .venv`
4. Install dependencies: `make install`
5. Copy settings: `cp settings.env.example settings.env`
6. Configure settings.env with your values

## Development Workflow

1. Create feature branch: `git checkout -b feature/your-feature-name`
2. Make changes with proper commit messages
3. Run tests: `make verify`
4. Push branch: `git push origin feature/your-feature-name`
5. Create Pull Request

## Commit Message Guidelines

- Use present tense: "Add feature" not "Added feature"
- Use imperative mood: "Fix bug" not "Fixed bug"
- Start with capital letter
- Keep first line under 50 characters
- Add detailed description if needed

## Code Style

- Follow PEP 8 for Python
- Use type hints
- Add docstrings to all public methods
- Keep functions small and focused

## Testing

- Add unit tests for new features
- Run full pipeline tests: `make verify`
- Test with different data sizes