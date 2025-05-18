# Contributing to SuperLake

Thank you for your interest in contributing to SuperLake! We welcome contributions from the community to make this project better.

## How to Contribute

### 1. Reporting Issues
- Search [existing issues](https://github.com/loicmagnien/superlake/issues) before opening a new one.
- Provide a clear and descriptive title and as much relevant information as possible.
- Include steps to reproduce, expected behavior, and screenshots/logs if applicable.

### 2. Submitting Pull Requests
- Fork the repository and create your branch from `main`.
- Write clear, concise commit messages.
- Add tests for new features or bug fixes.
- Ensure all tests pass locally (`pytest`).
- Follow the code style guidelines below.
- Open a pull request and describe your changes.
- Reference related issues in your PR description.

### 3. Code Style
- Follow [PEP8](https://www.python.org/dev/peps/pep-0008/) for Python code.
- Use type hints where possible.
- Run `flake8` or another linter before submitting.
- Write docstrings for all public classes and functions.

### 4. Communication
- Be respectful and constructive in all interactions.
- For major changes, open an issue to discuss your proposal first.

## Development Setup
1. Clone the repository.
2. (Recommended) Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run tests:
   ```bash
   pytest
   ```

## License
By contributing, you agree that your contributions will be licensed under the MIT License. 