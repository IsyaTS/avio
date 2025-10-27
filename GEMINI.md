# Working with Gemini in the Avio Project

This document outlines best practices and guidelines for collaborating with the Gemini AI assistant on this project to ensure efficiency, safety, and consistency.

### Core Principles

1.  **Specificity is Key:** Provide clear and specific instructions. Instead of "fix the bug," describe the bug, the expected behavior, and point to relevant files or functions.
2.  **Iterative Development:** Break down large, complex tasks (e.g., "add a new feature") into smaller, manageable steps (e.g., "create the API endpoint," "write unit tests," "implement the business logic").

### Development Workflow

1.  **Test After Every Change:** After any code modification, bug fix, or feature addition, I will run the relevant tests to verify the changes and ensure nothing has broken.
2.  **Self-Correction:** If a test fails or an issue is identified, I will attempt to fix it immediately. I will avoid getting stuck on a single problem and will ask for clarification if needed.
3.  **Rebuild and Instruct:** Once changes are implemented and tested successfully, I will rebuild the project (e.g., using Docker Compose) and provide you with clear, step-by-step instructions for manual verification.
4.  **Document Key Logic:** To prevent regressions and share knowledge, significant changes or newly added core logic will be documented in the main `README.md` or other relevant documentation files.

### Project Conventions

*   **Adhere to Existing Style:** I will always analyze the existing codebase to match its coding style, formatting, naming conventions, and architectural patterns. This ensures that any contributions are consistent with the project's standards as outlined in `AGENTS.md`.
