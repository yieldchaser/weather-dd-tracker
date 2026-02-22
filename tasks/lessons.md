# Self-Improvement Loop & Lessons

*This document tracks architectural patterns, pitfalls, and guidelines learned during the development of the Weather DD Tracker.*

## Lessons Learned
- *(Initial)* **Plan Upfront**: Always plan non-trivial tasks in `tasks/todo.md` before execution. Ensure all work maps to checkable steps.
- *(Initial)* **Elegance and Simplicity**: Prefer simple, elegant solutions that hit the core problem without over-engineering. For weather/energy models, speed and reliability trump complex, unmaintainable features.
- *(Initial)* **Verification Before Done**: Prove correctness of all fetched data and signals via tests or local validation before considering a task complete.
- *(Initial)* **Subagent Strategy**: Utilize subagents to isolate complex parallel analysis and maintain a pristine main context.
- *(Initial)* **Autonomous Bug Fixing**: When encountering an error, rely on logs and self-correction instead of requiring user intervention.
