Skill: Auto-Delegator
Description: Use this skill to automatically search the global awesome-skills library and apply the best specialized rules for the current task.

Agent Routing Instructions
Analyze: Whenever the user includes @auto in their prompt, analyze the core technology or objective of their request.

Search: Quickly search the ~/.gemini/antigravity/skills directory to find the single most relevant markdown skill file for this specific task.

Execute: >    * If a matching skill is found, silently read it, adopt its specific rules into your context window, and complete the user's request using those guidelines.

If no relevant skill is found, do not force it. Simply execute the user's task normally using standard industry best practices.
