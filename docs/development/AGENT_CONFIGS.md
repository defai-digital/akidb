# Local Agent Configuration

Agent and MCP client configuration is intentionally local-only. Files such as
`.gemini/settings.json`, `.ax-grok/settings.json`,
`.claude/settings.local.json`, and `opencode.json` can contain absolute paths,
user-specific tool locations, and local credentials.

Keep those files out of git. If a shared agent workflow becomes part of the
project, document the workflow here with placeholders instead of committing a
machine-specific config file.

Example MCP server command shape:

```json
{
  "mcpServers": {
    "local-agent": {
      "command": "node",
      "args": ["/path/to/ax", "mcp", "server"]
    }
  }
}
```
