# Repository Guidelines

## Project Structure & Module Organization
- `cmd/server/` 是服务入口，负责启动请求 API 与管理后台服务，并包含启动相关测试。
- `internal/gateway/` 处理 OpenAI 兼容转发、流式响应、重试、上游请求与诊断时间线。
- `internal/admin/` 提供管理 API；静态管理界面位于 `internal/admin/static/admin.html`。
- `internal/store/` 封装 bbolt 持久化，保存密钥、全局配置、统计计数与诊断信息。
- 根目录包含 `Dockerfile`、`go.mod`、`go.sum`、`README.md`；本地运行产生的 `data.db` 不应提交。

## Build, Test, and Development Commands
- `go test ./...`：运行全部 Go 单元测试，是提交前的默认检查。
- `go test ./internal/gateway -run TestName`：只运行某个包或指定测试，便于快速迭代。
- `ADMIN_TOKEN=dev-token PORT=8080 ADMIN_PORT=8081 DATA_PATH=./data.db go run ./cmd/server`：本地启动请求端口与管理端口。
- `docker buildx build --platform linux/amd64 -t pyroflux/api-fucker:latest .`：构建 amd64 容器镜像。

## Coding Style & Naming Conventions
- Go 代码必须使用 `gofmt` 格式化；提交前检查改动过的 `.go` 文件。
- 包名保持短小、小写，例如 `gateway`、`admin`、`store`。
- 导出类型、函数和字段使用清晰的 PascalCase；未导出标识符使用 camelCase。
- 管理后台目前是单文件静态页面，相关修改集中在 `internal/admin/static/admin.html`，不要无故引入前端构建链。

## Testing Guidelines
- 测试使用 Go 标准库 `testing`，文件命名为 `*_test.go`，与被测代码放在同一包目录。
- 修改路由、重试策略、密钥选择、IP 规则、持久化或管理接口时，必须补充或更新测试。
- 测试应使用临时数据库路径或隔离数据，避免依赖根目录的 `data.db`。
- PR 前运行 `go test ./...`，并在说明中写明结果；若有跳过项，需解释原因。

## Commit & Pull Request Guidelines
- 当前历史使用简短祈使句，例如 `add bulk key actions`；继续保持小写、动词开头、聚焦单一变更。
- PR 描述应说明用户可见行为、影响的包、配置变化和测试结果。
- 管理界面变更请附截图或操作说明；关联 issue 时在描述中注明。
- 不要提交 NVIDIA API Key、代理凭据、生成的数据库、日志或本地 IDE 配置。

## Security & Configuration Tips
- 部署时必须设置强 `ADMIN_TOKEN`，并限制管理端口访问范围。
- `DATA_PATH` 包含持久化密钥与配置，应放在受保护的卷或目录中。
- 日志、测试夹具和诊断输出中都应脱敏上游密钥与代理认证信息。
