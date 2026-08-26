# JobScheduler 分布式任务调度器

JobScheduler 是面向服务与批处理场景的分布式定时任务调度基础设施。
任务发布后进入等待队列，调度器按调度窗口把到期任务派发给持有租约的
执行器，并基于依赖编排、重试退避、心跳驱逐与背压流控保证任务不会
丢失、不会并发重复执行。

## 功能

- 任务发布：`POST /api/publish`，支持任务组、内容、延后时间与依赖
- 等待队列：按到期时间排序，批次派发时窗口快照一致
- 依赖编排：任务可声明前置依赖，依赖未就绪时保持待派状态
- 执行器租约：`POST /executors` 注册执行器并获取槽位租约
- 心跳与驱逐：超时执行器先回收租约，再等槽内在途任务收尾后释放
- 重试与幂等：失败任务按幂等键重试，同一任务不会并发执行两遍
- 背压流控：慢执行器受队列与令牌桶限制，不拖垮调度路径
- 任务存储：有界环形缓冲与派发游标，派发失败的任务可重取

## 构建与运行

环境要求：Go 1.23+（vendor 目录已包含全部依赖，可离线构建）。

```bash
go build -mod=vendor -o jobsched ./cmd/jobsched
./jobsched -addr :8080
```

启动后：

- 健康检查：`GET /healthz`
- 状态总览：`GET /api/status`
- 发布任务：`POST /api/publish`，请求体
  `{"group":"batch/etl","payload":"hello","due_after_ms":0,"depends_on":[]}`
- 注册执行器：`POST /executors`，请求体 `{"id":"worker-1"}`
- 调度控制台：`GET /`（浏览器打开）

## 前端控制台

页面文件位于 `web/console.html`，编译时通过 `go:embed` 打进二进制，
运行后访问根路径即可使用。控制台支持发布任务、注册执行器与查看
任务/执行器/指标状态。

## Docker

```bash
docker build -f benzhi.Dockerfile -t jobsched .
docker run --rm -p 8080:8080 jobsched
```

镜像内关闭模块下载（GOPROXY=off），全部依赖走 vendor 离线构建。
