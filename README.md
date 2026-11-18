# LaminaDB

LaminaDB 是一个 Go 实现的单进程关系数据库内核和 HTTP 服务，面向数据库工程学习、存储实验和可复现的语义研究。它实现自己的类型系统、快照版本、提交校验、二级索引、查询执行、日志和恢复；不依赖 SQLite、PostgreSQL 或在线服务。

领域：**基础设施 / 数据库**。主要技术：Go 标准库、HTTP、版本链、CRC32 帧日志、SHA-256 图像。

## 构建和启动

要求 Go 1.25+，Linux 或 macOS。生产工作流演示另外使用 Python 3 标准库。

```sh
go build -o bin/laminad ./cmd/laminad
./bin/laminad -data var/lamina -listen 127.0.0.1:7386
```

数据目录只能由一个进程打开。默认仅监听回环地址；服务没有身份认证，不应直接暴露在不可信网络。进程收到 SIGTERM/SIGINT 后停止接受新请求，等待 HTTP 请求结束，再关闭数据库。没有在退出时强制生成检查点，已成功返回的提交依赖日志恢复。

```sh
python3 examples/walkthrough.py --workflow all
```

该命令启动临时实例，执行 14 条真实生产入口工作流，输出逐条观测 JSON，最后关闭实例并删除本次临时目录。可用 `--workflow query-plan` 等方式只运行一条。它是有界运行演示，不是基线测试套件。测试代码和自动化测试基础设施有意留待后续工作，本基线 `testing=deferred`。

行布局升级另有独立核对脚本，它直接写出格式 1 的日志、检查点和备份，再交给生产入口处理：

```sh
python3 examples/layout-migration.py
python3 examples/layout-migration.py --legacy-binary /path/to/previous/laminad
```

不加 `--legacy-binary` 时校验数据语义、占用变化和失败关闭；加上时额外确认旧版本能打开哪些目录。

## 一个事务

所有业务接口前缀为 `/v1/lamina`，JSON 标量仅支持有符号 64 位整数、文本、布尔值和 null。

```sh
curl -s http://127.0.0.1:7386/v1/lamina/relations -H 'Content-Type: application/json' -d '{"name":"spectra","primary":"sample","columns":[{"name":"sample","type":"int"},{"name":"band","type":"text"},{"name":"amplitude","type":"int","nullable":true}]}'
curl -s http://127.0.0.1:7386/v1/lamina/transactions -H 'Content-Type: application/json' -d '{}'
```

取返回的事务 `id`，在下面 URL 中替换 `<id>`：

```sh
curl -s http://127.0.0.1:7386/v1/lamina/transactions/<id>/put -H 'Content-Type: application/json' -d '{"relation":"spectra","row":{"sample":1,"band":"infrared","amplitude":40},"insert_only":true}'
curl -s -X POST http://127.0.0.1:7386/v1/lamina/transactions/<id>/commit
curl -s http://127.0.0.1:7386/v1/lamina/sql -H 'Content-Type: application/json' -d '{"sql":"SELECT sample, amplitude FROM spectra WHERE amplitude >= 20 ORDER BY amplitude DESC LIMIT 10;"}'
```

`put` 是完整行替换；缺失的可空列补为 null，缺失的非空列拒绝。主键从新行中取得，不提供原主键重命名操作。删除接口为事务 URL 下的 `delete`，正文为 `{"relation":"spectra","key":1}`；不存在的键也可以形成墓碑。DDL 自身是单个耐久操作，不在用户事务中暂存。

## 查询与隔离

`POST /query` 接收 `relation`、`columns`、`where`、`order`、`limit`，以及可选的 `transaction` 或 `at`。两种快照选择互斥。`where` 是 AND 连接的比较，支持 `= != < <= > >= is_null not_null`；`order` 指定列及 `desc`。

```json
{"relation":"spectra","where":[{"column":"amplitude","op":">=","value":20}],"order":[{"column":"amplitude","desc":true}],"limit":10}
```

读事务固定开始时的提交序号，重复读稳定，支持读自己的暂存写入。普通读使用当前序号。按 `at` 查询只接受保留区间内的序号，并要求关系在该序号已存在。事务无法访问在它开始之后创建的关系。无显式排序时按类型化主键排序；排序相同时也用主键确定次序。升序 null 在前，降序 null 在后。null 普通比较不匹配；使用 `is_null` 检查。

数据库提供快照隔离和同键 first-committer-wins，不承诺串行化、谓词锁或跨行写偏差防护。提交前对最终可见状态检查所有受影响关系的唯一索引；一次事务内可以置换唯一值。失败提交会关闭该事务，不发布任何私有行。只读事务通过 `{"read_only":true}` 开始。

SQL 只支持单条 SELECT：投影或 `count(*)`、`count(column)`、`sum(integer_column)`，可选 WHERE AND、ORDER BY、LIMIT。字符串使用单引号，内部单引号写两次；关键字及未引号标识符转为小写。没有 JOIN、GROUP BY、别名、表达式、SQL DDL/DML。语义检查与结构化查询共用绑定和执行。`sum` 忽略 null，空输入返回 null，溢出报错；`count(column)` 忽略 null，`count(*)` 计算全部匹配行。

## 索引

`POST /indexes` 的示例正文：

```json
{"name":"by_amplitude","relation":"spectra","column":"amplitude","unique":false}
```

单列索引由类型化有序切片维护，保留历史非空值到主键的候选关联。唯一约束允许多个 null。索引在当前提交序号同步构建；构建失败不会发布目录。规划器在可用索引中选择候选键最少的一条，之后读取指定快照的真实版本并执行全部谓词。对早于索引创建时刻的查询使用扫描；含私有写入的事务查询也使用扫描。回收后重建历史候选。

结果的 `plan.candidates` 是访问路径候选键数，`scanned` 是存在可见行并进入残余判断的行数，`matched` 是限制输出前的匹配数。保留历史候选可能导致这些数字差异很大。`force_scan:true` 可用于对照观测。`plan` 是执行事实，不是 I/O 耗时估计。

## 日志、检查点、还原

提交顺序为校验、完整帧写入、fsync、内存发布。每个日志帧包括长度、CRC32 和完整提交记录。日志序号连续，当前记录格式版本为 2。

行在内部按关系目录的列位置保存为定宽值序列，列名和类型只存于目录；请求与结果仍是按列名的 JSON。

格式 1 的旧日志、检查点和备份是只读升级输入。启动时按序转换并重放在内存中，每条旧行都必须由该关系在此刻的目录解释；未知列、类型或空值不符、主键与存储键不符都会拒绝启动，且不改写目录。全部记录转换、重放和约束校验成功后，先原子替换检查点，再原子替换日志。转换只发布一次：升级后的目录只含格式 2，旧版本无法打开；未升级的目录与旧备份文件仍能由旧版本打开。若在两步之间中断，目录留下新检查点加仍含格式 1 记录的日志，新版本可继续打开并完成剩余转换，旧版本已不能打开。`GET /stats` 的 `row_layout`、`store_format`、`value_slots` 和 `upgraded_on_open` 报告实际布局、非墓碑值槽数和本次是否发生转换。

写盘或同步失败后实例拒绝继续服务；调用方可能无法确定该次提交最终是否落盘，应重启后核对实际状态，基线没有请求级去重协议。

启动先验证检查点，再按序重放剩余记录。只自动截断最后一个不完整帧；完整帧校验不匹配、非法长度或序号异常都会拒绝启动。完整日志一直保留，检查点不会截短日志。数据库状态在内存中，日志和镜像会随数据量增长。

`POST /checkpoint` 在数据库锁内捕获并写入图像，临时文件同步后原子替换检查点并同步目录，因此会暂停提交和查询。`GET /backup` 生成包含目录、所有保留版本、提交序号和回收水位的冻结图像，带 SHA-256 摘要；之后的源库提交不会改变已返回的图像。

```sh
curl -s http://127.0.0.1:7386/v1/lamina/backup -o /tmp/lamina-backup.lmi
./bin/laminad -restore /tmp/lamina-backup.lmi -data var/lamina-restored -listen 127.0.0.1:7387
```

还原目标必须不存在。启动前验证图像摘要、格式、目录、版本顺序及行类型，再安装图像和带恢复基点的日志头。还原保留图像内的历史读版本，但不包含源库历史日志记录；变更消费者必须从恢复基点重新对齐。禁止将备份文件当成一个新根日志目录手动拼接。

## 变更与回收

`GET /changes?after=0&limit=100` 返回按提交序号递增的目录、提交和回收记录。分页单位是完整记录，不拆分事务，`cursor` 是已返回的最后序号，`head` 是观测时的数据库头部序号。还原实例的 `history_base` 之前没有变更历史，旧游标返回 410。

`POST /vacuum` 正文为 `{"horizon":<sequence>}`。实际水位取请求值和所有活动事务快照的最小值；活动读者阻挡时 `pinned=true`。每条版本链保留水位时的最后版本和所有更新版本；水位前的最终墓碑可以全部移除。低于持久回收水位的按序号查询返回 410，水位不能回退。回收并不清理日志磁盘占用。

## 资源与错误

请求体上限 1MiB，列数 64，文本值 64KiB，活动事务 128，每事务最终写集 1000 个键，单日志帧 16MiB，镜像 128MiB。查询最多 32 个 AND 谓词、8 个排序项，默认输出 1000 行，可显式设到 10000；截断时 `truncated=true`。查询执行仍可能物化全部匹配行，事务没有自动过期，排序和备份没有外溢机制，应显式关闭不用的会话并为演练设置合理数据规模。

错误 JSON 为 `{"error":{"code":"...","message":"..."}}`。无效输入 400，已关闭或未知事务及缺失行 404，写冲突或唯一冲突 409，快照和变更历史不可用 410，容量拒绝 429，存储不确定或已关闭 503。`GET /stats` 返回会话、版本、候选索引大小、恢复尾字节等领域事实，进程计数在重启后归零。

## 容器

```sh
docker build -t lamina-db .
docker run --rm -p 127.0.0.1:7386:7386 -v lamina-volume:/data lamina-db
```

镜像使用非特权用户 10001。容器工作流需本机镜像构建可用；原生运行不依赖 Docker。持久目录使用 Unix 文件锁，当前不声明 Windows 支持。备份只能缓解存储故障；没有复制、自动切换或加密。
