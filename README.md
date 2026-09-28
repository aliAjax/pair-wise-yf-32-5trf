# 器官分配与转运协调系统

Python 标准库独立项目。一位捐献者可登记多个器官，每个器官各自独立排序、独立流转：分别提出分配，各自经历接受、转运、交接、植入，撤回或过期只影响对应器官，其余器官照常推进；全部器官植入或失效后捐献者才显示「已用完」。系统按器官类型、血型、地域、医疗匹配、紧急程度和等待时间排序候选患者，过期后继续流转会被阻止，全部状态变化写入审计记录。

## 运行

```bash
python3 app.py --db organ_allocation.db
```

默认监听 `127.0.0.1:8203`，首页 `/`（可展开捐献者查看每个器官的步骤进度与当前受阻位置），健康检查 `/health`。

身份头：`X-User-Id`、`X-Role`。角色为 `viewer`、`hospital`、`coordinator`、`allocation_officer`、`auditor`；医院角色还需 `X-Hospital`。

## 主要接口

- `POST /api/donors`：登记捐献者。单器官沿用 `organ`+`expires_at`；多器官用 `organs` 数组，每项为 `{"organ","expires_at","available_at"?, "clinical_match"?}`（窗口/匹配度缺省取捐献者级字段）。
- `POST /api/candidates`：登记候选患者。
- `GET /api/donors/{id}`：捐献者详情，含每个器官的状态、流程步骤、当前阶段与「受阻位置」（等待哪个角色做什么）。
- `GET /api/donors/{id}/ranking`：单器官捐献者的候选排序；多器官请用 `GET /api/organs/{id}/ranking`。
- `POST /api/allocations`：提出分配。多器官时传 `organ_id`（+`candidate_id`）；单器官可继续传 `donor_id`。撤回或过期后的器官可再次提出分配。
- `POST /api/allocations/{id}/accept`、`withdraw`：医院确认或撤回（只放回该器官）。
- `POST /api/allocations/{id}/transit`、`delay`：冷链转运和延误上报。
- `POST /api/allocations/{id}/handoff`、`handoff-accept`：来源医院发起、接收医院确认。
- `POST /api/allocations/{id}/implant`：确认植入（只结束该器官）。
- `GET /api/allocations/{id}/audit`、`GET /api/state`：完整审计和权限视图。

旧版（捐献者与器官合一）数据库在启动时自动迁移到每器官模型，历史捐献者、器官与分配 ID 保持不变。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

血型兼容与评分是演示规则，不包含 HLA 分型、器官大小、病程、儿科差异和真实移植网络规则。医院身份使用请求头模拟，SQLite 环境适合原型，不处理跨机构身份信任、远程患者隐私协议和真实冷链设备接入。
