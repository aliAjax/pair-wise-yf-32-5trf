# 器官分配与转运协调系统

Python 标准库独立项目。一位捐献者可携带多个器官，每个器官独立排序候选患者并独立经历提出、接受、转运、交接、植入或撤回；撤回或过期只影响对应器官，所有器官结束或失效后捐献者才显示已用完。器官过期后所有继续流转操作都会被阻止，全部状态变化写入审计记录。

## 运行

```bash
python3 app.py --db organ_allocation.db
```

默认监听 `127.0.0.1:8203`，首页 `/`（可展开查看捐献者下每个器官的进度与受阻位置），健康检查 `/health`。

身份头：`X-User-Id`、`X-Role`。角色为 `viewer`、`hospital`、`coordinator`、`allocation_officer`、`auditor`；医院角色还需 `X-Hospital`。

## 主要接口

- `POST /api/donors`：登记捐献者。可传单器官字段（`organ`/`available_at`/`expires_at`），或 `organs: [{organ,available_at,expires_at,clinical_match}]` 一次登记多器官。
- `POST /api/donors/{id}/organs`：为已有捐献者追加器官。
- `GET /api/donors`、`GET /api/donors/{id}`：捐献者列表/详情，含每个器官的 `effective_status`、当前分配和 `progress`（五段步骤与当前受阻位置）。
- `POST /api/candidates`：登记候选患者。
- `GET /api/organs/{id}/ranking`：查看某器官的兼容候选排序；`GET /api/donors/{id}/ranking` 仅在捐献者只有一个可分配器官时可用，多器官需指定 `organ_id`。
- `POST /api/allocations`：提出分配，参数为 `organ_id` 与 `candidate_id`（单器官捐献者也可回退使用 `donor_id`）。
- `POST /api/allocations/{id}/accept`、`withdraw`：医院确认或撤回（只回退该器官）。
- `POST /api/allocations/{id}/transit`、`delay`：冷链转运和延误上报。
- `POST /api/allocations/{id}/handoff`、`handoff-accept`：来源医院发起、接收医院确认。
- `POST /api/allocations/{id}/implant`：确认植入。
- `GET /api/allocations/{id}/audit`、`GET /api/donors/{id}/audit`、`GET /api/state`：单分配审计、捐献者全器官审计和权限视图。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

血型兼容与评分是演示规则，不包含 HLA 分型、器官大小、病程、儿科差异和真实移植网络规则。医院身份使用请求头模拟，SQLite 环境适合原型，不处理跨机构身份信任、远程患者隐私协议和真实冷链设备接入。
