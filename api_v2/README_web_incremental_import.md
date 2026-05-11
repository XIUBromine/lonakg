# 增量导入 Web 服务部署说明

## 1. 功能说明

该服务用于上传某日 CSV，并触发对应 `api_v2` 导入器写入 Neo4j。

数据集映射如下：

- `customer_log` -> `import_customer_logs`
- `logout` -> `import_logout`
- `login` -> `import_login`
- `lbs_gps` -> `import_lbs_gps`
- `first_linkman` -> `import_linkman`
- `second_linkman` -> `import_linkman`
- `first_linkman_derived` -> `import_linkman`
- `second_linkman_derived` -> `import_linkman`
- `order` -> `import_order`
- `repay_plan` -> `import_repay_plan`
- `company_log` -> `import_company_log`
- `complaint` -> `import_complaint`
- `consumer_protection_case` -> `import_consumer_case`
- `blacklist_log` -> `import_blacklist_log`

## 2. 安装依赖

在项目根目录执行：

```bash
cd /home/lsy/lonakg
pip install -r api_v2/requirements_web.txt
```

## 3. 环境变量

可选环境变量：

- `NEO4J_URI`，默认 `bolt://localhost:7687`
- `NEO4J_USER`，默认 `neo4j`
- `NEO4J_PASSWORD`，默认 `kBXwIuxLTvgxnbGD`
- `INCREMENTAL_UPLOAD_ROOT`，默认 `api_v2/uploads`
- `MAX_UPLOAD_MB`，默认 `200`
- `WEB_IMPORT_HOST`，默认 `0.0.0.0`
- `WEB_IMPORT_PORT`，默认 `18080`
- `WEB_IMPORT_DEBUG`，默认 `false`

示例：

```bash
export NEO4J_URI='bolt://127.0.0.1:7687'
export NEO4J_USER='neo4j'
export NEO4J_PASSWORD='your_password'
export WEB_IMPORT_PORT='18080'
```

## 4. 启动服务

前台启动：

```bash
cd /home/lsy/lonakg/api_v2
python web_incremental_import.py
```

启动后访问：

- `http://<server-ip>:18080/` 页面
- `http://<server-ip>:18080/api/datasets` 映射查询

## 5. nohup 后台部署

```bash
cd /home/lsy/lonakg/api_v2
nohup python web_incremental_import.py > web_incremental_import.log 2>&1 &
```

查看日志：

```bash
tail -f /home/lsy/lonakg/api_v2/web_incremental_import.log
```

## 6. 接口调用示例

上传并导入：

```bash
curl -X POST 'http://127.0.0.1:18080/api/import' \
  -F 'dataset=customer_log' \
  -F 'database=dev' \
  -F 'files=@/path/to/2026-04-12.csv'
```

黑名单导入可带 `blacklist_max_ts`：

```bash
curl -X POST 'http://127.0.0.1:18080/api/import' \
  -F 'dataset=blacklist_log' \
  -F 'database=dev' \
  -F 'blacklist_max_ts=2025-11-20T00:00:00' \
  -F 'files=@/path/to/2025-11-19.csv'
```
