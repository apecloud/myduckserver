# Superset 实体 PostgreSQL 数据集的列发现

## 触发与结果

Superset 6.1.0 使用 SQLAlchemy 1.4 的 PostgreSQL 列发现查询。镜像 `641b1489` 可以列出 shop_us 的五张表，SQL Lab 和虚拟 SQL 数据集也能运行，但创建实体 orders 数据集时返回 HTTP 422：列查询依赖尚未实现的 `pg_get_serial_sequence`。进一步追踪发现同一链路的内置 domain 查询把 PostgreSQL OID 交给 DuckDB 的 `format_type`，得到错误或空的类型名。既有兼容 pg_attribute 还把所有用户列标为文本，不能用它完成准确反射。

此次补齐这两条完整 SQLAlchemy 查询：实体数据集可以读取真实列定义，并用所选 order_id 字段聚合 COUNT。保留原有 SQL Lab/虚拟看板业务回归，实体 COUNT 另作为实际看板卡片验证。

## 实现

在现有完整查询适配入口识别 SQLAlchemy 1.4 的两条固定查询。列查询只允许关系 OID 是整数、数字字符串或绑定参数；其余查询内容必须完整匹配，仅容忍空白与注释。修改投影、过滤或字符串常量的其他 SQL 不会被当作这条查询处理。

列结果来自实时 duckdb_columns()，保留列顺序、实际类型、默认表达式、可空性和表 OID；MyDuck 的受管注释经解码后返回人的注释、VARCHAR 长度及已有生成列表达式元数据。时间精度类型、double、decimal 等转换为客户端认识的 PostgreSQL类型名。未出现的表 OID 返回空行集，预备语句更换 OID 时重新读取实际结果。

MySQL AUTO_INCREMENT 的真实 nextval 默认表达式原样保留。MyDuck 没有实现 PostgreSQL identity 列，因此不生成虚假的 identity_options。已有生成列的元数据如存在则保留，未生成的列返回空值。

内置 domain 查询使用现有 PostgreSQL catalog 的 typbasetype→typname 关系解释类型，保留 varchar 长度、时间精度、nullable/default/schema 等字段，不再让 DuckDB按另一套编号解释它们。

## 边界

这是 SQLAlchemy 1.4 实际客户端查询的适配，不是通用 pg_get_serial_sequence、json_build_object 或 PostgreSQL domain DDL 的实现。不会把用户 SQL 中类似名字的函数或文字替换掉，也没有声明所有 PostgreSQL 元数据查询已兼容。pg_attribute 的通用元数据扩展不包含在此次补丁。

## 验证

真实服务器测试使用 Superset 抓取的 SQL，父提交在简单与预备协议都复现缺失函数。修复验证整型、自增默认、VARCHAR 长度与注释、decimal 精度/默认、timestamp、double、nullable、重复调用、更换 OID、业务 COUNT，并校验内置 domain 类型。修改投影/过滤/常量的语句保持不匹配。

应用验证必须经过实体 orders 数据集创建、读取列、所选字段 COUNT 169、实际卡片渲染，以及两款应用各九组业务查询；保存实际请求/响应和截图。候选二进制与正式不可变镜像分开记录，只有新的同一 digest 通过后才完成镜像验收。
