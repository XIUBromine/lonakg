- 将完整的数据文件放在`data`目录下，示例数据文件放在`test`目录下
- 在项目的根目录下创建一个`.env`文件，配置Neo4j的连接信息，示例如下：
  
  ```env
  NEO4J_URI=bolt://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=your_password
  ```
- 依次运行`data_loader`下的各个脚本（login、customer、linkman、gps、order、blacklist），将数据导入Neo4j数据库