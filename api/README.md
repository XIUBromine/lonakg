这几个脚本的作用依次是：
1. stream_kg_api.py: 一个flask后端，提供图谱动态更新的接口，接收以HTTP传来的事件数据流，解析后更新图谱数据库【dev】
2. stream_kg_api_test.py: 测试stream_kg_api.py的接口是否能正常接收数据并更新图谱数据库
3. import_events_prod.py: 复用stream_kg_api.py中的数据处理和图谱更新逻辑，直接从/data/processed/events目录下读取事件数据流并导入图谱数据库【prod】。该脚本所需运行时间【极长】，正式运行务必以nohup的方式在服务器上运行（参考README_stream_api.md）

在调试或运行时，务必检查好使用的图数据库：
1. dev是一个测试的图数据库，你可以随便增删
2. prod是正式的图数据库，里面有重要的数据，不能修改（东豪要用）
3. 你也可以自行创建新的图数据库来测试（连接neo4j的方式见服务器连接文档）

# 接下来你需要做什么？
1. 先把新的事件数据流预处理好，然后思考如何设计图谱的构建规则（在PPT的基础上改进，尤其要引入公司log表），与我讨论图谱构建规则
2. 修改stream_kg_api.py中的事件处理逻辑，重构import_events_prod.py并运行，按照你设计的新的图谱构建规则把事件数据流正确地导入图谱数据库