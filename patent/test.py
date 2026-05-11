import json
import os
import math
import re
from collections import defaultdict, deque
from typing import List, Dict, Tuple, Any, Optional

import numpy as np
from sentence_transformers import SentenceTransformer

try:
    from datasets import load_dataset
except Exception:
    load_dataset = None

# =========================
# 说明：
# 这是一个更完整的实验脚本，实现了：
# - 支持从 HotpotQA/IIRC/2WikiMQA/MuSiQue 风格的数据加载（若提供文件）
# - 构建文本属性图、交互图和领域知识图谱（KG）
# - 多源核心实体识别（显性/隐性/历史）
# - 图结构约束的证据子图检索与多跳路径检索
# - 基于注意力的上下文聚合与简化自回归式答案生成（占位策略）
#
# 运行：
#   python patent/test.py
# 如果要用真实数据集，请传入 JSON 文件并在 load_dataset_from_file 中解析。
# =========================

# -------------------------
# 1. 初始化模型
# -------------------------
model = SentenceTransformer('all-MiniLM-L6-v2')

# -------------------------
# 1.1 数据集选择与缓存
# -------------------------
USE_HF_HOTPOT = True
HF_DATASET_NAME = "hotpotqa/hotpot_qa"
HF_SUBSET = "distractor"
HF_SPLIT = "train"
HF_SAMPLE_SIZE = 200
HF_CACHE_DIR = None  # 例如: "/data/hf_cache"
USE_LLM_KG_EXTRACT = False  # True 时启用大模型抽取（需自行实现/接入）

# -------------------------
# 2. 示例领域知识图谱（可被外部 KG 替换）
# -------------------------
KG: List[Tuple[str, str, str]] = [
    ("Alice", "works_at", "Google"),
    ("Alice", "graduated_from", "Stanford"),
    ("Google", "located_in", "USA"),
    ("Stanford", "located_in", "USA"),
    ("USA", "continent", "North America"),
    ("Bob", "works_at", "Microsoft"),
    ("Microsoft", "located_in", "USA"),
    ("Charlie", "works_at", "Amazon"),
    ("Amazon", "located_in", "USA"),
]

# 构建邻接表（含反向边以支持双向搜索）
graph: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
for h, r, t in KG:
    graph[h].append((r, t))
    graph[t].append(("rev_" + r, h))

# -------------------------
# 3. 数据加载与格式化
# 支持：HotpotQA 风格的 list/json 打包，或返回合成样例
# -------------------------

def load_dataset_from_file(path: str) -> List[Dict[str, Any]]:
    """尝试加载 JSON 格式的数据并转换为标准化条目。
    返回每条: {question, answer, context(list段落), supporting_facts, explicit(list)}
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    examples = []
    # 常见：HotpotQA 的 list of dict
    if isinstance(data, list):
        for item in data:
            q = item.get('question') or item.get('query')
            a = item.get('answer') or item.get('answers')
            if isinstance(a, list):
                a = a[0] if a else None
            ctx = item.get('context') or item.get('context_paragraphs') or []
            supp = item.get('supporting_facts') or []
            explicit = item.get('explicit_entities') or item.get('entity_mentions') or []
            examples.append({'question': q, 'answer': a, 'context': ctx, 'supporting_facts': supp, 'explicit': explicit})
    elif isinstance(data, dict):
        # 处理包装在 data 字段中的情况
        entries = data.get('data') or data.get('questions') or []
        for item in entries:
            q = item.get('question') or item.get('title')
            a = item.get('answer') or item.get('answers')
            if isinstance(a, list):
                a = a[0] if a else None
            ctx = item.get('context') or item.get('paragraphs') or []
            supp = item.get('supporting_facts') or []
            explicit = item.get('explicit_entities') or item.get('entity_mentions') or []
            examples.append({'question': q, 'answer': a, 'context': ctx, 'supporting_facts': supp, 'explicit': explicit})
    return examples


def load_hotpotqa_from_hf(split: str, sample_size: int = 200, cache_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    if load_dataset is None:
        raise RuntimeError("datasets is not available. Please install 'datasets' package.")
    ds = load_dataset(HF_DATASET_NAME, HF_SUBSET, split=split, cache_dir=cache_dir)
    examples = []
    for i, item in enumerate(ds):
        if sample_size and i >= sample_size:
            break
        q = item.get('question')
        a = item.get('answer')
        context = item.get('context') or []
        supporting = item.get('supporting_facts') or []
        examples.append({'question': q, 'answer': a, 'context': context, 'supporting_facts': supporting, 'explicit': []})
    return examples


def sample_synthetic_dataset() -> List[Dict[str, Any]]:
    return [
        {'question': 'Where does Alice work?', 'answer': 'Google', 'context': ['Alice is an engineer at Google.'], 'supporting_facts': [], 'explicit': ['Alice']},
        {'question': "Which country is Alice's company in?", 'answer': 'USA', 'context': ['Google is headquartered in USA.'], 'supporting_facts': [], 'explicit': ['Alice']},
        {'question': "Bob works at which company?", 'answer': 'Microsoft', 'context': ['Bob is employed by Microsoft.'], 'supporting_facts': [], 'explicit': []},
    ]


# -------------------------
# 4. 构建文本属性图与交互图
#    如果存在真实数据（context / logs），从中抽取；否则使用示例
# -------------------------

def build_text_graph_from_dataset(ds: List[Dict[str, Any]]) -> List[str]:
    paras: List[str] = []
    for ex in ds:
        ctx = ex.get('context') or []
        if isinstance(ctx, list):
            for p in ctx:
                if p and p not in paras:
                    paras.append(p)
        elif isinstance(ctx, str):
            if ctx not in paras:
                paras.append(ctx)
    if not paras:
        paras = [
            'Google is a multinational company based in USA.',
            'Microsoft is headquartered in USA and builds software.',
            'Stanford is a university in California in USA.',
        ]
    return paras


def build_interaction_graph_from_dataset(ds: List[Dict[str, Any]]) -> List[str]:
    # 如果有 interaction logs 可以替换此函数
    interactions: List[str] = []
    for ex in ds:
        q = ex.get('question')
        if q and q not in interactions:
            interactions.append('User asked: ' + q)
    if not interactions:
        interactions = [
            'User asked about Alice',
            'User searched Google company',
            'User explored Stanford university',
        ]
    return interactions


def normalize_hotpot_context(context: List[Any]) -> List[str]:
    """HotpotQA context: [[title, [sentences]], ...]. Convert to flat paragraphs."""
    paras: List[str] = []
    for item in context:
        if not isinstance(item, list) or len(item) != 2:
            continue
        title, sents = item[0], item[1]
        if isinstance(sents, list):
            for s in sents:
                if s:
                    paras.append(f"{title}: {s}")
        elif isinstance(sents, str):
            paras.append(f"{title}: {sents}")
    return paras


def extract_entities(text: str) -> List[str]:
    # Simple heuristic: capitalized word sequences
    if not text:
        return []
    pattern = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b")
    stop = {"The", "A", "An", "In", "On", "And", "Of", "For", "To", "With", "By", "At"}
    ents = [m.group(0) for m in pattern.finditer(text)]
    return [e for e in ents if e not in stop]


def extract_triples_from_sentence(sent: str) -> List[Tuple[str, str, str]]:
    """Lightweight pattern-based relation extraction for English sentences."""
    if not sent:
        return []
    triples: List[Tuple[str, str, str]] = []
    patterns = [
        (r"(.+?)\s+is\s+the\s+capital\s+of\s+(.+?)\.?$", "capital_of"),
        (r"(.+?)\s+is\s+located\s+in\s+(.+?)\.?$", "located_in"),
        (r"(.+?)\s+is\s+part\s+of\s+(.+?)\.?$", "part_of"),
        (r"(.+?)\s+was\s+born\s+in\s+(.+?)\.?$", "born_in"),
        (r"(.+?)\s+works\s+at\s+(.+?)\.?$", "works_at"),
        (r"(.+?)\s+graduated\s+from\s+(.+?)\.?$", "graduated_from"),
        (r"(.+?)\s+is\s+a[n]?\s+(.+?)\.?$", "is_a"),
    ]
    clean = sent.strip()
    for pat, rel in patterns:
        m = re.search(pat, clean, flags=re.IGNORECASE)
        if m:
            h = m.group(1).strip()
            t = m.group(2).strip()
            h_ents = extract_entities(h)
            t_ents = extract_entities(t)
            head = h_ents[0] if h_ents else h
            tail = t_ents[0] if t_ents else t
            triples.append((head, rel, tail))
    if not triples:
        ents = extract_entities(clean)
        if len(ents) >= 2:
            triples.append((ents[0], "co_mentioned", ents[1]))
    return triples


def extract_triples_with_llm(sent: str) -> List[Tuple[str, str, str]]:
    """LLM-based relation extraction placeholder.
    Return a list of (head, relation, tail). Implement your LLM call here.
    """
    return []


def build_text_graph_from_hotpot(ds: List[Dict[str, Any]]) -> List[str]:
    paras: List[str] = []
    for ex in ds:
        ctx = ex.get('context') or []
        if ctx:
            paras.extend(normalize_hotpot_context(ctx))
    return paras


def build_interaction_graph_from_hotpot(ds: List[Dict[str, Any]]) -> List[str]:
    interactions: List[str] = []
    for ex in ds:
        q = ex.get('question')
        if q:
            interactions.append('User asked: ' + q)
        supporting = ex.get('supporting_facts') or []
        for title, sent_idx in supporting:
            interactions.append(f"User opened: {title}")
            interactions.append(f"User read supporting sentence {sent_idx} in {title}")
    return interactions


def build_kg_from_hotpot(ds: List[Dict[str, Any]]) -> List[Tuple[str, str, str]]:
    triples: List[Tuple[str, str, str]] = []
    for ex in ds:
        context = ex.get('context') or []
        supporting = ex.get('supporting_facts') or []
        # Extract entity-relation-entity triples from supporting sentences
        for title, sent_idx in supporting:
            for item in context:
                if isinstance(item, list) and len(item) == 2 and item[0] == title:
                    sents = item[1]
                    if isinstance(sents, list) and sent_idx < len(sents):
                        sent = sents[sent_idx]
                        triples.extend(extract_triples_from_sentence(sent))
                        if USE_LLM_KG_EXTRACT:
                            triples.extend(extract_triples_with_llm(sent))
    return triples


# -------------------------
# 5. KG 实体向量化与工具函数
# -------------------------
kg_entities = list({h for h, _, _ in KG} | {t for _, _, t in KG})
kg_embeddings = model.encode(kg_entities, normalize_embeddings=True)
kg_entity_embeddings = {ent: model.encode([ent], normalize_embeddings=True)[0] for ent in kg_entities}


def init_kg_resources(kg_triples: List[Tuple[str, str, str]]):
    global KG, graph, kg_entities, kg_embeddings, kg_entity_embeddings
    KG = kg_triples if kg_triples else KG
    graph = defaultdict(list)
    for h, r, t in KG:
        graph[h].append((r, t))
        graph[t].append(("rev_" + r, h))
    kg_entities = list({h for h, _, _ in KG} | {t for _, _, t in KG})
    kg_embeddings = model.encode(kg_entities, normalize_embeddings=True)
    kg_entity_embeddings = {ent: model.encode([ent], normalize_embeddings=True)[0] for ent in kg_entities}


def sim(hE: np.ndarray, hG: np.ndarray) -> float:
    # 按论文公式 3: 1/(1+||x-y||)
    return 1.0 / (1.0 + float(np.linalg.norm(hE - hG)))


# -------------------------
# 6. 多源核心实体识别
# -------------------------
def get_core_entities(query: str, explicit_entities: List[str], text_embeddings: Optional[np.ndarray], text_graph: List[str], interaction_embeddings: Optional[np.ndarray], interaction_graph: List[str], top_k_text: int = 2, top_k_inter: int = 2) -> List[str]:
    Q = model.encode([query], normalize_embeddings=True)[0]

    # 文本图召回（Top-k 段落匹配并从段落中抽取实体候选）
    implicit_entities: List[str] = []
    if text_embeddings is not None and len(text_graph) > 0:
        text_scores = text_embeddings @ Q
        top_idxs = np.argsort(text_scores)[-top_k_text:]
        for idx in top_idxs:
            passage = text_graph[int(idx)].lower()
            for ent in kg_entities:
                if ent.lower() in passage:
                    implicit_entities.append(ent)

    # 交互图召回（Top-k）
    history_entities: List[str] = []
    if interaction_embeddings is not None and len(interaction_graph) > 0:
        inter_scores = interaction_embeddings @ Q
        top_i = np.argsort(inter_scores)[-top_k_inter:]
        for idx in top_i:
            history = interaction_graph[int(idx)].lower()
            for ent in kg_entities:
                if ent.lower() in history:
                    history_entities.append(ent)

    # 合并显性、隐性、历史候选
    candidates = list(dict.fromkeys((explicit_entities or []) + implicit_entities + history_entities))

    # 将候选映射到 KG 中最相似节点（向量匹配）
    core_entities: List[str] = []
    for ent in candidates:
        hE = kg_entity_embeddings.get(ent)
        if hE is None:
            hE = model.encode([ent], normalize_embeddings=True)[0]
        sims = [sim(hE, hG) for hG in kg_embeddings]
        best = kg_entities[int(np.argmax(sims))]
        core_entities.append(best)

    return list(dict.fromkeys(core_entities))


# -------------------------
# reasoning: 基于 KG 的 1-hop/2-hop 简单推理
# -------------------------
def reasoning(entities: List[str], max_hop: int = 2) -> set:
    answers = set()
    frontier = list(entities)
    visited = set(entities)
    for hop in range(max_hop):
        next_frontier = []
        for ent in frontier:
            for rel, t in graph.get(ent, []):
                if t not in visited:
                    answers.add(t)
                    next_frontier.append(t)
                    visited.add(t)
        frontier = next_frontier
    return answers


# -------------------------
# 7. 图结构约束的推理路径检索（多跳、证据子图）
# -------------------------

def bfs_paths(src: str, max_hop: int = 3) -> List[List[Tuple[str, str]]]:
    """从 src 出发，返回所有深度<=max_hop 的简单路径。路径用 (node, rel) 序列近似表示。"""
    paths: List[List[Tuple[str, str]]] = []
    queue = deque()
    queue.append((src, [], {src}))
    while queue:
        node, path, visited = queue.popleft()
        # path 长度以关系数量计，每追加一对 (node,rel)/(node,'') 作为展示
        if 0 < len(path) <= max_hop * 2:
            paths.append(path.copy())
        if len(path) >= max_hop * 2:
            continue
        for rel, nei in graph.get(node, []):
            if nei in visited:
                continue
            # path 以 (node,rel),(nei,'') 的形式增长，便于转换为文本
            new_path = path + [(node, rel), (nei, '')]
            new_visited = set(visited)
            new_visited.add(nei)
            queue.append((nei, new_path, new_visited))
    return paths


def path_to_text(path: List[Tuple[str, str]]) -> str:
    parts: List[str] = []
    for item in path:
        if item[1]:
            parts.append(f"{item[0]} {item[1]}")
        else:
            parts.append(item[0])
    return ' ; '.join(parts)


def score_paths_by_question(paths: List[List[Tuple[str, str]]], question: str, top_k: int = 3) -> List[Tuple[List[Tuple[str, str]], float]]:
    Q = model.encode([question], normalize_embeddings=True)[0]
    scored: List[Tuple[List[Tuple[str, str]], float]] = []
    for p in paths:
        txt = path_to_text(p)
        emb = model.encode([txt], normalize_embeddings=True)[0]
        score = float(np.dot(Q, emb))
        scored.append((p, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_k]


# -------------------------
# 8. 注意力机制的自回归推理（简化版）
#    - 聚合 top-k 路径的上下文
#    - 将聚合向量用于在候选实体/文本中选择答案（占位生成）
# -------------------------

def aggregate_context_with_attention(question: str, path_texts: List[str]):
    Q = model.encode([question], normalize_embeddings=True)[0]
    ctx_embs = model.encode(path_texts, normalize_embeddings=True)
    scores = (ctx_embs @ Q).tolist()
    exps = [math.exp(s) for s in scores]
    ssum = sum(exps) + 1e-12
    weights = [e / ssum for e in exps]
    agg = sum(w * e for w, e in zip(weights, ctx_embs))
    return agg, weights


def generate_answer_from_agg(agg_vec: np.ndarray, candidates: List[str]) -> str:
    # 简化的自回归占位：选择与聚合向量最相似的候选实体
    if not candidates:
        return ''
    cand_embs = model.encode(candidates, normalize_embeddings=True)
    sims = [float(np.dot(agg_vec, c)) for c in cand_embs]
    best = candidates[int(np.argmax(sims))]
    return best


# -------------------------
# 9. 主流程（示例运行）
# -------------------------

def main(dataset_path: str = None):
    # 加载/构建数据集
    if USE_HF_HOTPOT:
        ds = load_hotpotqa_from_hf(HF_SPLIT, HF_SAMPLE_SIZE, HF_CACHE_DIR)
        # 基于 HotpotQA 构建文本/交互图与 KG
        text_graph_local = build_text_graph_from_hotpot(ds)
        interaction_graph_local = build_interaction_graph_from_hotpot(ds)
        kg_triples = build_kg_from_hotpot(ds)
        init_kg_resources(kg_triples)
    elif dataset_path and os.path.exists(dataset_path):
        ds = load_dataset_from_file(dataset_path)
        text_graph_local = build_text_graph_from_dataset(ds)
        interaction_graph_local = build_interaction_graph_from_dataset(ds)
    else:
        ds = sample_synthetic_dataset()
        text_graph_local = build_text_graph_from_dataset(ds)
        interaction_graph_local = build_interaction_graph_from_dataset(ds)

    # 编码文本/交互图
    text_embeddings_local = model.encode(text_graph_local, normalize_embeddings=True) if text_graph_local else None
    interaction_embeddings_local = model.encode(interaction_graph_local, normalize_embeddings=True) if interaction_graph_local else None

    baseline_correct = 0
    multi_correct = 0
    gen_correct = 0

    for ex in ds:
        q = ex['question']
        gt = ex['answer']
        explicit = ex.get('explicit') or []

        # Baseline: 只用显式实体进行 2-hop 推理
        ans_base = reasoning(explicit)
        if gt in ans_base:
            baseline_correct += 1

        # Multi-graph: 多源核心实体识别 -> 推理
        core_entities = get_core_entities(q, explicit, text_embeddings_local, text_graph_local, interaction_embeddings_local, interaction_graph_local)
        ans_multi = reasoning(core_entities)
        if gt in ans_multi:
            multi_correct += 1

        # 证据子图 & 路径检索
        all_paths = []
        for c in core_entities:
            all_paths.extend(bfs_paths(c, max_hop=3))
        scored_paths = score_paths_by_question(all_paths, q, top_k=3)
        path_texts = [path_to_text(p) for p, _ in scored_paths]

        # 注意力聚合 & 占位式答案生成
        if path_texts:
            agg, weights = aggregate_context_with_attention(q, path_texts)
            # 候选答案集合：KG 实体 + 文本中命名实体（这里简化为 KG 实体）
            candidates = kg_entities
            gen_ans = generate_answer_from_agg(agg, candidates)
            if gen_ans == gt:
                gen_correct += 1
        else:
            gen_ans = ''

        print('\n问题:', q)
        print('- 显式实体:', explicit)
        print('- 核心实体:', core_entities)
        print('- Baseline 结果:', ans_base)
        print('- Multi-Graph 结果:', ans_multi)
        print('- Top Paths:', [path_to_text(p) for p, s in scored_paths])
        print('- 生成（占位）答案:', gen_ans, 'GT:', gt)

    n = len(ds) if ds else 1
    print('\n====== 实验结果 ======')
    print('Baseline Accuracy:', baseline_correct / n)
    print('Multi-Graph Accuracy:', multi_correct / n)
    print('Generator(agg) Accuracy:', gen_correct / n)


if __name__ == '__main__':
    # 可选地传入数据文件路径
    path = None
    main(path)
