from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from typing import List, Set, Dict
from collections import deque

print('server is up and running')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*'],
)

class Node(BaseModel):
    id: str
    model_config = ConfigDict(extra='allow')

class Edge(BaseModel):
    source: str
    target: str
    model_config = ConfigDict(extra='allow')

class PipelineRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
def parse_pipeline(payload: PipelineRequest):
    node_ids = {n.id for n in payload.nodes}
    num_nodes = len(node_ids)
    num_edges = len(payload.edges)

    adj: Dict[str, Set[str]] = {nid: set[str]() for nid in node_ids}
    in_degree: Dict[str, int] = {nid: 0 for nid in node_ids}
    for e in payload.edges:
        if e.source in node_ids and e.target in node_ids:
            if e.target not in adj[e.source]:
                adj[e.source].add(e.target)
                in_degree[e.target] += 1

    q = deque[str]([nid for nid in node_ids if in_degree[nid] == 0])
    visited = 0
    while q:
        u = q.popleft()
        visited += 1
        for v in adj[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                q.append(v)

    is_dag = visited == num_nodes
    return {'num_nodes': num_nodes, 'num_edges': num_edges, 'is_dag': is_dag}