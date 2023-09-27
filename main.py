from neo4j import GraphDatabase
import pandas as pd
from pyvis.network import Network
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from graphdatascience import GraphDataScience
from fastapi.responses import FileResponse




driver = GraphDatabase.driver(f"bolt://localhost:7687", auth=('neo4j', '12345678'), 
# database="data_public"
                              ) 

# +
host = "bolt://localhost:7687"
user = "neo4j"
password= "12345678"

gds = GraphDataScience(host, auth=(user, password))
# -

gds

# +
# query = """
# CALL db.index.fulltext.queryRelationships("relacionindexobjeto", $valor) YIELD relationship, score
# MATCH (startNode)-[relationship]->(endNode)
# RETURN startNode.ruc as ruc_proveedor, startNode.tipo, endNode.ruc as ruc_ep, relationship.description_cpc, relationship.Objeto as objeto, score
# """
# params = {'valor': 'manzana'}
# gds.run_cypher(query, params)

# +
# G, metadata = gds.graph.project(
#     "hp-graph", 
#     ['Accionista', 'Empresa_Publica'],
#     {"INTERACTS": {"type": 'PROVEEDOR',"orientation": "UNDIRECTED", "properties": ["Monto"]}}
# )

# +
# metadata

# +

# CALL db.index.fulltext.queryRelationships("relacionindexobjeto", $valor) YIELD relationship, score
# CALL gds.graph.create('undirected_weighted', ['Accionista', 'Empresa_Publica'],
#     {INTERACTS_1:{type: 'PROVEEDOR',
#                   orientation: 'UNDIRECTED',
#                   properties:['Monto']}
#     }
#                      )
# -



# +
# CALL gds.graph.create('undirected_weighted',['Person', 'Thing'],
#     {INTERACTS_1:{type: 'INTERACTS_1',
#                   orientation: 'UNDIRECTED',
#                   properties:['weight']},
#     INTERACTS_2:{type:'INTERACTS_2',
#                  orientation: 'UNDIRECTED',
#                  properties:['weight']},
#     INTERACTS_3: {type:'INTERACTS_3',
#                   orientation:'UNDIRECTED',
#                   properties:['weight']}});
# -

# pip install python-louvain
# pip install fastapi
# pip install fastapi uvicorn
# pip install graphdatascience


app = FastAPI()

# Monta la carpeta build del proyecto React como archivos estáticos
# app.mount("/static", StaticFiles(directory="C:/Users/Diego/proyectos/aihack/conexion/build"), name="static")
app.mount("/static", StaticFiles(directory="build/static"), name="static")


# +

# Configura CORS para permitir solicitudes desde la URL de tu aplicación React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Reemplaza con la URL de tu aplicación React
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Resto de tu código FastAP

# +
# with driver.session() as session:
#     result = session.run(
#         """
#         CALL db.index.fulltext.queryRelationships("relacionindexobjeto", 'carro') YIELD relationship, score
#         MATCH (startNode)-[relationship]->(endNode)
#         RETURN startNode.ruc as ruc_proveedor, startNode.tipo, endNode.ruc as ruc_ep, relationship.description_cpc, relationship.Objeto as objeto, score
#         """,
#         # valor=valor  # Utiliza el parámetro de consulta $valor
#     )

#     rows = []
#     for record in result:
#         row = {
#             "ruc_proveedor": record["ruc_proveedor"],
#             "ruc_ep": record["ruc_ep"],
#             "objeto": record["objeto"],
#         }
#         rows.append(row)

#     df = pd.DataFrame(rows)

# +
def plotnodes(df):
    df=df[0:1000]

    # Crea un gráfico de red
    graph = Network(height='900px', width='100%', directed=True,notebook=True, bgcolor="#222222", font_color="white",
                    # select_menu=True, filter_menu=True
                   )

    # Agrega nodos (proveedores y empresas públicas) al gráfico
    nodes = set(df['ruc_proveedor']).union(set(df['ruc_ep']))
    for node in nodes:
        graph.add_node(node)

    # Agrega aristas (conexiones) entre proveedores y empresas públicas
    for _, row in df.iterrows():
        graph.add_edge(row['ruc_proveedor'], row['ruc_ep'], title="Relación")
        
        
        

    # Guarda el gráfico en un archivo HTML o muestra en el navegador
    graph.toggle_physics(False)
    # graph.toggle_drag_nodes(False)
    # graph.show_buttons(filter_=['physics'])
    # graph.show_buttons()

    # graph.show('network_graph.html')
    
    return graph

# g=plotnodes(df)

# # g.show_buttons(filter_=['nodes', 'edges'])
# # g.show_buttons()
# g.show_buttons(filter_=['physics'])
# g.show('network_graph.html')
# -

@app.get("/home")
async def get_main():
    return FileResponse("build/index.html")


@app.get("/")
def findeword(valor: str):                          
    with driver.session() as session:
        result = session.run(
            """
            CALL db.index.fulltext.queryRelationships("relacionindexobjeto", $valor) YIELD relationship, score
            MATCH (startNode)-[relationship]->(endNode)
            RETURN startNode.ruc as ruc_proveedor, startNode.tipo, endNode.ruc as ruc_ep, relationship.description_cpc, relationship.Objeto as objeto, score
            
            """,
            valor=valor  # Utiliza el parámetro de consulta $valor
        )

        rows = []
        for record in result:
            row = {
                "ruc_proveedor": record["ruc_proveedor"],
                "ruc_ep": record["ruc_ep"],
                "objeto": record["objeto"],
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        
        # Convertir el DataFrame a una lista de diccionarios sin índice
        json_result = df.to_dict(orient='records')
        
        return json_result
        # grafo=plotnodes(df)
        # grafo.show('network_graph.html')


@app.get("/rank")
def findeword(valor: str):                          
    with driver.session() as session:
        result = session.run(
            """
            CALL db.index.fulltext.queryRelationships("relacionindexobjeto", $valor) YIELD relationship, score
           MATCH (startNode)-[relationship]->(endNode)
            WITH gds.graph.project(
              'mi_grafo_filtrado',
              startNode, endNode

            ) AS g

            CALL gds.pageRank.stream('mi_grafo_filtrado')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).nombre as nombre, gds.util.asNode(nodeId).ruc as ruc, score
            ORDER BY score DESC
            
            """,
            valor=valor  # Utiliza el parámetro de consulta $valor
        )

        rows = []
        for record in result:
            row = {
                "nombre": record["nombre"],
                "ruc": record["ruc"],
                 "score": record["score"],
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        
        # Convertir el DataFrame a una lista de diccionarios sin índice
        json_result = df.to_dict(orient='records')
        
        return json_result
        # grafo=plotnodes(df)
        # grafo.show('network_graph.html')


def testconect():
    with driver.session() as session:
        result = session.run(
            """
          MATCH (n) RETURN n.ruc AS Accionista_ruc, n.nombre AS Accionista_nombre  LIMIT 10

            """
        )

        rows = []
        for record in result:
          row = {
              "RUC Accionista": record["Accionista_ruc"],
              "Nombre": record["Accionista_nombre"],
          }
          rows.append(row)

        df = pd.DataFrame(rows)
