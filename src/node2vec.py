#!/usr/bin/env python

# node2vec algo call directly on neo

from matplotlib import pyplot as plt
import seaborn as sns
import numpy as np
from py2neo import Graph
import pandas as pd
#from sklearn.preprocessing import PCA
from sklearn import decomposition as pp
import plotly.express as px




class NODE2VEC:
	def __init__(self, port=7687):
		conn = "bolt://localhost:" + str(port)
		self.driver = Graph(conn, auth=("neo4j","1234"))

	def parse_labels(self,):
		self.ref = self.driver.run("match(n) return id(n) as nodeId, labels(n)[0] as label").to_data_frame()

	def createGraph(self, graph_name="node2vec-nba"):
		graph_already_exists = self.driver.run(
			f"call gds.graph.exists('{graph_name}') yield exists as rx return rx"
		).to_data_frame().values[0, 0]
		
		if not graph_already_exists:
			self.driver.run(f"""call gds.graph.create('{graph_name}',
							['PLAYER', 'COLLEGE', 'TEAM', 'PRIZE', 'POSITION'],
							['playsFor', 'wentTo', 'playsAt', 'won' ])""")
			print(f"Created graph {graph_name} inside gds catalog")

		self.graph_name = graph_name

	def embed(self, dim=50):
		assert type(dim) is int, "dim must be an integer"

		df = self.driver.run("call gds.beta.node2vec.stream('node2vec-nba', " +
					   " {embeddingDimension: " + str(dim) + "}) yield nodeId, embedding""").to_data_frame(
					   )
		#df[["emb" + str(i) for i in range(dim)]] = df["embedding"].values.tolist()
		embs_exp = df["embedding"].apply(pd.Series)
		self.embs = pd.DataFrame(embs_exp.values, columns=[['emb' + str(i) for i in range(dim)]])
		self.embs["nodeId"] = df["nodeId"]


	def reduce(self, dim=3):
		self.pca = pp.PCA(dim)
		embs = self.embs.values[:,1:]
		red = self.pca.fit_transform(embs)
		self.reduced = pd.DataFrame()
		self.reduced[["pca" + str(i) for i in range(dim)]] = red
		self.reduced["label"] = self.ref["label"]

	def main(self,):
		self.createGraph()
		self.parse_labels()
		self.embed(dim = 100)
		self.reduce(dim = 2)
		print(self.reduced)

		fg = sns.scatterplot(data=self.reduced, x='pca0', y='pca1', hue='label')
		plt.show()

if __name__ == "__main__":
	NODE2VEC().main()