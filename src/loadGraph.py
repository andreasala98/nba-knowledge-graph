import pandas as pd
from py2neo import Graph, Node, Relationship

import os
import sys
from pathlib import Path
from tqdm import tqdm, trange

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent
sys.path.append(ROOT_DIR)


class NBAGraph:
    class playsFor(Relationship): pass #player --> team
    class won(Relationship): pass      #player --> prize
    class wentTo(Relationship): pass   #player --> college
    class playsAt(Relationship): pass  #player --> position

    def __init__(self, port=7687):

        conn = "bolt://localhost:" + str(port)
        self.driver = Graph(conn, auth=('neo4j', '1234'))
        self.driver.delete_all()

        self.prizes = pd.read_csv("data/prize-list.csv", sep=';', )
        self.df = pd.read_csv("data/Players-college.csv").dropna()

    def createPlayerNode(self,teamNode, positionNode, collegeNode, prizeNodes, **kwargs):
       n = Node("PLAYER", **kwargs)
       n.__primarykey__='NAME'
       n.__primarylabel__='PLAYER'
       self.driver.merge(n)

       col = self.wentTo(n, collegeNode)
       self.driver.merge(col)

       rel = self.playsFor(n, teamNode)
       self.driver.merge(rel)

       role = self.playsAt(n, positionNode)
       self.driver.merge(role)

       for prizeName, prizeNode in zip(["MVP","MIP","6MOTY"], prizeNodes):
           if n["NAME"] in self.prizes[self.prizes["prize"]==prizeName]["player"].values:
               rel_w = self.won(n, prizeNode)
               self.driver.merge(rel_w)

       return n

    def createTeamNode(self, **kwargs):
       n = Node("TEAM", **kwargs)
       n.__primarykey__='NAME'
       n.__primarylabel__='TEAM'
       self.driver.merge(n)

       return n

    def createCollegeNode(self, **kwargs):
       n = Node("COLLEGE", **kwargs)
       n.__primarykey__='NAME'
       n.__primarylabel__='COLLEGE'
       self.driver.merge(n)

       return n

    def createMVPnode(self,):
        n = Node("PRIZE", NAME='MVP')
        n.__primarykey__='NAME'
        n.__primarylabel__='PRIZE'
    
        self.driver.merge(n)
        return n

    def createMIPnode(self,):
        n = Node("PRIZE", NAME='MIP')
        n.__primarykey__='NAME'
        n.__primarylabel__='PRIZE'
    
        self.driver.merge(n)
        return n

    def create6moty(self,):
        n = Node("PRIZE", NAME='6MOTY')
        n.__primarykey__='NAME'
        n.__primarylabel__='PRIZE'

        self.driver.merge(n)
        return n

    def createPosNode(self,**kwargs):
        pn = Node("POSITION", **kwargs)
        pn.__primarykey__='NAME'
        pn.__primarylabel__ = "POSITION"

        self.driver.merge(pn)
        return pn


    def processAllNodes(self):
        prizeList = self.createMVPnode(), self.createMIPnode(), self.create6moty()

        for i in trange(len(self.df)):
            row = self.df.iloc[i,:]

            college_args = {
                "NAME": row["College"]
            }

            team_args = {
                "NAME": row["Team"]
            }

            player_args = {
                "NAME":     str(row["Name"]),
                "NUMBER":   str(row["Number"]),
                "AGE":      str(row["Age"]),
                "HEIGHT":   str(row["Height"]),
                "WEIGHT":   str(row["Weight"]),
                "SALARY":   str(row["Salary"])
            }

            pos_args = {

                "NAME": row["Position"],
            }

            pos = self.createPosNode(**pos_args)
            c =   self.createCollegeNode(**college_args)
            t =   self.createTeamNode(**team_args)
            p =   self.createPlayerNode(teamNode=t, positionNode=pos, collegeNode=c,
                                        prizeNodes=prizeList, **player_args)



if __name__ == "__main__":
    g = NBAGraph()
    g.processAllNodes()