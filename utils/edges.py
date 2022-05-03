#!/usr/bin/env python

# Relationship definitions for the NBA Knowledge Graph
from py2neo import Relationship

"""
This is just  a utility file to contain all 
relationship types defined in the NBA Knowledge Graph
"""

class playsFor(Relationship):
    """
    PLAYER --> TEAM edge
    """
    pass

class won(Relationship):
    """
    PLAYER --> PRIZE edge
    """
    pass

class wentTo(Relationship):
    """
    PLAYER --> COLLEGE edge
    """
    pass

class playsAt(Relationship):
    """
    PLAYER --> POSITION edge"""
    pass 