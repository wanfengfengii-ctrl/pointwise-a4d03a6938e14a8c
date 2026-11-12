def initial():
    return {'request': 'load', 'expected': 0, 'changes': [
        ['L', [1,0], ['meter',10], 1],
        ['L', [0,1], ['meter',25], 1],
        ['R', [0,0], ['meter','east'], 2]],
        'upper': {'L': [[3,0],[0,3]], 'R': [[3,0],[0,3]]}, 'since': None}

def compact():
    return {'request': 'compact', 'expected': 1, 'changes': [],
        'upper': {'L': [[3,3]], 'R': [[3,3]]}, 'since': [[1,1]]}

def correction():
    return {'request': 'correction', 'expected': 2, 'changes': [
        ['L',[3,3],['meter',10],-1],
        ['R',[3,3],['meter','east'],-2],
        ['R',[3,3],['meter','west'],1]],
        'upper': {'L': [[4,4]], 'R': [[4,4]]}, 'since': None}
