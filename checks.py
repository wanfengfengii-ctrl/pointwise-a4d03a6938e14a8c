import copy
import json
from lakeflow import Store, FlowError
from samples import initial, compact, correction

def normal():
    s = Store(':memory:')
    try:
        assert s.apply(initial()) == {'request':'load','generation':1}
        assert s.read([1,0]) == {'generation':1,'rows':[['east',2,20]]}
        assert s.read([2,2]) == {'generation':1,'rows':[['east',4,70]]}
        before = s.inspect()
        bad = compact()
        bad['changes'] = [['L',[3,0],['meter',90],1], ['R',[0,0],['meter','bad'],1]]
        try:
            s.apply(bad)
            raise AssertionError('late batch accepted')
        except FlowError as e:
            assert str(e) == 'LATE'
        assert s.inspect() == before
        s.apply(compact())
        assert s.read([2,2]) == {'generation':2,'rows':[['east',4,70]]}
        try:
            s.read([0,0])
            raise AssertionError('old cut accepted')
        except FlowError as e:
            assert str(e) == 'COMPACTED'
        s.apply(correction())
        assert s.read([2,2]) == {'generation':3,'rows':[['east',4,70]]}
        assert s.read([3,3]) == {'generation':3,'rows':[['west',1,25]]}
        again = initial()
        again['expected'] = 999
        assert s.apply(again) == {'request':'load','generation':1}
        assert s.inspect()['state']['generation'] == 3
        conflict = copy.deepcopy(again)
        conflict['changes'][0][3] = 3
        try:
            s.apply(conflict)
            raise AssertionError('identity conflict accepted')
        except FlowError as e:
            assert str(e) == 'REQUEST_CONFLICT'
        before = s.inspect()
        def stop(point):
            if point == 'after_state':
                raise RuntimeError('injected')
        req = {'request':'interrupted','expected':3,'changes':[], 'upper':{},'since':None}
        try:
            s.apply(req, stop)
            raise AssertionError('injection missed')
        except RuntimeError:
            pass
        assert s.inspect() == before
        print('NORMAL_REGRESSION_OK')
    finally:
        s.close()

def demo():
    s = Store(':memory:')
    try:
        s.apply(initial())
        print(json.dumps({'before':s.read([2,2])}, ensure_ascii=False))
        s.apply(compact())
        s.apply(correction())
        print(json.dumps({'historical':s.read([2,2]),'corrected':s.read([3,3]),
                          'replay':s.apply(initial())}, ensure_ascii=False))
    finally:
        s.close()

if __name__ == '__main__':
    import sys
    {'normal':normal, 'demo':demo}[sys.argv[1]]()
