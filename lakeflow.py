import copy
import json
import sqlite3

class FlowError(ValueError):
    pass

def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)

def integer(v):
    return type(v) is int

def le(a, b):
    return all(x <= y for x, y in zip(a, b))

def timepoint(t, d):
    if not isinstance(t, list) or len(t) != d or any(not integer(v) or v < 0 for v in t):
        raise FlowError('TIME')
    return t[:]

def frontier(f, d, empty=True):
    if not isinstance(f, list) or (not empty and not f):
        raise FlowError('FRONTIER')
    f = [timepoint(t, d) for t in f]
    if any(le(a, b) for i, a in enumerate(f) for j, b in enumerate(f) if i != j):
        raise FlowError('ANTICHAIN')
    return sorted(f)

def precedes(a, b):
    return all(any(le(x, y) for x in a) for y in b)

def admits(f, t):
    return any(le(x, t) for x in f)

def advance(t, f):
    return [min(max(t[j], x[j]) for x in f) for j in range(len(t))]

def consolidate(rows, since):
    weights = {}
    for t, row, w in rows:
        t = advance(t, since)
        k = (tuple(t), tuple(row))
        weights[k] = weights.get(k, 0) + w
    return [[list(t), list(row), w] for (t, row), w in sorted(weights.items()) if w]

def strict_load(text):
    return json.loads(text, parse_constant=lambda s: (_ for _ in ()).throw(FlowError('JSON_NUMBER')))

class Store:
    def __init__(self, db, dimensions=2):
        if not integer(dimensions) or not 2 <= dimensions <= 6:
            raise FlowError('DIMENSIONS')
        self.c = sqlite3.connect(db, timeout=30, isolation_level=None)
        self.c.execute('PRAGMA journal_mode=WAL')
        self.c.execute('PRAGMA synchronous=FULL')
        self.c.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)')
        self.c.execute('CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, payload TEXT NOT NULL, response TEXT NOT NULL)')
        initial = dict(dimensions=dimensions, generation=0, since=[[0]*dimensions],
                       upper={'L': [[0]*dimensions], 'R': [[0]*dimensions]},
                       traces={'L': [], 'R': []})
        self.c.execute('INSERT OR IGNORE INTO state VALUES(1,?)', (encode(initial),))
        if self._state()['dimensions'] != dimensions:
            self.c.close()
            raise FlowError('DIMENSIONS')

    def close(self):
        self.c.close()

    def _state(self):
        return json.loads(self.c.execute('SELECT body FROM state WHERE id=1').fetchone()[0])

    def inspect(self):
        self.c.execute('BEGIN')
        try:
            result = {'state': self._state(), 'receipts': [
                [rid, json.loads(payload), json.loads(response)]
                for rid, payload, response in self.c.execute('SELECT id,payload,response FROM receipts ORDER BY id')]}
            self.c.execute('COMMIT')
            return result
        except BaseException:
            self.c.execute('ROLLBACK')
            raise

    def apply(self, request, hook=lambda point: None):
        if not isinstance(request, dict) or set(request) != {'request','expected','changes','upper','since'}:
            raise FlowError('REQUEST')
        rid = request['request']
        if not isinstance(rid, str) or not rid or not integer(request['expected']) or request['expected'] < 0:
            raise FlowError('REQUEST')
        payload = encode({k: request[k] for k in ('changes','upper','since')})
        self.c.execute('BEGIN IMMEDIATE')
        try:
            hook('after_begin')
            old = self.c.execute('SELECT payload,response FROM receipts WHERE id=?', (rid,)).fetchone()
            if old:
                if old[0] != payload:
                    raise FlowError('REQUEST_CONFLICT')
                self.c.execute('COMMIT')
                return json.loads(old[1])
            s = self._state()
            if request['expected'] != s['generation']:
                raise FlowError('GENERATION')
            d = s['dimensions']
            if not isinstance(request['changes'], list) or not isinstance(request['upper'], dict) or set(request['upper']) - {'L','R'}:
                raise FlowError('REQUEST')
            uppers = copy.deepcopy(s['upper'])
            for port, f in request['upper'].items():
                f = frontier(f, d)
                if not precedes(s['upper'][port], f):
                    raise FlowError('UPPER_REWIND')
                uppers[port] = f
            since = s['since'] if request['since'] is None else frontier(request['since'], d, False)
            if not precedes(s['since'], since) or any(not precedes(since, f) for f in uppers.values()):
                raise FlowError('SINCE')
            traces = copy.deepcopy(s['traces'])
            for change in request['changes']:
                if not isinstance(change, list) or len(change) != 4:
                    raise FlowError('CHANGE')
                port, t, row, w = change
                if port not in ('L','R'):
                    raise FlowError('PORT')
                t = timepoint(t, d)
                if not admits(s['upper'][port], t):
                    raise FlowError('LATE')
                if not isinstance(row, list) or len(row) != 2 or not isinstance(row[0], str) or not row[0]:
                    raise FlowError('ROW')
                if (port == 'L' and not integer(row[1])) or (port == 'R' and not isinstance(row[1], str)):
                    raise FlowError('ROW')
                if not integer(w) or not w:
                    raise FlowError('WEIGHT')
                traces[port].append([t, row[:], w])
            s.update(upper=uppers, since=since, generation=s['generation'] + 1,
                     traces={p: consolidate(rows, since) for p, rows in traces.items()})
            response = {'request': rid, 'generation': s['generation']}
            self.c.execute('UPDATE state SET body=? WHERE id=1', (encode(s),))
            hook('after_state')
            self.c.execute('INSERT INTO receipts VALUES(?,?,?)', (rid, payload, encode(response)))
            hook('after_receipt')
            self.c.execute('COMMIT')
        except BaseException:
            if self.c.in_transaction:
                self.c.execute('ROLLBACK')
            raise
        hook('after_commit')
        return response

    def read(self, t):
        s = self._state()
        t = timepoint(t, s['dimensions'])
        if not admits(s['since'], t):
            raise FlowError('COMPACTED')
        if any(admits(f, t) for f in s['upper'].values()):
            raise FlowError('INCOMPLETE')
        lefts = {}
        rights = {}
        for lt, row, w in s['traces']['L']:
            if le(lt, t):
                v = lefts.setdefault(row[0], [0, 0])
                v[0] += w
                v[1] += row[1] * w
        for rt, row, w in s['traces']['R']:
            if le(rt, t):
                k = tuple(row)
                rights[k] = rights.get(k, 0) + w
        sums = {}
        for (key, region), rw in rights.items():
            lc, amount = lefts.get(key, [0, 0])
            v = sums.setdefault(region, [0, 0])
            v[0] += lc * rw
            v[1] += amount * rw
        return {'generation': s['generation'], 'rows': [
            [region, count, total] for region, (count, total) in sorted(sums.items()) if count or total]}
