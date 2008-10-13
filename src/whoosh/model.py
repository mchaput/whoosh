import query

class Model(object):
    def __init__(self, ix):
        self.ix = ix
        self.writer = None
        
    def triples(self, subj, pred, obj):
        ir = self.ix.reader()
        dr = ir.doc_reader()
        tr = ir.term_reader()
        
        q = query.And([query.Term(f, x) for f, x
                       in zip(("subj", "pred", "obj"), (subj, pred, obj))
                       if x is not None])
        r = q.run(tr)
        for d in r.iterkeys():
            trips = dr[d]
            if subj is None: yield trips[0]
            elif pred is None: yield trips[1]
            elif obj is None: yield trips[2]
