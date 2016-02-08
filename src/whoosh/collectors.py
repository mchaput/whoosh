import copy
from abc import abstractmethod
from collections import defaultdict, namedtuple
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from whoosh import results, sorting
from whoosh.compat import iteritems, xrange
from whoosh.ifaces import matchers, queries, searchers, weights
from whoosh.util import now


# Typing aliases

# (score, global_docnum, features)
MatchTuple = Tuple[float, int, Dict[str, Any]]


# Registry
registry = {}
compound_prefixes = []


RegistryEntry = namedtuple("RegistryEntry", "cls compound")


def register(name, compound=False):
    if name in registry:
        raise Exception("%r is already registered to %r" %
                        (name, registry[name].cls))

    if compound:
        compound_prefixes.append(name)

    def register_(cls):
        registry[name] = RegistryEntry(cls, compound)
        return cls

    return register_


def as_query(obj):
    from whoosh.ifaces import queries

    if isinstance(obj, queries.Query):
        return obj
    elif isinstance(obj, Collector):
        return obj.query()
    else:
        raise ValueError("%r is not a query" % obj)


# Classes

class Collector(object):
    collector_priority = 0

    def __init__(self, searcher: 'searchers.Searcher', q: 'queries.Query'):
        self._searcher = searcher
        self._query = q

        self._items = None  # type: List[MatchTuple]
        self._current_data = None  # type: Dict[str, Any]
        self._current_searcher = None  # type: searchers.Searcher
        self._current_offset = 0
        self._current_context = None  # type: searchers.SearchContext
        self._current_docset = None  # type: Optional[Set]
        self._current_matcher = None  # type: matchers.Matcher
        self._current_minscore = 0.0

    def __repr__(self) -> str:
        return "<%s %r>" % (type(self).__name__, self._query)

    # Housekeeping

    def __getattr__(self, name):
        if name in registry:
            entry = registry[name]
            
            def _combiner(*args, **kwargs):
                return entry.cls.combine_collector(self, args, kwargs)

            return _combiner

        for prefix in compound_prefixes:
            if name.startswith(prefix):
                from whoosh.ifaces import queries

                rest = name[len(prefix):]
                comp_cls = registry[prefix].cls
                qcls = registry[rest].cls
                assert isinstance(qcls, queries.Query)
                qcol = Collector(self.searcher(), queries.NullQuery())

                def _combiner(*args, **kwargs):
                    q = qcls.combine_collector(qcol, args, kwargs).query()
                    return comp_cls.combine_collector(self, q)

                return _combiner

        raise AttributeError(name)

    def rewrap(self, child):
        raise Exception("%s can't rewrap" % self.__class__.__name__)

    @classmethod
    def combine_collector(cls, collector: 'Collector', args, kwargs
                          ) -> 'Collector':
        raise Exception("%s can't combine" % cls.__name__)

    def searcher(self) -> 'searchers.Searcher':
        return self._searcher

    def with_query(self, newq: 'queries.Query'):
        return self.__class__(self.searcher(), newq)

    # Convenience

    def all(self) -> 'Collector':
        from whoosh.query import Every

        return self.with_query(Every())

    def get(self, **kwargs) -> 'Collector':
        searcher = self.searcher()
        q = searcher.keywords_to_query(kwargs)
        return self.with_query(q)

    # Getters and setters

    def set_query(self, q: 'queries.Query'):
        self._query = q

    def query(self) -> 'queries.Query':
        return self._query

    def current_context(self) -> 'searchers.SearchContext':
        return self._current_context

    def init_docset(self):
        self._current_docset = set()

    def current_docset(self) -> Optional[Set]:
        return self._current_docset

    def set_matcher(self, m: 'matchers.Matcher'):
        self._current_matcher = m

    def current_matcher(self) -> 'matchers.Matcher':
        return self._current_matcher

    def set_minscore(self, minscore: float):
        self._current_minscore = minscore

    def current_minscore(self) -> float:
        return self._current_minscore

    def current_data(self) -> Dict[str, Any]:
        return self._current_data

    # Evaluation

    def exists(self) -> bool:
        m = self.query().matcher(self.searcher())
        exists = m.is_active()
        m.close()
        return exists

    def all_ids(self) -> Iterable[int]:
        for globalid, localid, score, _ in self.matches():
            yield globalid

    def docs(self) -> Set[int]:
        return set(self.all_ids())

    # Collecting

    def results(self, context: 'searchers.SearchContext'=None
                ) -> 'results.Results':
        searcher = self.searcher()

        t = now()
        context = context or searcher.context()
        self.set_context(context)
        self.set_query(self.query().normalize())
        self.run()
        items = self.get_items()
        runtime = now() - t
        docset = self.current_docset()
        data = self.current_data()
        r = results.Results(searcher, self.query(), items, runtime=runtime,
                            docset=docset, collector=self, data=data)
        return r

    def set_context(self, context: 'searchers.SearchContext'):
        self._current_context = context

    def start(self):
        self._items = []
        self._current_data = {}
        self._current_searcher = None
        self._current_offset = 0
        self._current_docset = None
        self._current_matcher = None
        self._current_minscore = 0.0

    def finish(self):
        self._current_matcher.close()

    def set_subsearcher(self, subsearcher: 'searchers.Searcher', offset: int):
        self._current_searcher = subsearcher
        self._current_offset = offset

    def record_docid(self, globalid: int):
        self.current_docset().add(globalid)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        self._items.append((score, globalid, data))
        if self._current_docset is not None:
            self._current_docset.add(globalid)

    def forget(self, globalid):
        items = self._items
        for i in xrange(len(items)):
            if items[i][1] == globalid:
                items.pop(i)
                break

        if self._current_docset is not None:
            self._current_docset.discard(globalid)

    def get_items(self) -> List[MatchTuple]:
        return self._items

    def matches(self):
        self.start()
        searcher = self.searcher()
        context = self.current_context()
        q = self.query()

        context.top_searcher = searcher
        context.top_query = q

        replaced_times = 0
        skipped_times = 0
        for subsearcher, offset in searcher.leaf_searchers():
            self.set_subsearcher(subsearcher, offset)
            weight = context.weighting
            self.set_matcher(q.matcher(subsearcher, context))

            # If the weighting model uses a final scoring adjustment, we can't
            # use quality optimizations
            m = self.current_matcher()
            optimize = (weight and context.optimize and
                        m.supports_block_quality() and not weight.use_final)

            minscore = 0.0
            replacecounter = 0
            checkquality = False
            # If we're not optimized, we can record all seen document IDs
            can_record_ids = not optimize
            # If we can record IDs, initialize the docset
            if can_record_ids and self.current_docset() is None:
                self.init_docset()

            score = 1.0
            while m.is_active():
                # Try to replace the matcher with a more efficient version
                # every once in a while
                new_minscore = self.current_minscore()
                if replacecounter == 0 or new_minscore != minscore:
                    m = m.replace(new_minscore)
                    self.set_matcher(m)
                    replacecounter = 10
                    replaced_times += 1
                    minscore = new_minscore
                    m = self.current_matcher()
                    if not m.is_active():
                        break

                # Try to skip ahead using quality optimizations
                if optimize and checkquality and minscore:
                    skipped_times += m.skip_to_quality(minscore)
                    # Skipping ahead might have moved the matcher to the end of
                    # the posting list
                    if not m.is_active():
                        break

                localid = m.id()
                globalid = localid + offset

                if weight:
                    score = m.score()
                    if weight.use_final:
                        score = weight.final(subsearcher, localid, score)

                data = {}
                yield globalid, localid, score, data

                checkquality = m.next()

        rdata = self.current_data()
        rdata["skipped_times"] = skipped_times
        rdata["replaced_times"] = replaced_times

        self.finish()

    def run(self):
        for globalid, localid, score, data in self.matches():
            self.collect(globalid, localid, score, data)


class WrappingCollector(Collector):
    def __init__(self, child: Collector):
        self.child = child

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.child)

    # Housekeeping

    @classmethod
    def combine_collector(cls, collector: Collector, args, kwargs
                          ) -> 'WrappingCollector':
        cls_pri = getattr(cls, "collector_priority", 0)
        existing_pri = getattr(collector, "collector_priority", 0)

        if existing_pri > cls_pri:
            # The existing wrapper has a higher priority than this class.
            # (We assume it's a wrapper since only wrappers should have non-zero
            # priorities.) So, wrap this class around the wrapper's CHILD,
            # *then*, re-wrap the existing wrapper around that
            newchild = cls.combine_collector(collector.child, args, kwargs)
            result = collector.rewrap(newchild)
        else:
            # Wrap this class around the existing collector
            result = cls(collector, *args, **kwargs)

        return result

    def rewrap(self, newchild: Collector) -> 'WrappingCollector':
        return self.__class__(newchild)

    def with_query(self, newq: 'queries.Query') -> 'WrappingCollector':
        return self.rewrap(self.child.with_query(newq))

    # Getters and setters

    def searcher(self) -> 'searchers.Searcher':
        return self.child.searcher()

    def set_query(self, q: 'queries.Query'):
        self.child.set_query(q)

    def query(self) -> 'queries.Query':
        return self.child.query()

    def current_context(self) -> 'searchers.SearchContext':
        return self.child.current_context()

    def init_docset(self):
        self.child.init_docset()

    def current_docset(self) -> Optional[Set]:
        return self.child.current_docset()

    def set_matcher(self, m: 'matchers.Matcher'):
        self.child.set_matcher(m)

    def current_matcher(self) -> 'matchers.Matcher':
        return self.child.current_matcher()

    def set_minscore(self, minscore: float):
        self.child.set_minscore(minscore)

    def current_minscore(self) -> float:
        return self.child.current_minscore()

    def current_data(self) -> Dict[str, Any]:
        return self.child.current_data()

    # Collecting

    def set_context(self, context: 'searchers.SearchContext'):
        self.child.set_context(context)

    def start(self):
        self.child.start()

    def finish(self):
        self.child.finish()

    def set_subsearcher(self, subsearcher: 'searchers.Searcher', offset: int):
        self.child.set_subsearcher(subsearcher, offset)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        self.child.collect(globalid, localid, score, data)

    def forget(self, globalid):
        self.child.forget(globalid)

    def get_items(self) -> List[MatchTuple]:
        return self.child.get_items()

    def all_ids(self) -> Iterable[int]:
        return self.child.all_ids()


@register("top")
class TopCollector(WrappingCollector):
    collector_priority = 100

    def __init__(self, child: Collector, limit: int=20):
        super(TopCollector, self).__init__(child)
        self._limit = limit

        self._scored = None  # type: List[MatchTuple]

    def __repr__(self):
        return "<%s %s %r>" % (type(self).__name__, self._limit, self.child)

    def rewrap(self, child: Collector) -> 'TopCollector':
        return self.__class__(child, self._limit)

    # Collecting

    def set_context(self, context: 'searchers.SearchContext'):
        context.limit = self._limit
        self.child.set_context(context)

    def start(self):
        self._scored = []
        self.child.start()

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        from heapq import heappush, heapreplace

        scored = self._scored
        limit = self._limit

        # Manipulate tuple so it sorts properly by score
        item = score, 0 - globalid, data
        # Put the item on the heap
        if limit:
            if len(scored) < limit:
                heappush(scored, item)
            elif score > scored[0][0]:
                heapreplace(scored, item)
                self.set_minscore(score)
        else:
            scored.append(item)

        self.child.collect(globalid, localid, score, data)

    def forget(self, globalid):
        from heapq import heapify

        scored = self._scored
        for i in xrange(len(scored)):
            if 0 - scored[i][1] == globalid:
                scored.pop(i)
                heapify(scored)

                if i == 0:
                    # We just forgot the doc with the lowest score, so we should
                    # set a new minscore
                    self.set_minscore(scored[0][0])

                break

        self.child.forget(globalid)

    def get_items(self) -> List[MatchTuple]:
        scored = self._scored
        # The items are in heap order, so we need to sort them (in reverse so
        # higher scores are first)
        scored.sort(reverse=True)
        # Un-invert the global ID
        return [(score, 0 - inv_globalid, d) for score, inv_globalid, d
                in scored]


@register("sorted_by")
class SortingCollector(WrappingCollector):
    collector_priority = 500

    def __init__(self, child: Collector,
                 *criteria: Sequence[sorting.FacetType]):
        super(SortingCollector, self).__init__(child)
        self._facet = sorting.FacetType.from_sortedby(criteria)
        self._catter = None  # type: sorting.Categorizer

    def rewrap(self, child: Collector) -> 'SortingCollector':
        return self.__class__(child, self._facet)

    # Collecting

    def set_context(self, context: 'searchers.SearchContext'):
        context.optimize = False
        self.child.set_context(context)

    def start(self):
        self._catter = self._facet.categorizer(self.searcher())
        self.child.start()

    def finish(self):
        self._catter.close()
        self.child.finish()

    def set_subsearcher(self, subsearcher: 'searchers.Searcher', offset: int):
        self._catter.set_searcher(subsearcher, offset)
        self.child.set_subsearcher(subsearcher, offset)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        # Get the sorting key for the current document and store it in the hit
        # data
        key = self._catter.key_for(self.current_matcher(), localid)
        data["sort_key"] = key
        self.child.collect(globalid, localid, score, data)

    def get_items(self) -> List[MatchTuple]:
        items = self.child.get_items()
        items = sorted(items, key=lambda x: x[-1]["sort_key"])
        return items


@register("grouped_by")
class GroupingCollector(WrappingCollector):
    def __init__(self, child: Collector, groupedby: 'sorting.FacetType',
                 name: str=None, maptype: 'sorting.FacetMap'=None):
        super(GroupingCollector, self).__init__(child)
        self._facet = sorting.FacetType.from_sortedby(groupedby, maptype)
        self._name = name or self._facet.default_name()

        self._catter = None  # type: sorting.Categorizer
        self._map = None  # type: sorting.FacetMap

    def rewrap(self, child: Collector) -> 'SortingCollector':
        return self.__class__(child, self._facet)

    # Collecting

    def start(self):
        self._catter = self._facet.categorizer(self.searcher())
        self._map = self._facet.map()
        self.child.start()

    def finish(self):
        self._catter.close()

        data = self.current_data()
        data[self._name] = self._map.as_dict()

        self.child.finish()

    def set_subsearcher(self, subsearcher: 'searchers.Searcher', offset: int):
        self._catter.set_searcher(subsearcher, offset)
        self.child.set_subsearcher(subsearcher, offset)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        name = self._name
        catter = self._catter
        facetmap = self._map
        matcher = self.current_matcher()
        sortkey = data.get("sort_key", 0 - score)

        if catter.allow_overlap:
            keynames = [catter.key_to_name(n) for n
                        in catter.keys_for(matcher, localid)]
            data[name] = keynames
            for keyname in keynames:
                facetmap.add(keyname, globalid, sortkey)
        else:
            keyname = catter.key_to_name(catter.key_for(matcher, localid))
            data[name] = keyname
            facetmap.add(keyname, globalid, sortkey)

        self.child.collect(globalid, localid, score, data)


@register("filter")
class FilterCollector(WrappingCollector):
    collector_priority = 1

    def __init__(self, child: Collector, include: 'queries.Query'=None,
                 exclude: 'queries.Query'=None):
        self.child = child
        self._include = include
        self._exclude = exclude

    def rewrap(self, child: Collector) -> 'FilterCollector':
        return self.__class__(child, self._include, self._exclude)

    # Collecting

    def set_context(self, context: 'searchers.SearchContext'):
        context.include = self._include
        context.exclude = self._exclude
        self.child.set_context(context)


@register("with_terms")
class TermsCollector(WrappingCollector):
    def __init__(self, child: Collector, fieldnames: Sequence[str]=None):
        self.child = child

        if isinstance(fieldnames, str):
            fieldnames = [fieldnames]
        self._fieldnames = fieldnames

        self._termset = None  # type: Set[bytes]

    def rewrap(self, child: Collector) -> 'FilterCollector':
        return self.__class__(child, self._fieldnames)

    # Collecting

    def start(self):
        self.child.start()
        self._termset = set()

        # Put a reference to the termset in the results data
        data = self.current_data()
        data["terms"] = self._termset

    def set_context(self, context: 'searchers.SearchContext'):
        context.optimize = False
        self.child.set_context(context)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        fieldnames = self._fieldnames
        matcher = self.current_matcher()
        hitset = set()
        for term in matcher.matching_terms(localid):
            if not fieldnames or term[0] in fieldnames:
                hitset.add(term)

        # Store the matched terms in the hit data
        data["terms"] = hitset
        # Add the matched terms to the result data
        self._termset.update(hitset)

        self.child.collect(globalid, localid, score, data)


@register("with_spans")
class SpansCollector(WrappingCollector):
    def __init__(self, child: Collector, fieldnames: Sequence[str]=None):
        self.child = child
        self._fieldnames = fieldnames

    def rewrap(self, child: Collector) -> 'FilterCollector':
        return self.__class__(child, self._fieldnames)

    # Collecting

    def set_context(self, context: 'searchers.SearchContext'):
        context.optimize = False
        self.child.set_context(context)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        fieldnames = self._fieldnames
        matcher = self.current_matcher()
        spans = []
        for tm in matcher.term_matchers():
            if tm.is_active() and tm.id() == localid:
                fieldname, text = tm.term()
                if not fieldnames or fieldname in fieldnames:
                    for span in tm.spans():
                        span.fieldname = fieldname
                        span.text = text
                        spans.append(span)
        # Add the spans to the hit data
        data["spans"] = spans
        self.child.collect(globalid, localid, score, data)


@register("collapse")
class CollapsingCollector(WrappingCollector):
    # This needs to be outside a TopCollector
    collector_priority = 200

    def __init__(self, child: Collector, facet: 'sorting.FacetType',
                 limit: int=1, order: 'sorting.FacetType'=None):
        self.child = child
        self._facet = sorting.FacetType.from_sortedby(facet)
        self._limit = limit
        self._order = order

        self._collapse_cat = None  # type: sorting.Categorizer
        self._order_cat = None  # type: sorting.Categorizer
        self._lists = None  # type: Dict[Any, List[Tuple[Any, int]]]
        self._counts = None  # type: Dict[Any, int]
        self._i = 0

    def rewrap(self, child: Collector) -> 'CollapsingCollector':
        return self.__class__(child, self._facet, self._limit, self._order)

    # Collecting

    def start(self):
        searcher = self.searcher()
        self._collapse_cat = self._facet.categorizer(searcher)
        if self._order:
            self._order_cat = self._order.categorizer(searcher)
        else:
            self._order_cat = None
        self._lists = defaultdict(list)
        self._counts = defaultdict(int)
        self.child.start()

    def finish(self):
        self._collapse_cat.close()
        if self._order_cat:
            self._order_cat.close()

        counts = self._counts
        # Put references to the collapsed counts in the result data
        data = self.current_data()
        data["collapsed"] = counts
        data["collapsed_total"] = sum(counts.values()) if counts else 0
        self.child.finish()

    def set_context(self, context: 'searchers.SearchContext'):
        context.optimize = False
        self.child.set_context(context)

    def set_subsearcher(self, subsearcher: 'searchers.Searcher', offset: int):
        self._collapse_cat.set_searcher(subsearcher, offset)
        if self._order_cat:
            self._order_cat.set_searcher(subsearcher, offset)
        self.child.set_subsearcher(subsearcher, offset)

    def _add_forget(self, globalid, localid, score, data):
        from bisect import insort

        limit = self._limit
        m = self.current_matcher()
        collapse_cat = self._collapse_cat
        order_cat = self._order_cat
        lists = self._lists
        counts = self._counts

        add = True
        forget = None

        key = collapse_cat.key_to_name(collapse_cat.key_for(m, localid))
        if order_cat:
            sortkey = order_cat.key_for(m, localid) if order_cat else None
        elif "sort_key" in data:
            sortkey = data["sort_key"]
        else:
            sortkey = 0 - score

        best = lists[key]
        if len(best) < limit:
            # The heap is not full, just add this document
            best.append((sortkey, globalid))

        elif sortkey < best[-1][0]:
            # The heap is full but this doc has a lower sortkey than the
            # "least-best" one on the heap
            if limit == 1:
                # Special case the default for speed
                forget = best[0][1]
                best[0] = (sortkey, globalid)
            else:
                _, forget = best.pop()
                insort(best, (sortkey, globalid))

        else:
            # This doc doesn't have a low enough sort key, discard it
            add = False
            # Remember that we filtered a document
            counts[key] += 1

        return add, forget

    def all_ids(self) -> Iterable[int]:
        ids = []
        add_forget = self._add_forget
        for globalid, localid, score, data in self.matches():
            add, forget = add_forget(globalid, localid, score, data)
            if forget is not None:
                ids.remove(forget)
            if add:
                ids.append(globalid)
        return iter(ids)

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        add, forget = self._add_forget(globalid, localid, score, data)
        if forget is not None:
            self.child.forget(forget)
        if add:
            self.child.collect(globalid, localid, score, data)



@register("sample")
class SamplingCollector(WrappingCollector):
    """
    Produces a random sampling of the matching documents. The results are in
    random order::

        searcher.q.term("content", "bravo").sample(10).results()
    """

    def __init__(self, child: Collector, limit: int=1):
        super(SamplingCollector, self).__init__(child)
        self._limit = limit
        self._sample = None  # type: List[int]
        self._i = 0

    def rewrap(self, child: Collector) -> 'SamplingCollector':
        return self.__class__(child, self._limit)

    # Collecting

    def start(self):
        self._sample = []
        self._i = 0
        self.child.start()

    def collect(self, globalid: int, localid: int, score: float,
                data: Dict[str, Any]):
        from random import randint, random

        limit = self._limit
        sample = self._sample
        i = self._i
        if i < limit:
            sample.append(globalid)
            self.child.collect(globalid, localid, score, data)
        elif random() < limit/float(self._i + 1):
            replace = randint(0, len(sample) - 1)
            old_global = sample[replace]
            sample[replace] = globalid
            self.child.forget(old_global)
            self.child.collect(globalid, localid, score, data)

        self._i += 1


@register("weighted_by")
class WeightingCollector(WrappingCollector):
    def __init__(self, weighting: 'weights.WeightingModel'):
        self._weighting = weighting

    def rewrap(self, child: Collector) -> 'WeightingCollector':
        return self.__class__(child, self._weighting)

    # Collecting

    def set_context(self, context: 'searchers.SearchContext'):
        context.weighting = self._weighting
        self.child.set_context(context)


@register("page")
class PageCollector(WrappingCollector):
    def __init__(self, child, page=1, size=10):
        super(PageCollector, self).__init__(child)
        self._page = page
        self._size = size

    def rewrap(self, child: Collector) -> 'PageCollector':
        return self.__class__(child, self._page, self._size)

    def results(self, context: 'searchers.SearchContext'=None
                ) -> 'results.Results':
        from whoosh.results import ResultsPage

        r = self.child.results(context)
        return ResultsPage(r, self._page, self._size)


@register("limit")
class LimitCollector(WrappingCollector):
    collector_priority = 1000

    def __init__(self, child: Collector, limit: int=10):
        super(LimitCollector, self).__init__(child)
        self._limit = limit

    def rewrap(self, newchild: Collector) -> 'LimitCollector':
        return self.__class__(newchild, self._limit)

    def get_items(self) -> List[MatchTuple]:
        items = self.child.get_items()
        if self._limit is not None and len(items) > self._limit:
            del items[self._limit:]
        return items


@register("reversed")
class ReversingCollector(WrappingCollector):
    collector_priority = 900

    def __init__(self, child: Collector, limit: int=10):
        super(ReversingCollector, self).__init__(child)
        self._limit = limit

    def rewrap(self, child: Collector) -> 'ReversingCollector':
        return self.__class__(self.child, self._limit)

    def get_items(self) -> List[MatchTuple]:
        items = self.child.get_items()
        items.reverse()
        return items


