============
Field caches
============

The default (``filedb``) backend uses *field caches*. The field cache basically
pre-computes the order of documents in the index to speed up sorting and
faceting.

Generating field caches can take time the first time you sort/facet on a large
index. The field cache is kept in memory and written to disk so subsequent
sorted/faceted searches should be faster.

TBD.







