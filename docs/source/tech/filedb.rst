============
filedb notes
============

TBD.

Files created
=============

<revision_number>.toc
    The "master" file containing information about the index and its segments.

The index directory will contain a set of files for each segment. A segment is like a mini-index -- when you add documents to the index, whoosh creates a new segment and then searches the old segment(s) and the new segment to avoid having to do a big merge every time you add a document. When you get enough small segments whoosh will merge them into larger segments or a single segment.

<segment_number>.dci
    Contains per-document information (e.g. field lengths). This will grow linearly with the number of documents.

<segment_number>.dcz
    Contains the stored fields for each document.

<segment_number>.tiz
    Contains per-term information. The size of file will vary based on the number of unique terms.

<segment_number>.pst
    Contains per-term postings. The size of this file depends on the size of the collection and the formats used for each field (e.g. storing term positions takes more space than storing frequency only).

<segment_number>.fvz
    contains term vectors (forward indexes) for each document. This file is only created if at least one field in the schema stores term vectors. The size will vary based on the number of documents, field length, the formats used for each vector (e.g. storing term positions takes more space than storing frequency only), etc.

