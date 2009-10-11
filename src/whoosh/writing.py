#===============================================================================
# Copyright 2007 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

from whoosh.index import DeletionMixin

# Exceptions

class IndexingError(Exception):
    pass


# Base class

class IndexWriter(DeletionMixin):
    """High-level object for writing to an index.
    
    To get a writer for a particular index, call
    call :meth:`~whoosh.index.Index.writer` on the Index object.
    
    >>> writer = my_index.writer()
    
    You can use this object as a context manager. If an exception is thrown
    from within the context it calls cancel(), otherwise it calls commit()
    when the context exits.
    """
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.cancel()
        else:
            self.commit()
    
    def searcher(self, **kwargs):
        """Returns a searcher for the existing index."""
        if not self._searcher:
            self._searcher = self.index.searcher(**kwargs)
        return self._searcher
    
    def _close_reader(self):
        if self._searcher:
            self._searcher.close()
            self._searcher = None
    
    def delete_document(self, docnum, delete=True):
        """Deletes a document by number."""
        raise NotImplementedError
    
    def add_document(self, **fields):
        """Adds all the fields of a document at once. This is an alternative to calling
        start_document(), add_field() [...], end_document().
        
        The keyword arguments map field names to the values to index/store.
        
        For fields that are both indexed and stored, you can specify an alternate
        value to store using a keyword argument in the form "_stored_<fieldname>".
        For example, if you have a field named "title" and you want to index the
        text "a b c" but store the text "e f g", use keyword arguments like this::
        
            add_document(title=u"a b c", _stored_title=u"e f g")
        """
        raise NotImplementedError
    
    def update_document(self, **fields):
        """Adds or replaces a document. At least one of the fields for which you
        supply values must be marked as 'unique' in the index's schema.
        
        The keyword arguments map field names to the values to index/store.
        
        For fields that are both indexed and stored, you can specify an alternate
        value to store using a keyword argument in the form "_stored_<fieldname>".
        For example, if you have a field named "title" and you want to index the
        text "a b c" but store the text "e f g", use keyword arguments like this::
        
            update_document(title=u"a b c", _stored_title=u"e f g")
        """
        
        # Check which of the supplied fields are unique
        unique_fields = [name for name, field
                         in self.index.schema.fields()
                         if name in fields and field.unique]
        if not unique_fields:
            raise IndexingError("None of the fields in %r are unique" % fields.keys())
        
        # Delete documents in which the supplied unique fields match
        from whoosh import query
        delquery = query.Or([query.Term(name, fields[name]) for name in unique_fields])
        delquery = delquery.normalize()
        self.delete_by_query(delquery)
        
        # Add the given fields
        self.add_document(**fields)
    
    def commit(self):
        """Finishes writing and unlocks the index.
        """
        pass
        
    def cancel(self):
        """Cancels any documents/deletions added by this object
        and unlocks the index.
        """
        pass
    


        
        
